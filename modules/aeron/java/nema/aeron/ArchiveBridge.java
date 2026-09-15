package nema.aeron;

import io.aeron.Aeron;
import io.aeron.AeronCounters;
import io.aeron.ChannelUri;
import io.aeron.ControlledFragmentAssembler;
import io.aeron.Image;
import io.aeron.Subscription;
import io.aeron.archive.Archive;
import io.aeron.archive.ArchiveCounters;
import io.aeron.archive.ArchiveThreadingMode;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.PersistentSubscription;
import io.aeron.archive.client.PersistentSubscriptionListener;
import io.aeron.archive.codecs.SourceLocation;
import io.aeron.logbuffer.ControlledFragmentHandler;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.SleepingMillisIdleStrategy;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

/** Narrow Archive control/buffer interop. Callers own duty cycles and retention policy. */
public final class ArchiveBridge implements AutoCloseable {
    private static final int CONTROL_STREAM = 9100;
    private static final String IPC = "aeron:ipc?term-length=65536";
    private final String driverDirectory;
    private final Path storage;
    private final LockLease storageLease;
    private final LockLease driverLease;
    private final Archive server;
    private final AeronArchive client;
    private final String incarnation;
    private final Set<AutoCloseable> children = new HashSet<>();
    private final AtomicReference<Throwable> failure = new AtomicReference<>();
    private volatile boolean closed;
    private int leakedChildren;

    private ArchiveBridge(String driverDirectory, Path storage, long archiveId, int segmentLength, int syncLevel)
        throws IOException {
        if (syncLevel < 0 || syncLevel > 2) throw new IllegalArgumentException("syncLevel must be 0, 1 or 2");
        this.driverDirectory = driverDirectory;
        this.storage = storage.toAbsolutePath();
        Files.createDirectories(this.storage);
        storageLease = LockLease.acquire(this.storage.resolve("nema-owner.lock"));
        LockLease openedDriverLease = null;
        Archive openedServer = null;
        AeronArchive openedClient = null;
        try {
            openedDriverLease = LockLease.acquire(Path.of(driverDirectory).resolve("nema-archive-owner.lock"));
            driverLease = openedDriverLease;
            Path identityFile = this.storage.resolve("nema-incarnation");
            if (Files.exists(identityFile) && !Files.exists(this.storage.resolve("archive.catalog")))
                throw new IllegalStateException("Retained incarnation has no catalog; refuse recording ID reuse");
            if (!Files.exists(identityFile)) {
                if (Files.exists(this.storage.resolve("archive.catalog")))
                    throw new IllegalStateException("Existing catalog has no Nema incarnation: explicit adoption required");
                String fresh = archiveId + ":" + UUID.randomUUID();
                try (FileChannel out = FileChannel.open(identityFile,
                    StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
                    ByteBuffer bytes = StandardCharsets.UTF_8.encode(fresh + "\n");
                    while (bytes.hasRemaining()) out.write(bytes);
                    out.force(true);
                }
                try (FileChannel dir = FileChannel.open(this.storage, StandardOpenOption.READ)) { dir.force(true); }
            }
            incarnation = Files.readString(identityFile).trim();
            if (!incarnation.startsWith(archiveId + ":"))
                throw new IllegalArgumentException("Archive native id differs from retained incarnation");
            openedServer = Archive.launch(new Archive.Context()
                .aeronDirectoryName(driverDirectory).archiveDir(this.storage.toFile())
                .archiveId(archiveId).deleteArchiveOnStart(false)
                .archiveClientContext(context())
                .controlChannelEnabled(false).localControlChannel(IPC).localControlStreamId(CONTROL_STREAM)
                .recordingEventsEnabled(false).replicationChannel("aeron:udp?endpoint=127.0.0.1:0")
                .segmentFileLength(segmentLength).fileSyncLevel(syncLevel).catalogFileSyncLevel(syncLevel)
                .lowStorageSpaceThreshold(0).threadingMode(ArchiveThreadingMode.SHARED)
                .idleStrategySupplier(() -> new SleepingMillisIdleStrategy(1))
                .errorHandler(this::onError));
            openedClient = AeronArchive.connect(context());
            if (openedClient.archiveId() != archiveId)
                throw new IllegalStateException("Connected to unexpected Archive id " + openedClient.archiveId());
            server = openedServer;
            client = openedClient;
        } catch (Throwable error) {
            closeAfterFailure(openedClient, error);
            closeAfterFailure(openedServer, error);
            closeAfterFailure(openedDriverLease, error);
            closeAfterFailure(storageLease, error);
            throw error;
        }
    }

    private static void closeAfterFailure(AutoCloseable resource, Throwable original) {
        if (resource != null) {
            try { resource.close(); } catch (Throwable cleanup) { original.addSuppressed(cleanup); }
        }
    }

    /** Closing acquisition failures matters: overlapping JVM locks throw rather than returning null. */
    private record LockLease(FileChannel channel, FileLock lock) implements AutoCloseable {
        static LockLease acquire(Path path) throws IOException {
            FileChannel channel = FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.WRITE);
            try {
                FileLock lock = channel.tryLock();
                if (lock == null) throw new IllegalStateException("Archive owner already holds " + path);
                return new LockLease(channel, lock);
            } catch (Throwable error) {
                closeAfterFailure(channel, error);
                throw error;
            }
        }
        public void close() throws IOException {
            try { lock.close(); } finally { channel.close(); }
        }
    }

    /** Owns Archive and its clients; borrows the Media Driver at driverDirectory. */
    public static ArchiveBridge openOwned(String driverDirectory, String storageDirectory,
        long archiveId, int segmentLength, int syncLevel) throws IOException {
        return new ArchiveBridge(driverDirectory, Path.of(storageDirectory), archiveId, segmentLength, syncLevel);
    }

    private AeronArchive.Context context() {
        return new AeronArchive.Context().aeronDirectoryName(driverDirectory)
            .controlRequestChannel(IPC).controlRequestStreamId(CONTROL_STREAM)
            .controlResponseChannel(IPC).controlResponseStreamId(CONTROL_STREAM + 1)
            .messageTimeoutNs(3_000_000_000L).idleStrategy(new SleepingMillisIdleStrategy(1))
            .errorHandler(this::onError);
    }

    private void onError(Throwable error) { failure.compareAndSet(null, error); }
    private void check() {
        if (closed) throw new IllegalStateException("Archive closed");
        Throwable error = failure.get();
        if (error != null) throw new IllegalStateException("Archive worker failed", error);
    }

    public String incarnation() { return incarnation; }
    public long archiveId() { return client.archiveId(); }
    public int fileSyncLevel() { return server.context().fileSyncLevel(); }
    public int catalogSyncLevel() { return server.context().catalogFileSyncLevel(); }
    public String failure() { Throwable error = failure.get(); return error == null ? "" : error.toString(); }
    public synchronized int openChildren() { return children.size(); }
    public synchronized int leakedChildren() { return leakedChildren; }

    public synchronized long startRecording(String channel, int stream) {
        check(); return client.startRecording(channel, stream, SourceLocation.LOCAL);
    }
    public synchronized void stopRecording(long subscriptionId) { check(); client.stopRecording(subscriptionId); }
    public synchronized long findRecording(String channelFragment, int stream, int session) {
        check(); return client.findLastMatchingRecording(0, channelFragment, stream, session);
    }
    /** Native recorded byte progress; this API deliberately does not call it an fsync completion. */
    public synchronized long progress(long recordingId) { check(); return client.getMaxRecordedPosition(recordingId); }

    public synchronized Descriptor descriptor(long recordingId) {
        check();
        final Descriptor[] result = new Descriptor[1];
        client.listRecording(recordingId, (control, correlation, id, startTime, stopTime, start, stop,
            initialTerm, segment, term, mtu, session, stream, stripped, original, source) ->
            result[0] = new Descriptor(incarnation, id, start, stop, initialTerm, segment, term, mtu,
                session, stream, original, source));
        if (result[0] == null) throw new IllegalArgumentException("Unknown recording " + recordingId);
        return result[0];
    }

    public synchronized Descriptor[] list(long firstId, int count) {
        check();
        if (firstId < 0 || count < 1 || count > 10000) throw new IllegalArgumentException("Invalid list bounds");
        ArrayList<Descriptor> result = new ArrayList<>();
        client.listRecordings(firstId, count, (control, correlation, id, startTime, stopTime, start, stop,
            initialTerm, segment, term, mtu, session, stream, stripped, original, source) ->
            result.add(new Descriptor(incarnation, id, start, stop, initialTerm, segment, term, mtu,
                session, stream, original, source)));
        return result.toArray(Descriptor[]::new);
    }

    private void validateRange(Descriptor d, long start, long end) {
        long available = progress(d.recordingId);
        if (start < d.startPosition || end < start || end > available || (start & 31) != 0 || (end & 31) != 0)
            throw new IllegalArgumentException("Invalid/pruned replay range [" + start + "," + end +
                "); retained [" + d.startPosition + "," + available + ")");
        if (start < available) checkMessageBoundary(d, start);
        if (end < available) checkMessageBoundary(d, end);
    }

    /** Check the retained source header, including fragmented-message boundaries. */
    private void checkMessageBoundary(Descriptor d, long position) {
        long base = d.segmentBase(position);
        // Archive.segmentFileName and its suffix constant are package-private in 1.53.1.
        Path segment = storage.resolve(d.recordingId + "-" + base + ".rec");
        ByteBuffer header = ByteBuffer.allocate(32).order(ByteOrder.LITTLE_ENDIAN);
        try (FileChannel file = FileChannel.open(segment, StandardOpenOption.READ)) {
            long cursor = Math.max(base, d.startPosition);
            int length, flags, type;
            do {
                header.clear();
                long offset = cursor - base;
                while (header.hasRemaining()) {
                    int count = file.read(header, offset + header.position());
                    if (count < 0) throw new IllegalArgumentException("Range has no retained frame header");
                }
                length = header.getInt(0);
                flags = Byte.toUnsignedInt(header.get(5));
                type = Short.toUnsignedInt(header.getShort(6));
                if (length < 32 || (type != 0 && type != 1))
                    throw new IllegalArgumentException("Invalid retained frame before replay boundary: " + cursor);
                if (cursor == position) break;
                cursor += ((long)length + 31) & ~31L;
                if (cursor > position) throw new IllegalArgumentException("Range cuts an Aeron frame: " + position);
            } while (cursor <= position);
            if (type != 0 && (flags & 0x80) == 0) {
                throw new IllegalArgumentException("Range cuts a fragmented application message: " + position);
            }
        } catch (IOException error) { throw new IllegalStateException("Cannot inspect retained range", error); }
    }

    /** Prove a source-coordinate gap consists only of complete recorded PAD frames, never unseen application data. */
    public synchronized boolean paddingOnly(long recordingId, long from, long to) {
        check();
        Descriptor descriptor = descriptor(recordingId);
        validateRange(descriptor, from, to);
        long cursor = from;
        ByteBuffer header = ByteBuffer.allocate(32).order(ByteOrder.LITTLE_ENDIAN);
        while (cursor < to) {
            long base = descriptor.segmentBase(cursor);
            Path segment = storage.resolve(recordingId + "-" + base + ".rec");
            header.clear();
            try (FileChannel file = FileChannel.open(segment, StandardOpenOption.READ)) {
                while (header.hasRemaining()) {
                    int count = file.read(header, cursor - base + header.position());
                    if (count < 0) throw new IllegalArgumentException("Padding range has no retained header");
                }
            } catch (IOException error) { throw new IllegalStateException("Cannot inspect retained padding", error); }
            int length = header.getInt(0);
            int type = Short.toUnsignedInt(header.getShort(6));
            if (length < 32 || type != 0) return false;
            long next = cursor + (((long)length + 31) & ~31L);
            if (next > to) return false;
            cursor = next;
        }
        return true;
    }

    public synchronized Replay replay(long recordingId, long start, long end, int stream, int capacity) {
        check();
        Descriptor d = descriptor(recordingId);
        validateRange(d, start, end);
        Replay replay = new Replay(this, d, start, end, stream, capacity);
        children.add(replay);
        return replay;
    }

    public synchronized Persistent persistent(long recordingId, long start, String liveChannel,
        int liveStream, int replayStream, int capacity) {
        check();
        Descriptor d = descriptor(recordingId);
        if (start != PersistentSubscription.FROM_START && start != PersistentSubscription.FROM_LIVE)
            validateRange(d, start, start);
        Persistent persistent = new Persistent(this, d, start, liveChannel, liveStream, replayStream, capacity);
        children.add(persistent);
        return persistent;
    }

    /** Mechanism only: caller supplies its satisfied watermark AND earliest required replay pin. */
    public synchronized long safePurgePosition(long recordingId, long satisfiedThrough, long replayPin) {
        Descriptor d = descriptor(recordingId);
        long safe = Math.min(Math.min(satisfiedThrough, replayPin), progress(recordingId));
        return safe <= d.startPosition ? d.startPosition : Math.max(d.startPosition, d.segmentBase(safe));
    }

    /** Front purge, never truncate. Refuses to cross active bounded/persistent child replay ranges. */
    public synchronized long purgeSegments(long recordingId, long satisfiedThrough, long replayPin) {
        check();
        long next = safePurgePosition(recordingId, satisfiedThrough, replayPin);
        Descriptor d = descriptor(recordingId);
        if (next == d.startPosition) return 0;
        for (AutoCloseable child : children) {
            if (child instanceof Reader reader && reader.descriptor.recordingId == recordingId && reader.pin < next)
                throw new IllegalStateException("Open replay/persistent reader pins the requested segments");
        }
        return client.purgeSegments(recordingId, next);
    }

    private synchronized void released(AutoCloseable child) { children.remove(child); }

    @Override public void close() {
        final ArrayList<AutoCloseable> toClose;
        synchronized (this) {
            if (closed) return;
            closed = true;
            leakedChildren = children.size();
            toClose = new ArrayList<>(children);
        }
        Throwable first = null;
        for (AutoCloseable child : toClose) {
            try { child.close(); } catch (Throwable error) { if (first == null) first = error; }
        }
        try { server.close(); } catch (Throwable error) { if (first == null) first = error; }
        // Native close sends asynchronous driver removals. Keep this independent client alive to observe reclamation,
        // so an immediate restart with the same Archive id does not race stale allocated counters.
        try {
            long deadline = System.nanoTime() + 3_000_000_000L;
            while (ArchiveCounters.find(client.context().aeron().countersReader(),
                AeronCounters.ARCHIVE_ERROR_COUNT_TYPE_ID, server.context().archiveId()) != Aeron.NULL_VALUE) {
                if (System.nanoTime() >= deadline)
                    throw new IllegalStateException("Archive native counter reclamation timed out");
                LockSupport.parkNanos(1_000_000);
            }
        } catch (Throwable error) { if (first == null) first = error; }
        try { client.close(); } catch (Throwable error) { if (first == null) first = error; }
        try { driverLease.close(); } catch (Throwable error) { if (first == null) first = error; }
        try { storageLease.close(); } catch (Throwable error) { if (first == null) first = error; }
        if (first != null) throw new IllegalStateException("Archive close failed", first);
    }

    public static final class Descriptor {
        public final String incarnation;
        public final long recordingId, startPosition, stopPosition;
        public final int initialTermId, segmentLength, termLength, mtuLength, sessionId, streamId;
        public final String channel, sourceIdentity;
        Descriptor(String incarnation, long id, long start, long stop, int initialTerm, int segment,
            int term, int mtu, int session, int stream, String channel, String source) {
            this.incarnation = incarnation; recordingId = id; startPosition = start; stopPosition = stop;
            initialTermId = initialTerm; segmentLength = segment; termLength = term; mtuLength = mtu;
            sessionId = session; streamId = stream; this.channel = channel; sourceIdentity = source;
        }
        public String incarnation() { return incarnation; }
        public long recordingId() { return recordingId; }
        public long startPosition() { return startPosition; }
        public long stopPosition() { return stopPosition; }
        public int initialTermId() { return initialTermId; }
        public int segmentLength() { return segmentLength; }
        public int termLength() { return termLength; }
        public int mtuLength() { return mtuLength; }
        public int sessionId() { return sessionId; }
        public int streamId() { return streamId; }
        public String channel() { return channel; }
        public String sourceIdentity() { return sourceIdentity; }
        public long segmentBase(long position) {
            if (position < startPosition) throw new IllegalArgumentException("Position precedes retained start");
            return AeronArchive.segmentFileBasePosition(startPosition, position, termLength, segmentLength);
        }
    }

    /** One polling owner; only owned byte arrays leave the native callback. */
    public abstract static class Reader implements AutoCloseable {
        final ArchiveBridge parent;
        final Descriptor descriptor;
        final long pin;
        final ArrayBlockingQueue<Transport.Message> queue;
        final AtomicLong owner = new AtomicLong();
        volatile boolean closed;
        long queueFull;
        Reader(ArchiveBridge parent, Descriptor descriptor, long pin, int capacity) {
            if (capacity < 1) throw new IllegalArgumentException("Queue capacity must be positive");
            this.parent = parent; this.descriptor = descriptor; this.pin = pin;
            queue = new ArrayBlockingQueue<>(capacity);
        }
        final void claimOwner() {
            long thread = Thread.currentThread().threadId();
            owner.compareAndSet(0, thread);
            if (owner.get() != thread) throw new IllegalStateException("Reader already has a different polling owner");
        }
        final ControlledFragmentHandler.Action receive(DirectBuffer buffer, int offset, int length, Header header) {
            if (queue.remainingCapacity() == 0) { queueFull++; return ControlledFragmentHandler.Action.ABORT; }
            Transport.Message raw = Transport.copyMessage(buffer, offset, length, header);
            // Replay session/stream identify the replay publication. The recording descriptor retains native source.
            Transport.Message attributed = new Transport.Message(raw.bytes, descriptor.sessionId, descriptor.streamId,
                raw.startPosition, raw.endPosition, raw.termId, raw.termOffset, raw.flags, raw.reservedValue,
                descriptor.sourceIdentity, raw.imageCorrelationId);
            if (!queue.offer(attributed)) throw new IllegalStateException("Single-producer staging invariant violated");
            return ControlledFragmentHandler.Action.CONTINUE;
        }
        public Transport.Message take() { return queue.poll(); }
        public int queued() { return queue.size(); }
        public long queueFullCount() { return queueFull; }
        public boolean isClosed() { return closed; }
        public abstract int poll(int fragmentLimit);
    }

    public static final class Replay extends Reader {
        private final long replayId, end;
        private final Subscription subscription;
        private final ControlledFragmentAssembler assembler;
        private boolean connected, complete;
        private long observedPosition;
        private String failureReason = "";
        Replay(ArchiveBridge parent, Descriptor descriptor, long start, long end, int stream, int capacity) {
            super(parent, descriptor, start, capacity);
            this.end = end;
            observedPosition = start;
            assembler = new ControlledFragmentAssembler(this::receive);
            if (start == end) { replayId = -1; subscription = null; complete = true; }
            else {
                replayId = parent.client.startReplay(descriptor.recordingId, start, end - start, IPC, stream);
                try {
                    subscription = parent.client.context().aeron().addSubscription(IPC + "|session-id=" + (int)replayId, stream);
                } catch (Throwable error) {
                    try { parent.client.stopReplay(replayId); } catch (Throwable cleanup) { error.addSuppressed(cleanup); }
                    throw error;
                }
            }
        }
        @Override public synchronized int poll(int fragmentLimit) {
            if (closed) return 0;
            claimOwner(); parent.check();
            if (!failureReason.isEmpty()) throw new IllegalStateException(failureReason);
            if (fragmentLimit < 1) throw new IllegalArgumentException("fragmentLimit must be positive");
            if (subscription == null) return 0;
            int work = subscription.controlledPoll(assembler, fragmentLimit);
            for (Image image : subscription.images()) {
                connected = true;
                observedPosition = Math.max(observedPosition, image.position());
                if (observedPosition >= end) complete = true;
                else if (image.isEndOfStream())
                    failureReason = "Replay ended at " + observedPosition + " before requested end " + end;
            }
            if (connected && !subscription.isConnected() && !complete)
                failureReason = "Replay disconnected at " + observedPosition + " before requested end " + end;
            if (!failureReason.isEmpty()) throw new IllegalStateException(failureReason);
            return work;
        }
        public boolean complete() { return complete && queue.isEmpty(); }
        public String failureReason() { return failureReason; }
        @Override public synchronized void close() {
            if (closed) return;
            closed = true;
            Throwable first = null;
            try { if (subscription != null) subscription.close(); } catch (Throwable error) { first = error; }
            try { if (replayId != -1) parent.client.stopReplay(replayId); }
            catch (Throwable error) { if (first == null) first = error; else first.addSuppressed(error); }
            finally { assembler.clear(); queue.clear(); parent.released(this); }
            if (first != null) throw new IllegalStateException("Replay close failed", first);
        }
    }

    public static final class Persistent extends Reader {
        private final PersistentSubscription subscription;
        private int liveJoined, liveLeft;
        private String lastError = "";
        Persistent(ArchiveBridge parent, Descriptor descriptor, long start, String liveChannel,
            int liveStream, int replayStream, int capacity) {
            super(parent, descriptor, start < 0 ? descriptor.startPosition : start, capacity);
            if (liveStream != descriptor.streamId) throw new IllegalArgumentException("Live stream differs from recording");
            ChannelUri liveUri = ChannelUri.parse(liveChannel);
            liveUri.put("session-id", Integer.toString(descriptor.sessionId));
            subscription = PersistentSubscription.create(new PersistentSubscription.Context()
                .aeronDirectoryName(parent.driverDirectory).recordingId(descriptor.recordingId).startPosition(start)
                .liveChannel(liveUri.toString()).liveStreamId(liveStream).replayChannel(IPC).replayStreamId(replayStream)
                .aeronArchiveContext(parent.context()).listener(new PersistentSubscriptionListener() {
                    public void onLiveJoined() { liveJoined++; }
                    public void onLiveLeft() { liveLeft++; }
                    public void onError(Exception error) { lastError = error.toString(); }
                }));
        }
        @Override public synchronized int poll(int fragmentLimit) {
            if (closed) return 0;
            claimOwner(); parent.check();
            if (fragmentLimit < 1) throw new IllegalArgumentException("fragmentLimit must be positive");
            // PersistentSubscription 1.53.1 assembles internally: deliberately no second assembler.
            return subscription.controlledPoll(this::receive, fragmentLimit);
        }
        public boolean isLive() { return subscription.isLive(); }
        public boolean isReplaying() { return subscription.isReplaying(); }
        public boolean hasFailed() { return subscription.hasFailed(); }
        public String failureReason() { return hasFailed() ? String.valueOf(subscription.failureReason()) : lastError; }
        public int liveJoinedCount() { return liveJoined; }
        public int liveLeftCount() { return liveLeft; }
        @Override public synchronized void close() {
            if (closed) return;
            closed = true;
            try { subscription.close(); } finally { queue.clear(); parent.released(this); }
        }
    }
}
