package nema.aeron;

import io.aeron.Aeron;
import io.aeron.ControlledFragmentAssembler;
import io.aeron.Image;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.logbuffer.ControlledFragmentHandler.Action;
import io.aeron.logbuffer.Header;
import io.aeron.logbuffer.LogBufferDescriptor;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.SleepingIdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

/**
 * Narrow Aeron 1.53.1 interop: callbacks copy complete application messages into a
 * bounded queue. Application retry, effects, retention and interpretation live in Flix.
 */
public final class Transport {
    private Transport() { }

    private static final AtomicLong NEXT_RESOURCE = new AtomicLong();
    private static final ConcurrentHashMap<Long, String> OPEN = new ConcurrentHashMap<>();
    private static final ConcurrentLinkedQueue<String> DIAGNOSTICS = new ConcurrentLinkedQueue<>();

    private static long register(String description) {
        long id = NEXT_RESOURCE.incrementAndGet();
        OPEN.put(id, description);
        return id;
    }

    private static void report(String scope, Throwable error) {
        DIAGNOSTICS.add(scope + ": " + error.getClass().getName() + ": " + error.getMessage());
    }

    /** Only this handle owns its driver. Client.close never closes a MediaDriver. */
    public static final class Driver implements AutoCloseable {
        public final MediaDriver nativeDriver;
        private final long resource;
        private final AtomicBoolean closed = new AtomicBoolean();

        private Driver(MediaDriver driver) {
            nativeDriver = driver;
            resource = register("driver:" + driver.aeronDirectoryName());
        }

        @Override public void close() {
            if (closed.compareAndSet(false, true)) {
                try { nativeDriver.close(); }
                finally { OPEN.remove(resource); }
            }
        }
    }

    /** An Aeron connection borrows the external driver identified by its directory. */
    public static final class Client implements AutoCloseable {
        public final Aeron nativeClient;
        private final long resource;
        private final Set<AutoCloseable> children = ConcurrentHashMap.newKeySet();
        private boolean closed;

        private Client(Aeron aeron) {
            nativeClient = aeron;
            resource = register("client:" + aeron.context().aeronDirectoryName());
        }

        private synchronized void requireOpen() {
            if (closed || nativeClient.isClosed()) throw new IllegalStateException("client is closed");
        }

        @Override public synchronized void close() {
            if (closed) return;
            closed = true;
            if (!children.isEmpty()) {
                DIAGNOSTICS.add("client close reclaimed " + children.size() + " unclosed child resources");
            }
            RuntimeException failure = null;
            for (AutoCloseable child : new ArrayList<>(children)) {
                try { child.close(); }
                catch (Exception e) {
                    report("child close", e);
                    if (failure == null) failure = new IllegalStateException("child close failed", e);
                    else failure.addSuppressed(e);
                }
            }
            if (!children.isEmpty()) {
                // A subscription deliberately remains mapped when its polling
                // worker has not joined. Closing Aeron here would bypass that
                // guard and unmap buffers underneath the remaining worker.
                closed = false;
                DIAGNOSTICS.add("client remains open: " + children.size() + " child resources failed to close");
                if (failure == null) failure = new IllegalStateException("client has unclosed child resources");
                throw failure;
            }
            try { nativeClient.close(); }
            finally { OPEN.remove(resource); }
            if (failure != null) throw failure;
        }
    }

    public static final class Pub implements AutoCloseable {
        public final Publication nativePublication;
        private final Client client;
        private final UnsafeBuffer offerBuffer = new UnsafeBuffer(new byte[0]);
        private final long resource;
        private boolean closed;

        private Pub(Client client, Publication publication) {
            this.client = client;
            nativePublication = publication;
            resource = register("publication:" + publication.registrationId());
        }

        @Override public synchronized void close() {
            if (closed) return;
            closed = true;
            try { nativePublication.close(); }
            finally { client.children.remove(this); OPEN.remove(resource); }
        }
    }

    /** Owned payload; no borrowed DirectBuffer or Header escapes the callback. */
    public static final class Message {
        public final byte[] bytes;
        public final int sessionId;
        public final int streamId;
        public final long startPosition;
        public final long endPosition;
        public final int termId;
        public final int termOffset;
        public final int flags;
        public final long reservedValue;
        public final String sourceIdentity;
        public final long imageCorrelationId;

        public Message(byte[] bytes, int sessionId, int streamId, long startPosition,
                       long endPosition, int termId, int termOffset, int flags,
                       long reservedValue, String sourceIdentity, long imageCorrelationId) {
            this.bytes = bytes;
            this.sessionId = sessionId;
            this.streamId = streamId;
            this.startPosition = startPosition;
            this.endPosition = endPosition;
            this.termId = termId;
            this.termOffset = termOffset;
            this.flags = flags;
            this.reservedValue = reservedValue;
            this.sourceIdentity = sourceIdentity;
            this.imageCorrelationId = imageCorrelationId;
        }
    }

    public static final class Sub implements AutoCloseable {
        public final Subscription nativeSubscription;
        private final Client client;
        private final ArrayBlockingQueue<Message> queue;
        private final ControlledFragmentAssembler assembler;
        private final ConcurrentLinkedQueue<Integer> unavailableSessions;
        private final long resource;
        private final AtomicLong queueFull = new AtomicLong();
        private final AtomicLong highWater = new AtomicLong();
        private volatile boolean closed;
        private Thread pollingOwner;
        private Worker worker;

        private Sub(Client client, Subscription subscription, int capacity,
                    ConcurrentLinkedQueue<Integer> unavailableSessions) {
            this.client = client;
            nativeSubscription = subscription;
            this.unavailableSessions = unavailableSessions;
            queue = new ArrayBlockingQueue<>(capacity);
            assembler = new ControlledFragmentAssembler((buffer, offset, length, header) -> {
                // Only the polling owner produces. A consumer can create capacity but
                // cannot remove it: this precheck avoids allocating on ABORT retries.
                if (queue.remainingCapacity() == 0) {
                    queueFull.incrementAndGet();
                    return Action.ABORT;
                }
                Message message = copyMessage(buffer, offset, length, header);
                if (!queue.offer(message)) throw new IllegalStateException("single producer queue invariant");
                highWater.accumulateAndGet(queue.size(), Math::max);
                return Action.CONTINUE;
            });
            resource = register("subscription:" + subscription.registrationId());
        }

        private synchronized int poll(int fragmentLimit) {
            if (closed) return 0;
            if (fragmentLimit <= 0) throw new IllegalArgumentException("fragment limit must be positive");
            Thread caller = Thread.currentThread();
            if (pollingOwner == null) pollingOwner = caller;
            if (pollingOwner != caller) throw new IllegalStateException("subscription has a different polling owner");
            Integer unavailable;
            while ((unavailable = unavailableSessions.poll()) != null) assembler.freeSessionBuffer(unavailable);
            return nativeSubscription.controlledPoll(assembler, fragmentLimit);
        }

        @Override public void close() {
            Worker toClose;
            synchronized (this) {
                if (closed) return;
                closed = true;
                toClose = worker;
            }
            // Never join a thread while holding the lock needed by poll().
            RuntimeException failure = null;
            if (toClose != null) {
                try { toClose.close(); }
                catch (RuntimeException e) { failure = e; }
                if (toClose.thread.isAlive()) {
                    // Do not unmap an image while its polling thread could still use it.
                    // Keep both resources visible, and permit an explicit later retry.
                    synchronized (this) { closed = false; }
                    throw failure == null ? new IllegalStateException("poll worker is still alive") : failure;
                }
            }
            synchronized (this) {
                try { nativeSubscription.close(); }
                catch (RuntimeException e) {
                    if (failure == null) failure = e; else failure.addSuppressed(e);
                }
                finally {
                    assembler.clear();
                    unavailableSessions.clear();
                    client.children.remove(this);
                    OPEN.remove(resource);
                }
            }
            if (failure != null) throw failure;
        }
    }

    /** Cancellation is level triggered and wakes every thread parked through this token. */
    public static final class Cancel {
        private final AtomicBoolean stopped = new AtomicBoolean();
        private final Set<Thread> parked = ConcurrentHashMap.newKeySet();
    }

    /** Optional bounded queue polling adapter. No Flix or arbitrary application callback runs here. */
    public static final class Worker implements AutoCloseable {
        private final Sub subscription;
        private final Cancel cancel;
        private final Thread thread;
        private final long resource;
        private final AtomicBoolean closed = new AtomicBoolean();
        private volatile Throwable failure;

        private Worker(Sub sub, Cancel token, long idleNanos) {
            if (idleNanos <= 0) throw new IllegalArgumentException("worker idle must be positive");
            subscription = sub;
            cancel = token;
            resource = register("poll-worker:" + sub.nativeSubscription.registrationId());
            thread = new Thread(() -> {
                try {
                    while (!cancelled(token) && !sub.closed) {
                        if (poll(sub, 32) == 0) awaitIdle(token, idleNanos);
                    }
                } catch (Throwable e) {
                    failure = e;
                    report("poll worker", e);
                    cancel(token);
                } finally {
                    synchronized (sub) {
                        if (sub.pollingOwner == Thread.currentThread()) sub.pollingOwner = null;
                    }
                }
            }, "nema-aeron-poll-" + resource);
            thread.setDaemon(true);
        }

        @Override public synchronized void close() {
            if (closed.get()) return;
            cancel(cancel);
            LockSupport.unpark(thread);
            if (Thread.currentThread() == thread) {
                throw new IllegalStateException("poll worker cannot join itself");
            }
            try {
                thread.join(2000);
                if (thread.isAlive()) {
                    DIAGNOSTICS.add("worker failed to stop within 2 seconds: " + thread.getName());
                    throw new IllegalStateException("poll worker failed to stop");
                }
                closed.set(true);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("interrupted joining poll worker", e);
            } finally {
                if (!thread.isAlive()) OPEN.remove(resource);
            }
            if (failure != null) throw new IllegalStateException("poll worker failed", failure);
        }
    }

    /** Launch in a fresh explicit directory; existing driver state is never replaced. */
    public static Driver ownedDriver(String directory) {
        Path path = Path.of(directory).toAbsolutePath().normalize();
        if (Files.exists(path)) throw new IllegalArgumentException("driver directory must be fresh: " + path);
        try { Files.createDirectories(path.getParent()); }
        catch (IOException e) { throw new IllegalStateException("create driver parent", e); }
        MediaDriver.Context context = new MediaDriver.Context()
            .aeronDirectoryName(path.toString())
            .dirDeleteOnStart(false).dirDeleteOnShutdown(false).warnIfDirectoryExists(false)
            .threadingMode(ThreadingMode.SHARED)
            .sharedIdleStrategy(new SleepingIdleStrategy(1_000_000L))
            .ipcTermBufferLength(1024 * 1024).publicationTermBufferLength(1024 * 1024)
            .ipcMtuLength(1408).mtuLength(1408)
            .errorHandler(error -> report("media driver", error));
        return new Driver(MediaDriver.launch(context));
    }

    public static Client connect(String directory) {
        Aeron.Context context = new Aeron.Context()
            .aeronDirectoryName(directory)
            .driverTimeoutMs(2000)
            .idleStrategy(new SleepingIdleStrategy(1_000_000L))
            .errorHandler(error -> report("client conductor", error))
            .subscriberErrorHandler(error -> { report("subscriber", error); throw new IllegalStateException(error); });
        return new Client(Aeron.connect(context));
    }

    public static Pub addPublication(Client client, String channel, int streamId) {
        synchronized (client) {
            client.requireOpen();
            Pub publication = new Pub(client, client.nativeClient.addPublication(channel, streamId));
            client.children.add(publication);
            return publication;
        }
    }

    /** Exclusive native publication gives each invocation a distinct publication session. */
    public static Pub addExclusivePublication(Client client, String channel, int streamId) {
        synchronized (client) {
            client.requireOpen();
            Pub publication = new Pub(client, client.nativeClient.addExclusivePublication(channel, streamId));
            client.children.add(publication);
            return publication;
        }
    }

    public static Sub addSubscription(Client client, String channel, int streamId, int queueCapacity) {
        if (queueCapacity <= 0) throw new IllegalArgumentException("queue capacity must be positive");
        synchronized (client) {
            client.requireOpen();
            ConcurrentLinkedQueue<Integer> unavailable = new ConcurrentLinkedQueue<>();
            Subscription nativeSub = client.nativeClient.addSubscription(channel, streamId,
                image -> { }, image -> unavailable.add(image.sessionId()));
            Sub sub = new Sub(client, nativeSub, queueCapacity, unavailable);
            client.children.add(sub);
            return sub;
        }
    }

    /** One offer only. Positive values are accepted byte positions; -1..-5 retain native meaning. */
    public static long offer(Pub publication, byte[] bytes) {
        synchronized (publication) {
            if (publication.closed) return Publication.CLOSED;
            publication.offerBuffer.wrap(bytes);
            return publication.nativePublication.offer(publication.offerBuffer, 0, bytes.length);
        }
    }

    public static int poll(Sub sub, int fragmentLimit) { return sub.poll(fragmentLimit); }
    public static Message take(Sub sub) { return sub.queue.poll(); }
    public static int queued(Sub sub) { return sub.queue.size(); }
    public static long queueFullCount(Sub sub) { return sub.queueFull.get(); }
    public static long queueHighWater(Sub sub) { return sub.highWater.get(); }
    public static boolean publicationConnected(Pub pub) { return pub.nativePublication.isConnected(); }
    public static boolean subscriptionConnected(Sub sub) { return sub.nativeSubscription.isConnected(); }
    /** Empty until the driver has resolved the channel's endpoint port. */
    public static String subscriptionChannel(Sub sub) {
        String resolved = sub.nativeSubscription.tryResolveChannelEndpointPort();
        return resolved == null ? "" : resolved;
    }
    public static boolean clientClosed(Client client) { return client.nativeClient.isClosed(); }
    public static int publicationSession(Pub pub) { return pub.nativePublication.sessionId(); }
    public static long publicationPosition(Pub pub) { return pub.nativePublication.position(); }
    public static int maxMessageLength(Pub pub) { return pub.nativePublication.maxMessageLength(); }
    public static int maxPayloadLength(Pub pub) { return pub.nativePublication.maxPayloadLength(); }

    public static Cancel newCancel() { return new Cancel(); }
    public static boolean cancelled(Cancel token) { return token.stopped.get(); }
    public static void cancel(Cancel token) {
        token.stopped.set(true);
        for (Thread thread : token.parked) LockSupport.unpark(thread);
    }

    public static void awaitIdle(Cancel token, long nanos) {
        if (nanos <= 0) return;
        Thread thread = Thread.currentThread();
        token.parked.add(thread);
        try {
            if (!cancelled(token)) LockSupport.parkNanos(token, nanos);
            if (thread.isInterrupted()) cancel(token);
        } finally { token.parked.remove(thread); }
    }

    public static Worker startWorker(Sub sub, Cancel cancel, long idleNanos) {
        synchronized (sub) {
            if (sub.closed) throw new IllegalStateException("subscription is closed");
            if (sub.worker != null || sub.pollingOwner != null) {
                throw new IllegalStateException("subscription polling already has an owner");
            }
            Worker worker = new Worker(sub, cancel, idleNanos);
            sub.worker = worker;
            worker.thread.start();
            return worker;
        }
    }

    public static String workerFailure(Worker worker) {
        return worker.failure == null ? "" : worker.failure.toString();
    }

    public static void releasePollOwner(Sub sub) {
        synchronized (sub) {
            if (sub.worker != null) throw new IllegalStateException("subscription has a polling worker");
            if (sub.pollingOwner != null && sub.pollingOwner != Thread.currentThread()) {
                throw new IllegalStateException("only polling owner can release ownership");
            }
            sub.pollingOwner = null;
        }
    }

    /** Copies exactly once after queue capacity has been established. Header values are scalar snapshots. */
    public static Message copyMessage(DirectBuffer buffer, int offset, int length, Header header) {
        byte[] bytes = new byte[length];
        buffer.getBytes(offset, bytes);
        long start = LogBufferDescriptor.computePosition(header.termId(), header.termOffset(),
            header.positionBitsToShift(), header.initialTermId());
        Object context = header.context();
        Image image = context instanceof Image ? (Image)context : null;
        return new Message(bytes, header.sessionId(), header.streamId(), start, header.position(),
            header.termId(), header.termOffset(), Byte.toUnsignedInt(header.flags()), header.reservedValue(),
            image == null ? "" : image.sourceIdentity(), image == null ? -1 : image.correlationId());
    }

    public static byte[] bytes(Message message) { return message.bytes; }
    public static int session(Message message) { return message.sessionId; }
    public static int stream(Message message) { return message.streamId; }
    public static long startPosition(Message message) { return message.startPosition; }
    public static long endPosition(Message message) { return message.endPosition; }
    public static String source(Message message) { return message.sourceIdentity; }
    public static int termId(Message message) { return message.termId; }
    public static int termOffset(Message message) { return message.termOffset; }
    public static int flags(Message message) { return message.flags; }
    public static long reservedValue(Message message) { return message.reservedValue; }
    public static long imageCorrelationId(Message message) { return message.imageCorrelationId; }
    public static boolean isNull(Message message) { return message == null; }
    public static long nanoTime() { return System.nanoTime(); }
    public static int openResources() { return OPEN.size(); }
    public static String resourceReport() { return OPEN.toString(); }
    public static String diagnostic() { String value = DIAGNOSTICS.poll(); return value == null ? "" : value; }
    public static void closeDriver(Driver driver) { driver.close(); }
    public static void closeClient(Client client) { client.close(); }
    public static void closePublication(Pub pub) { pub.close(); }
    public static void closeSubscription(Sub sub) { sub.close(); }
    public static void closeWorker(Worker worker) { worker.close(); }
}
