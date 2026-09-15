package nema.aeron;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.zip.CRC32C;

/** A deliberately small, single-writer durable journal for the handoff example.
 * All semantic choices (recording identity, coverage, acknowledgement, required
 * obligations and purge eligibility) remain in the Flix application.
 */
public final class DurableSink implements AutoCloseable {
    private static final int MAGIC = 0x4e534b31;
    private static final int COMMIT = 0x434d5431;
    private static final int MAX_KEY = 16 * 1024;
    private static final int MAX_BODY = 64 * 1024 * 1024;
    private static final int MIN_BODY = 28;

    private final Path directory;
    private final Path journalPath;
    private final FileChannel journal;
    private final FileLock lock;
    private final Map<String, TreeMap<Long, Entry>> ranges = new HashMap<>();
    private final Map<String, Map<String, Entry>> identities = new HashMap<>();
    private final Map<String, Entry> locators = new HashMap<>();
    private boolean closed;
    private boolean failed;
    private long recoveredTailBytes;

    private record Entry(String source, String identity, long start, long end,
                         byte[] bytes, String locator) {}

    private DurableSink(Path directory, FileChannel journal, FileLock lock) {
        this.directory = directory;
        this.journalPath = directory.resolve("records.nsk");
        this.journal = journal;
        this.lock = lock;
    }

    /** Opens an exclusively owned directory; refuses unsupported directory sync. */
    public static DurableSink open(String directoryName) throws IOException {
        Path directory = Path.of(directoryName).toAbsolutePath().normalize();
        createDirectoriesDurably(directory);
        FileChannel channel = FileChannel.open(directory.resolve("records.nsk"),
            StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
        FileLock lock = null;
        try {
            try {
                lock = channel.tryLock();
            } catch (OverlappingFileLockException error) {
                throw new IOException("sink already has a writer: " + directory, error);
            }
            if (lock == null) throw new IOException("sink already has a writer: " + directory);
            channel.force(true);
            syncDirectory(directory);
            DurableSink sink = new DurableSink(directory, channel, lock);
            sink.recover();
            return sink;
        } catch (Throwable error) {
            if (lock != null) {
                try { lock.release(); } catch (Throwable cleanup) { error.addSuppressed(cleanup); }
            }
            try { channel.close(); } catch (Throwable cleanup) { error.addSuppressed(cleanup); }
            throw error;
        }
    }

    /** sourceKey must include stable Archive incarnation and recording ID.
     * start/end are verified half-open recording byte coordinates, including any
     * padding the caller has actually established is covered by this record.
     */
    public synchronized String commit(String sourceKey, long start, long end, byte[] bytes)
        throws IOException {
        return commitRecord(sourceKey, "position:" + start, start, end, bytes);
    }

    /** Returns only after mapping, bytes and source checkpoint candidate are forced.
     * An exact retry returns the original locator without appending a new record.
     */
    public synchronized String commitRecord(String sourceKey, String recordIdentity,
                                             long start, long end, byte[] bytes)
        throws IOException {
        requireOpen();
        validate(sourceKey, recordIdentity, start, end, bytes);
        byte[] owned = bytes.clone();
        Entry existing = checkConflict(sourceKey, recordIdentity, start, end, owned);
        if (existing != null) return existing.locator();

        byte[] body = encode(sourceKey, recordIdentity, start, end, owned);
        long offset = journal.size();
        CRC32C checksum = new CRC32C();
        checksum.update(body);
        ByteBuffer frame = ByteBuffer.allocate(body.length + 20);
        frame.putInt(MAGIC).putInt(body.length).putInt(~body.length).put(body)
            .putInt((int) checksum.getValue()).putInt(COMMIT).flip();
        try {
            journal.position(offset);
            writeFully(journal, frame);
            journal.force(true);
        } catch (IOException error) {
            failed = true;
            throw new IOException("sink commit outcome unknown; close and reopen before retry", error);
        }
        Entry entry = new Entry(sourceKey, recordIdentity, start, end, owned, locatorAt(offset));
        index(entry);
        return entry.locator();
    }

    /** Highest contiguous committed boundary starting at an explicitly trusted base.
     * A hole blocks progress. A base in the middle of a stored application range is
     * invalid; it cannot establish an application-frame boundary.
     */
    public synchronized long persistedThrough(String sourceKey, long base) throws IOException {
        requireOpen();
        if (base < 0) throw new IllegalArgumentException("negative recording base");
        TreeMap<Long, Entry> sourceRanges = ranges.get(sourceKey);
        if (sourceRanges == null) return base;
        Map.Entry<Long, Entry> preceding = sourceRanges.floorEntry(base);
        if (preceding != null && preceding.getValue().start() < base && preceding.getValue().end() > base)
            throw new IllegalArgumentException("base is inside an application range");
        long through = base;
        while (true) {
            Entry next = sourceRanges.get(through);
            if (next == null) return through;
            through = next.end();
        }
    }

    /** Returns owned bytes; never returns a borrowed Aeron or internal journal buffer. */
    public synchronized byte[] lookup(String sourceKey, long start) throws IOException {
        requireOpen();
        return find(sourceKey, start).bytes().clone();
    }

    public synchronized String locator(String sourceKey, long start) throws IOException {
        requireOpen();
        return find(sourceKey, start).locator();
    }

    /** Locators resolve independently of the Archive and its retained segment files. */
    public synchronized byte[] lookupLocator(String locator) throws IOException {
        requireOpen();
        Entry entry = locators.get(locator);
        if (entry == null) throw new IOException("unknown sink locator: " + locator);
        return entry.bytes().clone();
    }

    public synchronized long recordCount() throws IOException {
        requireOpen();
        return locators.size();
    }

    public synchronized long recoveredTailBytes() { return recoveredTailBytes; }

    public static String durabilityPolicy() {
        return "one checksummed journal record per commit; FileChannel.force(true); " +
            "forced parent directory entries on creation; exclusive process writer; " +
            "complete corruption fails closed; incomplete final write quarantined";
    }

    @Override public synchronized void close() throws IOException {
        if (closed) return;
        closed = true;
        IOException error = null;
        try { lock.release(); } catch (IOException failure) { error = failure; }
        try { journal.close(); } catch (IOException failure) {
            if (error == null) error = failure; else error.addSuppressed(failure);
        }
        if (error != null) throw error;
    }

    private void requireOpen() throws IOException {
        if (closed) throw new IOException("sink is closed");
        if (failed) throw new IOException("sink failed; close and reopen before retry");
    }

    private Entry find(String source, long start) throws IOException {
        TreeMap<Long, Entry> sourceRanges = ranges.get(source);
        Entry entry = sourceRanges == null ? null : sourceRanges.get(start);
        if (entry == null) throw new IOException("no committed source range: " + source + " @ " + start);
        return entry;
    }

    private Entry checkConflict(String source, String identity, long start, long end, byte[] bytes)
        throws IOException {
        Map<String, Entry> sourceIdentities = identities.get(source);
        Entry sameIdentity = sourceIdentities == null ? null : sourceIdentities.get(identity);
        if (sameIdentity != null) {
            if (sameIdentity.start() == start && sameIdentity.end() == end && Arrays.equals(sameIdentity.bytes(), bytes))
                return sameIdentity;
            throw new IOException("conflicting source record identity: " + source + " / " + identity);
        }
        TreeMap<Long, Entry> sourceRanges = ranges.get(source);
        if (sourceRanges != null) {
            Map.Entry<Long, Entry> before = sourceRanges.floorEntry(start);
            Map.Entry<Long, Entry> after = sourceRanges.ceilingEntry(start);
            if ((before != null && before.getValue().end() > start) || (after != null && after.getKey() < end))
                throw new IOException("overlapping source ranges: " + source + " [" + start + "," + end + ")");
        }
        return null;
    }

    private void index(Entry entry) {
        ranges.computeIfAbsent(entry.source(), ignored -> new TreeMap<>()).put(entry.start(), entry);
        identities.computeIfAbsent(entry.source(), ignored -> new HashMap<>()).put(entry.identity(), entry);
        locators.put(entry.locator(), entry);
    }

    private String locatorAt(long offset) { return journalPath.toUri() + "#offset=" + offset; }

    private static void validate(String source, String identity, long start, long end, byte[] bytes) {
        if (source == null || source.isEmpty() || source.getBytes(StandardCharsets.UTF_8).length > MAX_KEY)
            throw new IllegalArgumentException("source key must contain 1..16384 UTF-8 bytes");
        if (identity == null || identity.isEmpty() || identity.getBytes(StandardCharsets.UTF_8).length > MAX_KEY)
            throw new IllegalArgumentException("record identity must contain 1..16384 UTF-8 bytes");
        if (start < 0 || end <= start) throw new IllegalArgumentException("expected positive half-open source range");
        if (bytes == null || bytes.length > MAX_BODY - MIN_BODY - 2 * MAX_KEY)
            throw new IllegalArgumentException("payload exceeds sink journal record limit");
    }

    private static byte[] encode(String source, String identity, long start, long end, byte[] bytes)
        throws IOException {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        try (DataOutputStream data = new DataOutputStream(output)) {
            putString(data, source);
            putString(data, identity);
            data.writeLong(start);
            data.writeLong(end); // committed range end is this record's checkpoint candidate
            data.writeInt(bytes.length);
            data.write(bytes);
        }
        return output.toByteArray();
    }

    private static void putString(DataOutputStream data, String value) throws IOException {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        data.writeInt(bytes.length);
        data.write(bytes);
    }

    private static String getString(DataInputStream input) throws IOException {
        int length = input.readInt();
        if (length <= 0 || length > MAX_KEY || length > input.available())
            throw new IOException("invalid sink journal string length: " + length);
        return new String(input.readNBytes(length), StandardCharsets.UTF_8);
    }

    private void recover() throws IOException {
        long length = journal.size();
        long offset = 0;
        while (offset < length) {
            if (length - offset < 12) { quarantineTail(offset, length); break; }
            ByteBuffer header = ByteBuffer.allocate(12);
            readFully(journal, header, offset);
            header.flip();
            int magic = header.getInt();
            int bodyLength = header.getInt();
            int bodyLengthComplement = header.getInt();
            if (magic != MAGIC || bodyLength < MIN_BODY || bodyLength > MAX_BODY || bodyLengthComplement != ~bodyLength)
                throw new IOException("corrupt sink journal header at byte " + offset);
            if (length - offset < (long) bodyLength + 20) { quarantineTail(offset, length); break; }
            ByteBuffer bodyAndTrailer = ByteBuffer.allocate(bodyLength + 8);
            readFully(journal, bodyAndTrailer, offset + 12);
            bodyAndTrailer.flip();
            byte[] body = new byte[bodyLength];
            bodyAndTrailer.get(body);
            int expectedChecksum = bodyAndTrailer.getInt();
            int commit = bodyAndTrailer.getInt();
            CRC32C checksum = new CRC32C();
            checksum.update(body);
            if ((int) checksum.getValue() != expectedChecksum || commit != COMMIT)
                throw new IOException("corrupt complete sink journal record at byte " + offset);
            try (DataInputStream data = new DataInputStream(new ByteArrayInputStream(body))) {
                String source = getString(data);
                String identity = getString(data);
                long start = data.readLong();
                long end = data.readLong();
                int count = data.readInt();
                if (count < 0 || count != data.available()) throw new IOException("invalid sink payload length");
                byte[] bytes = data.readNBytes(count);
                try { validate(source, identity, start, end, bytes); }
                catch (IllegalArgumentException error) { throw new IOException("invalid sink journal range", error); }
                Entry duplicate = checkConflict(source, identity, start, end, bytes);
                if (duplicate != null) throw new IOException("duplicate journal entry at byte " + offset);
                index(new Entry(source, identity, start, end, bytes, locatorAt(offset)));
            }
            offset += (long) bodyLength + 20;
        }
        journal.position(journal.size());
    }

    private void quarantineTail(long offset, long length) throws IOException {
        Path quarantine = directory.resolve("incomplete-tail-" + UUID.randomUUID() + ".bin");
        try (FileChannel copy = FileChannel.open(quarantine, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
            ByteBuffer buffer = ByteBuffer.allocate(8192);
            long position = offset;
            while (position < length) {
                buffer.clear();
                buffer.limit((int) Math.min(buffer.capacity(), length - position));
                int read = journal.read(buffer, position);
                if (read < 0) throw new EOFException("journal changed during incomplete-tail recovery");
                if (read == 0) throw new IOException("journal made no progress during recovery");
                buffer.flip();
                writeFully(copy, buffer);
                position += read;
            }
            copy.force(true);
        }
        syncDirectory(directory);
        journal.truncate(offset);
        journal.force(true);
        recoveredTailBytes = length - offset;
    }

    private static void writeFully(FileChannel channel, ByteBuffer bytes) throws IOException {
        while (bytes.hasRemaining()) {
            if (channel.write(bytes) == 0) throw new IOException("file write made no progress");
        }
    }

    private static void readFully(FileChannel channel, ByteBuffer bytes, long position) throws IOException {
        while (bytes.hasRemaining()) {
            int read = channel.read(bytes, position);
            if (read < 0) throw new EOFException("short sink journal read");
            if (read == 0) throw new IOException("file read made no progress");
            position += read;
        }
    }

    private static void createDirectoriesDurably(Path directory) throws IOException {
        ArrayList<Path> missing = new ArrayList<>();
        Path current = directory;
        while (current != null && !Files.exists(current)) {
            missing.add(current);
            current = current.getParent();
        }
        Files.createDirectories(directory);
        for (int index = missing.size() - 1; index >= 0; index--) {
            Path created = missing.get(index);
            syncDirectory(created);
            if (created.getParent() != null) syncDirectory(created.getParent());
        }
    }

    private static void syncDirectory(Path directory) throws IOException {
        try (FileChannel channel = FileChannel.open(directory, StandardOpenOption.READ)) {
            channel.force(true);
        }
    }
}
