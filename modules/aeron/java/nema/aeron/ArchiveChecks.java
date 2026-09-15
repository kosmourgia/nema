package nema.aeron;

import io.aeron.Aeron;
import io.aeron.ExclusivePublication;
import io.aeron.archive.client.AeronArchive;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import org.agrona.concurrent.SleepingMillisIdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

/** Bounded native producer checks, independent from the Flix publication wrapper. */
public final class ArchiveChecks {
    private static final String CHANNEL = "aeron:ipc?term-length=65536|mtu=1408";
    private static final int STREAM = 601;
    private static long deadline() { return System.nanoTime() + 12_000_000_000L; }
    private static void require(boolean condition, String message) {
        if (!condition) throw new AssertionError(message);
    }
    private static void until(BooleanSupplier predicate, String message) {
        long deadline = deadline();
        while (!predicate.getAsBoolean()) {
            if (System.nanoTime() > deadline) throw new AssertionError("Deadline: " + message);
            LockSupport.parkNanos(1_000_000);
        }
    }
    private static long offer(ExclusivePublication publication, byte[] data) {
        long deadline = deadline();
        UnsafeBuffer buffer = new UnsafeBuffer(data);
        for (;;) {
            long result = publication.offer(buffer);
            if (result > 0) return result;
            if (result < -3 || System.nanoTime() > deadline) throw new AssertionError("offer failed: " + result);
            LockSupport.parkNanos(1_000_000);
        }
    }
    private static List<Transport.Message> drain(ArchiveBridge.Replay replay) {
        ArrayList<Transport.Message> result = new ArrayList<>();
        until(() -> {
            replay.poll(16);
            Transport.Message message;
            while ((message = replay.take()) != null) result.add(message);
            return replay.complete();
        }, "bounded replay complete");
        return result;
    }
    private static void same(List<byte[]> expected, List<Transport.Message> actual, int session) {
        require(expected.size() == actual.size(), "message count " + expected.size() + " != " + actual.size());
        long previous = 0;
        for (int i = 0; i < expected.size(); i++) {
            Transport.Message message = actual.get(i);
            require(Arrays.equals(expected.get(i), message.bytes), "opaque bytes at " + i);
            require(message.sessionId == session && message.streamId == STREAM, "native source attribution");
            require(message.startPosition >= previous && message.endPosition > message.startPosition, "source range");
            previous = message.endPosition;
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length == 2 && args[0].equals("crash-child")) { crashChild(Path.of(args[1])); return; }
        ArchiveBridge.Descriptor offsetRecording = new ArchiveBridge.Descriptor("fixture", 1, 65664,
            -1, 3, 131072, 65536, 1408, 1, STREAM, CHANNEL, "fixture");
        require(offsetRecording.segmentBase(196608) == 196608,
            "segment base must account for nonzero recording start term (naive division gives 131072)");
        Path root = args.length == 0 ? Path.of(System.getenv("NEMA_AERON_RUN")) : Path.of(args[0]);
        Files.createDirectories(root);
        Path run = Files.createTempDirectory(root, "archive-");
        String driverDirectory = run.resolve("driver").toString();
        String archiveDirectory = run.resolve("archive").toString();
        ArrayList<Throwable> driverErrors = new ArrayList<>();
        try (MediaDriver driver = MediaDriver.launch(new MediaDriver.Context()
            .aeronDirectoryName(driverDirectory).dirDeleteOnStart(false).dirDeleteOnShutdown(false)
            .threadingMode(ThreadingMode.SHARED).sharedIdleStrategy(new SleepingMillisIdleStrategy(1))
            .timerIntervalNs(10_000_000).untetheredWindowLimitTimeoutNs(100_000_000)
            .untetheredLingerTimeoutNs(100_000_000).untetheredRestingTimeoutNs(100_000_000)
            .errorHandler(driverErrors::add));
             Aeron producer = Aeron.connect(new Aeron.Context().aeronDirectoryName(driverDirectory))) {
            ArrayList<byte[]> expected = new ArrayList<>();
            expected.add(new byte[] {0, 1, -1, 0, -128, 42});
            expected.add("Flix Archive — Καλημέρα 🌒".getBytes(StandardCharsets.UTF_8));
            byte[] fragmented = new byte[7000];
            for (int i = 0; i < fragmented.length; i++) fragmented[i] = (byte)(i * 37);
            expected.add(fragmented);
            long recording, end;
            int session;
            String identity;
            try (ArchiveBridge archive = ArchiveBridge.openOwned(driverDirectory, archiveDirectory, 71001, 65536, 2)) {
                identity = archive.incarnation();
                long beforeFds;
                try (var fds = Files.list(Path.of("/proc/self/fd"))) { beforeFds = fds.count(); }
                for (int attempt = 0; attempt < 20; attempt++) {
                    try (ArchiveBridge duplicate = ArchiveBridge.openOwned(driverDirectory, archiveDirectory, 71001, 65536, 2)) {
                        throw new AssertionError("same Archive directory acquired twice");
                    } catch (IllegalStateException correct) { }
                    try (ArchiveBridge duplicate = ArchiveBridge.openOwned(driverDirectory,
                        run.resolve("rejected-archive-" + attempt).toString(), 71002, 65536, 2)) {
                        throw new AssertionError("same driver control streams acquired twice");
                    } catch (IllegalStateException correct) { }
                }
                try (var fds = Files.list(Path.of("/proc/self/fd"))) {
                    require(fds.count() <= beforeFds + 2, "failed lock acquisition leaked file descriptors");
                }
                require(archive.archiveId() == 71001, "native Archive identity verified");
                System.out.println("PASS same-storage/same-driver owner rejection; repeated lock failure has no fd leak");
                long registration = archive.startRecording(CHANNEL, STREAM);
                try (ExclusivePublication publication = producer.addExclusivePublication(CHANNEL, STREAM)) {
                    session = publication.sessionId();
                    until(publication::isConnected, "recording subscription");
                    for (byte[] data : expected) offer(publication, data);
                    end = publication.position();
                    until(() -> archive.findRecording("", STREAM, publication.sessionId()) >= 0, "recording descriptor");
                    recording = archive.findRecording("", STREAM, session);
                    long recordingFinal = recording;
                    long endFinal = end;
                    until(() -> archive.progress(recordingFinal) >= endFinal, "recorded byte progress");
                    require(archive.list(0, 10).length == 1, "list recordings");
                    try (ArchiveBridge.Replay replay = archive.replay(recording, 0, end, 701, 1)) {
                        until(() -> { replay.poll(16); return replay.queued() == 1; }, "bounded queue full");
                        for (int i = 0; i < 10; i++) replay.poll(16);
                        require(replay.queueFullCount() > 0, "ABORT queue backpressure");
                        same(expected, drain(replay), session);
                    }
                    try {
                        archive.replay(recording, 1, end, 702, 1);
                        throw new AssertionError("unaligned range accepted");
                    } catch (IllegalArgumentException correct) { }
                    try {
                        archive.replay(recording, 128 + 1408, end, 702, 1);
                        throw new AssertionError("middle of fragmented message accepted");
                    } catch (IllegalArgumentException correct) { }
                    try (ArchiveBridge.Replay cancelled = archive.replay(recording, 0, end, 703, 1)) {
                        until(() -> { cancelled.poll(16); return cancelled.queued() == 1; }, "cancel during queue pressure");
                        cancelled.close(); require(cancelled.isClosed(), "replay cancellation");
                    }
                    System.out.println("PASS recording/list/progress/binary/UTF-8/fragmentation/queue ABORT/ranges/cancel");
                    // An unrelated publisher on the same stream must never enter this recording's live reader.
                    try (ExclusivePublication otherSession = producer.addExclusivePublication(CHANNEL, STREAM)) {
                        until(otherSession::isConnected, "second source session connected");
                        byte[] otherBytes = "other-publication-session".getBytes(StandardCharsets.UTF_8);
                        long otherEnd = offer(otherSession, otherBytes);
                        until(() -> archive.findRecording("", STREAM, otherSession.sessionId()) >= 0,
                            "second source recording descriptor");
                        long otherRecording = archive.findRecording("", STREAM, otherSession.sessionId());
                        require(otherRecording != recording, "distinct sessions have distinct recording ids");
                        until(() -> archive.progress(otherRecording) >= otherEnd, "second source recorded");
                        try (ArchiveBridge.Replay replay = archive.replay(otherRecording, 0, otherEnd, 849, 1)) {
                            same(List.of(otherBytes), drain(replay), otherSession.sessionId());
                        }
                        persistent(archive, publication, recording, expected);
                    }
                    System.out.println("PASS multiple publication sessions retain distinct recordings and live attribution");
                    end = publication.position();
                    long stoppedEnd = end;
                    until(() -> archive.progress(recordingFinal) >= stoppedEnd, "recorded before stop");
                    prematureReplay(archive, recording, end, driverDirectory);
                    try (ArchiveBridge.Persistent cancelled = archive.persistent(recording, -1,
                        CHANNEL + "|tether=false", STREAM, 851, 1)) {
                        until(() -> { cancelled.poll(16); return cancelled.queued() == 1; }, "persistent replay before cancellation");
                        cancelled.close();
                        require(cancelled.isClosed(), "persistent replay cancelled while staging full");
                    }
                }
                archive.stopRecording(registration);
                require(archive.openChildren() == 0, "child handles closed");
                    require(archive.failure().isEmpty(), "Archive worker healthy: " + archive.failure());
            }
            try (ArchiveBridge archive = ArchiveBridge.openOwned(driverDirectory, archiveDirectory, 71001, 65536, 2)) {
                require(archive.incarnation().equals(identity), "incarnation retained across restart");
                ArchiveBridge.Descriptor d = archive.descriptor(recording);
                require(d.stopPosition == end, "stopped position retained");
                List<Transport.Message> replayed;
                try (ArchiveBridge.Replay replay = archive.replay(recording, 0, end, 704, 4)) {
                    replayed = drain(replay);
                }
                same(expected, replayed, session);
                long preceding = 0;
                int paddingGaps = 0;
                for (Transport.Message message : replayed) {
                    require(archive.paddingOnly(recording, preceding, message.startPosition), "gap contains only actual PAD frames");
                    if (preceding < message.startPosition) paddingGaps++;
                    require(!archive.paddingOnly(recording, message.startPosition, message.endPosition), "DATA never counts as padding");
                    preceding = message.endPosition;
                }
                require(paddingGaps > 0, "actual term padding exercised");
                System.out.println("PASS retained gap validation distinguishes PAD from missing DATA; gaps=" + paddingGaps);
                long pin = 0;
                require(archive.safePurgePosition(recording, end, pin) == 0, "external replay pin prevents purge");
                require(archive.purgeSegments(recording, end, pin) == 0, "pin retains source bytes");
                long safe = archive.safePurgePosition(recording, end, end);
                require(safe > 0 && safe <= end, "safe complete segment exists");
                try (ArchiveBridge.Replay replay = archive.replay(recording, 0, end, 705, 1)) {
                    try { archive.purgeSegments(recording, end, end); throw new AssertionError("reader pin ignored"); }
                    catch (IllegalStateException correct) { }
                }
                long count = archive.purgeSegments(recording, end, end);
                require(count > 0, "front segments removed");
                require(archive.descriptor(recording).startPosition == safe, "catalog front advanced");
                try { archive.replay(recording, 0, end, 706, 1); throw new AssertionError("pruned range accepted"); }
                catch (IllegalArgumentException correct) { }
                try (ArchiveBridge.Replay replay = archive.replay(recording, safe, end, 707, 4)) {
                    require(!drain(replay).isEmpty(), "retained tail still replays");
                }
                System.out.println("PASS Archive restart/identity/retained replay/pins/front purge/pruned rejection; purged=" + count);
            }
            require(!producer.isClosed(), "borrowed driver/client remain open");
            require(driverErrors.isEmpty(), "driver errors: " + driverErrors);
        }
        crashRestart(root);
        System.out.println("PASS Archive checks run=" + run);
    }

    private static final byte[] CRASH_BYTES = new byte[] {0, -1, 17, 0, 99};

    private static void prematureReplay(ArchiveBridge archive, long recording, long end, String driverDirectory) {
        try (ArchiveBridge.Replay replay = archive.replay(recording, 0, end, 850, 1);
             AeronArchive independentControl = AeronArchive.connect(new AeronArchive.Context()
                 .aeronDirectoryName(driverDirectory).controlRequestChannel("aeron:ipc?term-length=65536")
                 .controlRequestStreamId(9100).controlResponseChannel("aeron:ipc?term-length=65536")
                 .controlResponseStreamId(9101).idleStrategy(new SleepingMillisIdleStrategy(1)))) {
            until(() -> { replay.poll(16); return replay.queued() == 1; }, "replay connected before forced stop");
            independentControl.stopAllReplays(recording);
            until(() -> {
                try { replay.poll(16); while (replay.take() != null) { } return false; }
                catch (IllegalStateException correct) { return !replay.failureReason().isEmpty(); }
            }, "premature native replay stop surfaces failure");
            require(!replay.complete(), "premature end was not successful completion");
        }
        System.out.println("PASS premature native replay stop reports failure rather than completion");
    }

    private static void crashChild(Path run) throws Exception {
        String driverDir = run.resolve("driver-before-crash").toString();
        try (MediaDriver driver = MediaDriver.launch(new MediaDriver.Context()
            .aeronDirectoryName(driverDir).threadingMode(ThreadingMode.SHARED)
            .sharedIdleStrategy(new SleepingMillisIdleStrategy(1)));
             Aeron producer = Aeron.connect(new Aeron.Context().aeronDirectoryName(driverDir));
             ArchiveBridge archive = ArchiveBridge.openOwned(driverDir, run.resolve("storage").toString(), 72001, 65536, 2)) {
            archive.startRecording(CHANNEL, STREAM);
            try (ExclusivePublication publication = producer.addExclusivePublication(CHANNEL, STREAM)) {
                until(publication::isConnected, "child Archive connection");
                long end = offer(publication, CRASH_BYTES);
                until(() -> archive.findRecording("", STREAM, publication.sessionId()) >= 0, "child recording");
                long recording = archive.findRecording("", STREAM, publication.sessionId());
                until(() -> archive.progress(recording) >= end, "child recorded progress");
                String checkpoint = recording + "\n" + end + "\n" + publication.sessionId() + "\n" + archive.incarnation();
                try (FileChannel file = FileChannel.open(run.resolve("checkpoint"),
                    StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
                    ByteBuffer bytes = StandardCharsets.UTF_8.encode(checkpoint);
                    while (bytes.hasRemaining()) file.write(bytes);
                    file.force(true);
                }
                // Real abrupt process death: no Archive, Aeron, publication, or driver close hooks run.
                Runtime.getRuntime().halt(91);
            }
        }
    }

    private static void crashRestart(Path root) throws Exception {
        Path run = Files.createTempDirectory(root, "archive-crash-");
        Process child = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
            "--add-opens", "java.base/jdk.internal.misc=ALL-UNNAMED", "--sun-misc-unsafe-memory-access=allow",
            "-XX:ActiveProcessorCount=2", "-cp", System.getProperty("java.class.path"),
            ArchiveChecks.class.getName(), "crash-child", run.toString())
            .redirectErrorStream(true).redirectOutput(run.resolve("child.log").toFile()).start();
        if (!child.waitFor(20, TimeUnit.SECONDS)) { child.destroyForcibly(); throw new AssertionError("crash child deadline"); }
        require(child.exitValue() == 91, "crash child exit=" + child.exitValue() + ": " + Files.readString(run.resolve("child.log")));
        List<String> checkpoint = Files.readAllLines(run.resolve("checkpoint"));
        long recording = Long.parseLong(checkpoint.get(0)), end = Long.parseLong(checkpoint.get(1));
        int session = Integer.parseInt(checkpoint.get(2));
        // Archive's mark-file liveness window protects against opening a still-running owner.
        // The process is known dead; let the released implementation's ten-second window expire.
        Thread.sleep(11_000);
        String driverDir = run.resolve("driver-after-crash").toString();
        try (MediaDriver driver = MediaDriver.launch(new MediaDriver.Context()
            .aeronDirectoryName(driverDir).threadingMode(ThreadingMode.SHARED)
            .sharedIdleStrategy(new SleepingMillisIdleStrategy(1)));
             ArchiveBridge archive = ArchiveBridge.openOwned(driverDir, run.resolve("storage").toString(), 72001, 65536, 2);
             ArchiveBridge.Replay replay = archive.replay(recording, 0, end, 901, 2)) {
            require(archive.incarnation().equals(checkpoint.get(3)), "crash retained incarnation");
            same(List.of(CRASH_BYTES), drain(replay), session);
        }
        System.out.println("PASS abrupt process halt(91)/Archive recovery/replay with syncLevel=2; power-loss claim excluded");
    }

    private static void persistent(ArchiveBridge archive, ExclusivePublication publication, long recording,
        ArrayList<byte[]> expected) {
        try (ArchiveBridge.Persistent subscription = archive.persistent(recording, -1,
            CHANNEL + "|tether=false", STREAM, 801, 2)) {
            ArrayList<Transport.Message> consumed = new ArrayList<>();
            until(() -> {
                subscription.poll(16);
                Transport.Message message;
                while ((message = subscription.take()) != null) consumed.add(message);
                require(!subscription.hasFailed(), subscription.failureReason());
                return subscription.isLive();
            }, "PersistentSubscription replay to IPC live");
            same(expected, consumed, publication.sessionId());
            byte[] live = "live-after-catchup".getBytes(StandardCharsets.UTF_8);
            expected.add(live); offer(publication, live);
            until(() -> {
                subscription.poll(16);
                Transport.Message message;
                while ((message = subscription.take()) != null) consumed.add(message);
                return consumed.size() == expected.size();
            }, "live bytes");
            // Stop polling; an untethered IPC reader falls behind while the Archive continues recording.
            for (int i = 0; i < 100; i++) {
                byte[] bytes = new byte[4096];
                ByteBuffer.wrap(bytes).putInt(i);
                expected.add(bytes); offer(publication, bytes);
            }
            until(() -> {
                subscription.poll(32);
                Transport.Message message;
                while ((message = subscription.take()) != null) consumed.add(message);
                require(!subscription.hasFailed(), subscription.failureReason());
                return consumed.size() == expected.size() && subscription.isLive();
            }, "fallen-behind IPC resumes through replay");
            same(expected, consumed, publication.sessionId());
            require(subscription.liveLeftCount() > 0, "actual live fall-behind transition observed");
            require(subscription.liveJoinedCount() >= 2, "live rejoined after replay");
            System.out.println("PASS PersistentSubscription IPC replay/live/fall-behind/replay/live; joins=" +
                subscription.liveJoinedCount() + " leaves=" + subscription.liveLeftCount());
        }
    }
}
