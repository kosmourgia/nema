package nema.aeron;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

/** Bounded journal reliability fixtures; application acknowledgements live in Flix. */
public final class SinkChecks {
    private static final String SOURCE = "archive-instance=lab-A/incarnation=0001/recording=7";
    private static final byte[] BINARY = {0, 1, -1, 10, 13, 0, -128, 127};
    private static int checks;

    public static void main(String[] args) throws Exception {
        if (args.length == 2 && args[0].equals("--crash-after-commit")) {
            crashAfterCommit(Path.of(args[1]));
            return;
        }
        Path parent = args.length == 1 ? Path.of(args[0]) : Path.of(".nema");
        Files.createDirectories(parent);
        Path root = Files.createTempDirectory(parent, "sink-checks-").toAbsolutePath();
        basicAndHoles(root.resolve("basic"));
        tornTail(root.resolve("torn"), false);
        tornTail(root.resolve("torn-header"), true);
        corruptCompleteRecord(root.resolve("corrupt"));
        corruptLength(root.resolve("corrupt-length"));
        crashRecovery(root.resolve("crash"));
        System.out.println("SINK_CHECKS_PASS assertions=" + checks + " runtime=" + root);
        System.out.println("sink.policy=" + DurableSink.durabilityPolicy());
    }

    private static void basicAndHoles(Path directory) throws Exception {
        String locator;
        try (DurableSink sink = DurableSink.open(directory.toString())) {
            check(sink.persistedThrough(SOURCE, 0) == 0, "empty retained state");
            locator = sink.commit(SOURCE, 0, 64, BINARY);
            check(sink.commit(SOURCE, 0, 64, BINARY).equals(locator), "exact retry locator");
            check(sink.recordCount() == 1, "exact retry does not append");
            sink.commit(SOURCE, 128, 192, "opaque UTF-8: α🦀".getBytes(StandardCharsets.UTF_8));
            check(sink.persistedThrough(SOURCE, 0) == 64, "hole blocks maximum seen end");
            sink.commit(SOURCE, 64, 128, new byte[0]);
            check(sink.persistedThrough(SOURCE, 0) == 192, "hole closure advances through subsequent ranges");
            check(sink.persistedThrough(SOURCE, 192) == 192, "end boundary accepted");
            check(sink.persistedThrough(SOURCE + "-different-incarnation", 0) == 0, "incarnation isolation");
            byte[] returned = sink.lookup(SOURCE, 0);
            returned[0] = 99;
            check(Arrays.equals(sink.lookup(SOURCE, 0), BINARY), "owned lookup bytes");
            check(Arrays.equals(sink.lookupLocator(locator), BINARY), "locator resolves without Archive");
            expectFailure(() -> sink.commit(SOURCE, 0, 64, new byte[] {42}), "conflicting retry");
            expectFailure(() -> sink.commit(SOURCE, 32, 96, BINARY), "overlapping range");
            expectFailure(() -> sink.persistedThrough(SOURCE, 16), "base inside frame rejected");
            sink.commitRecord(SOURCE, "application-record-4", 192, 256, BINARY);
            expectFailure(() -> sink.commitRecord(SOURCE, "application-record-4", 256, 320, BINARY), "identity reused at different coordinate");
            expectFailure(() -> DurableSink.open(directory.toString()), "second writer rejected");
        }
        for (int iteration = 0; iteration < 20; iteration++) {
            try (DurableSink reopened = DurableSink.open(directory.toString())) {
                check(reopened.persistedThrough(SOURCE, 0) == 256, "retained late waiter state on reopen");
                check(reopened.locator(SOURCE, 0).equals(locator), "stable locator after reopen");
                check(Arrays.equals(reopened.lookupLocator(locator), BINARY), "reopened source locator bytes");
            }
        }
        DurableSink closed = DurableSink.open(directory.toString());
        closed.close();
        closed.close();
        expectFailure(() -> closed.lookup(SOURCE, 0), "closed handle rejects lookup");
    }

    private static void tornTail(Path directory, boolean partialHeader) throws Exception {
        try (DurableSink sink = DurableSink.open(directory.toString())) {
            sink.commit(SOURCE, 0, 64, BINARY);
        }
        Path journal = directory.resolve("records.nsk");
        long committedLength = Files.size(journal);
        byte[] tail = partialHeader ? new byte[] {0x4e, 0x53, 0x4b} :
            ByteBuffer.allocate(17).putInt(0x4e534b31).putInt(100).putInt(~100).put(new byte[] {1, 2, 3, 4, 5}).array();
        Files.write(journal, tail, StandardOpenOption.APPEND);
        try (DurableSink sink = DurableSink.open(directory.toString())) {
            check(sink.recoveredTailBytes() == tail.length, "incomplete tail reported");
            check(sink.persistedThrough(SOURCE, 0) == 64, "committed prefix survives tail repair");
            check(Files.size(journal) == committedLength, "only incomplete tail removed");
            try (var files = Files.list(directory)) {
                Path quarantine = files.filter(path -> path.getFileName().toString().startsWith("incomplete-tail-")).findFirst().orElseThrow();
                check(Arrays.equals(Files.readAllBytes(quarantine), tail), "raw incomplete tail remains inspectable");
            }
            sink.commit(SOURCE, 64, 128, BINARY);
            check(sink.persistedThrough(SOURCE, 0) == 128, "append works after recovery");
        }
    }

    private static void corruptCompleteRecord(Path directory) throws Exception {
        try (DurableSink sink = DurableSink.open(directory.toString())) { sink.commit(SOURCE, 0, 64, BINARY); }
        Path journal = directory.resolve("records.nsk");
        try (FileChannel channel = FileChannel.open(journal, StandardOpenOption.WRITE)) {
            channel.write(ByteBuffer.wrap(new byte[] {42}), 12);
            channel.force(true);
        }
        long corruptedLength = Files.size(journal);
        expectFailure(() -> DurableSink.open(directory.toString()), "complete checksum corruption fails closed");
        check(Files.size(journal) == corruptedLength, "complete corrupt record not silently truncated");
    }

    private static void crashRecovery(Path directory) throws Exception {
        Files.createDirectories(directory);
        Process process = new ProcessBuilder(
            Path.of(System.getProperty("java.home"), "bin", "java").toString(),
            "-cp", System.getProperty("java.class.path"), SinkChecks.class.getName(),
            "--crash-after-commit", directory.toString()).inheritIO().start();
        if (!process.waitFor(15, TimeUnit.SECONDS)) {
            process.destroyForcibly();
            throw new AssertionError("crash fixture exceeded 15 seconds");
        }
        check(process.exitValue() == 73, "process halted at after-commit crash point");
        check(!Files.exists(directory.resolve("notification-sent")), "notification did not occur before crash");
        try (DurableSink reopened = DurableSink.open(directory.toString())) {
            check(reopened.persistedThrough(SOURCE, 0) == 64, "retained state reconstructs ack after crash");
            check(Arrays.equals(reopened.lookup(SOURCE, 0), BINARY), "bytes and mapping recovered together");
            String locator = reopened.locator(SOURCE, 0);
            check(reopened.commit(SOURCE, 0, 64, BINARY).equals(locator), "crash retry is idempotent");
            check(reopened.recordCount() == 1, "crash retry creates no duplicate");
        }
    }

    private static void corruptLength(Path directory) throws Exception {
        try (DurableSink sink = DurableSink.open(directory.toString())) { sink.commit(SOURCE, 0, 64, BINARY); }
        Path journal = directory.resolve("records.nsk");
        try (FileChannel channel = FileChannel.open(journal, StandardOpenOption.WRITE)) {
            channel.write(ByteBuffer.allocate(4).putInt(10_000).flip(), 4);
            channel.force(true);
        }
        long corruptedLength = Files.size(journal);
        expectFailure(() -> DurableSink.open(directory.toString()), "corrupt length cannot disguise complete record as torn tail");
        check(Files.size(journal) == corruptedLength, "corrupt length preserved for diagnosis");
    }

    private static void crashAfterCommit(Path directory) throws Exception {
        DurableSink sink = DurableSink.open(directory.toString());
        sink.commit(SOURCE, 0, 64, BINARY);
        Runtime.getRuntime().halt(73); // exact injection: forced commit, before notification
        Files.writeString(directory.resolve("notification-sent"), "unreachable");
    }

    @FunctionalInterface private interface Checked { void run() throws Exception; }

    private static void expectFailure(Checked action, String name) throws Exception {
        try { action.run(); }
        catch (IOException | IllegalArgumentException expected) { checks++; return; }
        throw new AssertionError("expected failure: " + name);
    }

    private static void check(boolean condition, String name) {
        if (!condition) throw new AssertionError(name);
        checks++;
    }
}
