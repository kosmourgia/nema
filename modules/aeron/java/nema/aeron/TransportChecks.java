package nema.aeron;

import io.aeron.Publication;
import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

/** Small bounded live checks, using independent native peers for cross-process UDP. */
public final class TransportChecks {
    private final Path runtime;
    private int nextStream = 5100;

    private TransportChecks(Path runtime) { this.runtime = runtime; }

    public static void main(String[] args) throws Exception {
        String defaultRoot = System.getenv("NEMA_AERON_RUN");
        if (defaultRoot == null) defaultRoot = ".nema/transport-checks-" + System.nanoTime();
        boolean runMdc = args.length > 0 && args[args.length - 1].equals("mdc");
        Path root = Path.of(args.length == 0 || (args.length == 1 && runMdc) ? defaultRoot : args[0]);
        Files.createDirectories(root);
        TransportChecks checks = new TransportChecks(root.toAbsolutePath());
        checks.binaryFanout();
        checks.fullQueue();
        checks.pollOwnershipAndWorkers();
        checks.workerFailureCleanup();
        checks.interruptedWorkerJoin();
        checks.disconnectedAndBackpressure();
        checks.borrowedAndRepeated();
        checks.independentUdp();
        checks.driverFailure();
        checks.idleAndThroughput();
        if (runMdc) checks.mdc();
        check(Transport.openResources() == 0, "resource leak: " + Transport.resourceReport());
        String diagnostic;
        while (!(diagnostic = Transport.diagnostic()).isEmpty()) System.out.println("DIAGNOSTIC " + diagnostic);
        System.out.println("PASS transport-suite runtime=" + root);
    }

    private void binaryFanout() throws Exception {
        String directory = directory("fanout");
        int stream = nextStream++;
        try (var driver = Transport.ownedDriver(directory);
             var client = Transport.connect(directory);
             var sub1 = Transport.addSubscription(client, "aeron:ipc", stream, 16);
             var sub2 = Transport.addSubscription(client, "aeron:ipc", stream, 16);
             var pub1 = Transport.addExclusivePublication(client, "aeron:ipc", stream);
             var pub2 = Transport.addExclusivePublication(client, "aeron:ipc", stream)) {
            connected(pub1, sub1, sub2);
            connected(pub2, sub1, sub2);
            check(Transport.publicationSession(pub1) != Transport.publicationSession(pub2), "sessions must differ");
            byte[][] payloads = {new byte[]{0, -1, 0, 1, 127, -128},
                "Flix → Aeron · καλημέρα · 🦑".getBytes(StandardCharsets.UTF_8), randomBytes(10_000)};
            Map<Integer, List<Long>> ends = new HashMap<>();
            for (var pub : List.of(pub1, pub2)) {
                List<Long> positions = new ArrayList<>();
                for (byte[] bytes : payloads) positions.add(offer(pub, bytes));
                ends.put(Transport.publicationSession(pub), positions);
            }
            for (var sub : List.of(sub1, sub2)) {
                List<Transport.Message> messages = receive(sub, 6);
                Map<Integer, Integer> seen = new HashMap<>();
                Map<Integer, Long> previous = new HashMap<>();
                for (var message : messages) {
                    int n = seen.getOrDefault(message.sessionId, 0);
                    check(n < 3 && Arrays.equals(payloads[n], message.bytes), "bytes/session order");
                    check(message.startPosition == previous.getOrDefault(message.sessionId, 0L), "frame range start");
                    check(message.endPosition == ends.get(message.sessionId).get(n), "offer/header coordinate equality");
                    check(message.streamId == stream && !message.sourceIdentity.isEmpty(), "native attribution");
                    seen.put(message.sessionId, n + 1);
                    previous.put(message.sessionId, message.endPosition);
                }
                check(seen.size() == 2, "two publisher sessions observed");
                check(Transport.take(sub) == null, "no duplicate message");
            }
        }
        passed("ipc binary UTF8 fragmentation two-session fanout", "two subscribers, six messages each");
    }

    private void fullQueue() throws Exception {
        String directory = directory("queue");
        int stream = nextStream++;
        try (var driver = Transport.ownedDriver(directory);
             var client = Transport.connect(directory);
             var sub = Transport.addSubscription(client, "aeron:ipc", stream, 1);
             var pub = Transport.addExclusivePublication(client, "aeron:ipc", stream)) {
            connected(pub, sub);
            byte[] first = {7};
            byte[] fragmented = randomBytes(19_000);
            offer(pub, first);
            offer(pub, fragmented);
            for (int i = 0; i < 30; i++) Transport.poll(sub, 100);
            check(Transport.queued(sub) == 1 && Transport.queueHighWater(sub) == 1, "bounded staging");
            check(Transport.queueFullCount(sub) > 0, "full queue ABORT observed");
            check(Arrays.equals(first, Transport.take(sub).bytes), "first retained");
            var second = receive(sub, 1).get(0);
            check(Arrays.equals(fragmented, second.bytes), "ABORT final fragment then retry exact bytes");
            for (int i = 0; i < 10; i++) Transport.poll(sub, 100);
            check(Transport.take(sub) == null, "ABORT did not enqueue twice");
            passed("controlled queue", "capacity=1 highWater=1 ABORTs=" + Transport.queueFullCount(sub));
        }
    }

    private void pollOwnershipAndWorkers() throws Exception {
        String directory = directory("workers");
        int stream = nextStream++;
        try (var driver = Transport.ownedDriver(directory);
             var client = Transport.connect(directory);
             var sub = Transport.addSubscription(client, "aeron:ipc", stream, 2)) {
            Transport.poll(sub, 1);
            AtomicReference<Throwable> error = new AtomicReference<>();
            Thread other = new Thread(() -> {
                try { Transport.poll(sub, 1); } catch (Throwable e) { error.set(e); }
            });
            other.start(); other.join(1000);
            check(error.get() instanceof IllegalStateException, "second polling owner rejected");
            Transport.releasePollOwner(sub);
            var token = Transport.newCancel();
            var worker = Transport.startWorker(sub, token, 10_000_000_000L);
            Thread.sleep(30);
            long before = System.nanoTime();
            Transport.cancel(token);
            worker.close();
            long millis = Duration.ofNanos(System.nanoTime() - before).toMillis();
            check(millis < 500, "cancel wakes idle worker immediately");
            check(Transport.workerFailure(worker).isEmpty(), "worker success report");
            passed("poll ownership and idle cancellation", "10s park cancelled in " + millis + "ms");
        }
        // Closing the subscription itself must also wake and join its worker.
        try (var driver = Transport.ownedDriver(directory("scope-workers"));
             var client = Transport.connect(driver.nativeDriver.aeronDirectoryName())) {
            var sub = Transport.addSubscription(client, "aeron:ipc", nextStream++, 1);
            Transport.startWorker(sub, Transport.newCancel(), 10_000_000_000L);
            Thread.sleep(20);
            long before = System.nanoTime();
            sub.close();
            check(System.nanoTime() - before < 500_000_000L, "scope exit wakes worker");
        }
    }

    private void disconnectedAndBackpressure() throws Exception {
        String directory = directory("backpressure");
        int stream = nextStream++;
        try (var driver = Transport.ownedDriver(directory);
             var client = Transport.connect(directory);
             var pub = Transport.addExclusivePublication(client, "aeron:ipc?term-length=65536", stream)) {
            check(Transport.offer(pub, new byte[]{1}) == Publication.NOT_CONNECTED, "disconnected code");
            try (var sub = Transport.addSubscription(client, "aeron:ipc", stream, 1)) {
                connected(pub, sub);
                byte[] bytes = randomBytes(4096);
                long deadline = deadline();
                long result;
                do {
                    result = Transport.offer(pub, bytes);
                    if (result == Publication.ADMIN_ACTION) LockSupport.parkNanos(1_000_000L);
                    check(System.nanoTime() < deadline, "bounded pressure injection");
                } while (result != Publication.BACK_PRESSURED);
                var cancel = Transport.newCancel();
                AtomicReference<Long> outcome = new AtomicReference<>();
                Thread retry = new Thread(() -> {
                    while (!Transport.cancelled(cancel)) {
                        long attempt = Transport.offer(pub, bytes);
                        if (attempt > 0) { outcome.set(attempt); return; }
                        Transport.awaitIdle(cancel, 10_000_000_000L);
                    }
                    outcome.set(-99L);
                });
                retry.start(); Thread.sleep(20);
                Transport.cancel(cancel);
                retry.join(500);
                check(!retry.isAlive() && Long.valueOf(-99).equals(outcome.get()), "cancelled backpressure retry");
            }
            pub.close();
            check(Transport.offer(pub, new byte[]{1}) == Publication.CLOSED, "closed result code");
        }
        passed("disconnected slow-subscriber backpressure cancellation", "native -1/-2/-4 observed");
    }

    private void workerFailureCleanup() throws Exception {
        String directory = directory("worker-failed-cleanup");
        try (var driver = Transport.ownedDriver(directory);
             var client = Transport.connect(directory)) {
            var sub = Transport.addSubscription(client, "aeron:ipc", nextStream++, 1);
            var token = Transport.newCancel();
            var worker = Transport.startWorker(sub, token, 10_000_000_000L);
            // Inject a retained worker error to exercise the cleanup error path.
            // This is synthetic failure evidence, distinct from the real driver-loss test.
            var failureField = Transport.Worker.class.getDeclaredField("failure");
            failureField.setAccessible(true);
            failureField.set(worker, new IllegalStateException("injected retained worker failure"));
            boolean failed = false;
            try { sub.close(); }
            catch (IllegalStateException e) { failed = e.getMessage().contains("poll worker failed"); }
            check(failed, "worker failure propagated from scope exit");
            check(sub.nativeSubscription.isClosed(), "failed-worker subscription closed");
            check(Transport.openResources() == 2, "failed-worker cleanup leaves only driver/client");
            sub.close();
        }
        passed("worker failure cleanup", "synthetic retained failure reported; joined worker and subscription reclaimed");
    }

    private void interruptedWorkerJoin() throws Exception {
        String directory = directory("worker-unjoined-cleanup");
        try (var driver = Transport.ownedDriver(directory)) {
            var client = Transport.connect(directory);
            var sub = Transport.addSubscription(client, "aeron:ipc", nextStream++, 1);
            Transport.startWorker(sub, Transport.newCancel(), 10_000_000_000L);
            Thread.sleep(10);
            boolean failed = false;
            // Hold the monitor that the real worker needs for its finally block,
            // then interrupt the closing thread's join. No fake native worker.
            synchronized (sub) {
                Thread.currentThread().interrupt();
                try { client.close(); }
                catch (IllegalStateException expected) { failed = true; }
                finally { Thread.interrupted(); }
                check(failed, "interrupted worker join propagated");
                check(!client.nativeClient.isClosed(), "live child keeps borrowed client mapped");
                check(!sub.nativeSubscription.isClosed(), "unjoined worker keeps subscription mapped");
                check(Transport.openResources() == 4, "driver/client/sub/worker remain inspectable");
            }
            client.close();
            check(Transport.openResources() == 1, "retry joins and cleans client children");
        }
        passed("interrupted worker join", "client/sub mappings retained until successful close retry");
    }

    private void borrowedAndRepeated() throws Exception {
        String directory = directory("borrowed");
        try (var driver = Transport.ownedDriver(directory)) {
            for (int i = 0; i < 8; i++) {
                try (var client = Transport.connect(directory);
                     var sub = Transport.addSubscription(client, "aeron:ipc", nextStream, 1);
                     var pub = Transport.addExclusivePublication(client, "aeron:ipc", nextStream)) {
                    connected(pub, sub);
                    offer(pub, new byte[]{(byte)i});
                    check(receive(sub, 1).get(0).bytes[0] == i, "repeated scoped delivery");
                }
                nextStream++;
                check(Transport.openResources() == 1, "only owner driver survives borrower close");
            }
            var forgotten = Transport.connect(directory);
            Transport.addSubscription(forgotten, "aeron:ipc", nextStream++, 1);
            forgotten.close();
            check(Transport.openResources() == 1, "client reclaims leaked child and reports it");
        }
        passed("borrowed driver and repeated scope cleanup", "8 open/close cycles, resource count returns to zero");
    }

    private void independentUdp() throws Exception {
        int stream = nextStream++;
        String channel = "aeron:udp?endpoint=127.0.0.1:0";
        byte[] bytes = randomBytes(10_000);
        String hex = HexFormat.of().formatHex(bytes);
        Path ready = runtime.resolve("native-consumer.ready");
        Process receiver = nativePeer("receive-owned", directory("native-receiver"), channel,
            Integer.toString(stream), hex, "3", ready.toString());
        try {
            waitFile(ready, receiver);
            channel = Files.readString(ready);
            String directory = directory("bridge-udp-sender");
            try (var driver = Transport.ownedDriver(directory);
                 var client = Transport.connect(directory);
                 var pub = Transport.addExclusivePublication(client, channel, stream)) {
                for (int i = 0; i < 3; i++) offer(pub, bytes);
                processSucceeded(receiver, "native UDP receiver");
            }
        } finally { stop(receiver); }

        String directory = directory("bridge-udp-receiver");
        channel = "aeron:udp?endpoint=127.0.0.1:0";
        try (var driver = Transport.ownedDriver(directory);
             var client = Transport.connect(directory);
             var sub = Transport.addSubscription(client, channel, stream, 4)) {
            Process sender = nativePeer("send-owned", directory("native-sender"), resolvedChannel(sub),
                Integer.toString(stream), hex, "3");
            try {
                for (var message : receive(sub, 3)) check(Arrays.equals(bytes, message.bytes), "native producer bytes");
                processSucceeded(sender, "native UDP sender");
            } finally { stop(sender); }
        }
        passed("independent process UDP", "native producer→bridge and bridge→native consumer, 10KB fragmented bytes");
    }

    private void driverFailure() throws Exception {
        String directory = directory("failed-driver");
        var driver = Transport.ownedDriver(directory);
        try (var client = Transport.connect(directory);
             var pub = Transport.addExclusivePublication(client, "aeron:ipc", nextStream++)) {
            while (!Transport.diagnostic().isEmpty()) { }
            driver.close();
            long deadline = System.nanoTime() + 6_000_000_000L;
            String found = "";
            while (System.nanoTime() < deadline) {
                String diagnostic = Transport.diagnostic();
                if (diagnostic.contains("DriverTimeoutException")) { found = diagnostic; break; }
                Thread.sleep(10);
            }
            check(!found.isEmpty(), "driver failure diagnostic captured");
            passed("driver failure", found);
        } finally { driver.close(); }
    }

    private void idleAndThroughput() throws Exception {
        String directory = directory("measure");
        int stream = nextStream++;
        try (var driver = Transport.ownedDriver(directory);
             var client = Transport.connect(directory);
             var sub = Transport.addSubscription(client, "aeron:ipc", stream, 64);
             var pub = Transport.addExclusivePublication(client, "aeron:ipc", stream)) {
            connected(pub, sub);
            var token = Transport.newCancel();
            var worker = Transport.startWorker(sub, token, 1_000_000L);
            var bean = (com.sun.management.OperatingSystemMXBean)ManagementFactory.getOperatingSystemMXBean();
            long cpu = bean.getProcessCpuTime();
            long wall = System.nanoTime();
            Thread.sleep(500);
            double idleCpu = (bean.getProcessCpuTime() - cpu) / (double)(System.nanoTime() - wall);
            check(idleCpu < 0.7, "unattended idle must not consume a CPU core");
            byte[] payload = randomBytes(256);
            long before = System.nanoTime();
            int sent = 0, received = 0;
            int count = 5000;
            long deadline = deadline();
            while (received < count) {
                if (sent < count && Transport.offer(pub, payload) > 0) sent++;
                Transport.Message message;
                while ((message = Transport.take(sub)) != null) {
                    check(Arrays.equals(payload, message.bytes), "throughput bytes");
                    received++;
                }
                check(System.nanoTime() < deadline, "throughput check deadline");
                if (sent == count && received < count) LockSupport.parkNanos(100_000L);
            }
            double seconds = (System.nanoTime() - before) / 1e9;
            worker.close();
            passed("diagnostic measurement", String.format("idle CPU %.3f cores; %,d x 256B in %.3fs; queue highWater=%d/64 ABORTs=%d",
                idleCpu, count, seconds, Transport.queueHighWater(sub), Transport.queueFullCount(sub)));
        }
    }

    private void mdc() throws Exception {
        String directory = directory("mdc");
        int stream = nextStream++;
        String endpoint1 = "aeron:udp?endpoint=127.0.0.1:0";
        String endpoint2 = "aeron:udp?endpoint=127.0.0.1:0";
        try (var driver = Transport.ownedDriver(directory);
             var client = Transport.connect(directory);
             var sub1 = Transport.addSubscription(client, endpoint1, stream, 2);
             var sub2 = Transport.addSubscription(client, endpoint2, stream, 2);
             var pub = Transport.addExclusivePublication(client, "aeron:udp?control-mode=manual", stream)) {
            pub.nativePublication.addDestination(resolvedChannel(sub1));
            pub.nativePublication.addDestination(resolvedChannel(sub2));
            connected(pub, sub1, sub2);
            byte[] bytes = randomBytes(4096);
            offer(pub, bytes);
            check(Arrays.equals(receive(sub1, 1).get(0).bytes, bytes), "MDC destination 1");
            check(Arrays.equals(receive(sub2, 1).get(0).bytes, bytes), "MDC destination 2");
            passed("optional MDC", "manual UDP two destinations supported on loopback");
        }
    }

    private String directory(String name) { return runtime.resolve(name + "-" + System.nanoTime()).toString(); }
    private static long deadline() { return System.nanoTime() + 10_000_000_000L; }
    private static byte[] randomBytes(int length) { byte[] bytes = new byte[length]; new Random(813).nextBytes(bytes); return bytes; }
    private static void check(boolean condition, String message) { if (!condition) throw new AssertionError(message); }
    private static void passed(String name, String evidence) { System.out.println("PASS " + name + " — " + evidence); }

    private static long offer(Transport.Pub publication, byte[] bytes) {
        long deadline = deadline();
        while (System.nanoTime() < deadline) {
            long result = Transport.offer(publication, bytes);
            if (result > 0) return result;
            check(result != Publication.CLOSED && result != Publication.MAX_POSITION_EXCEEDED, "terminal offer " + result);
            LockSupport.parkNanos(1_000_000L);
        }
        throw new AssertionError("offer deadline");
    }

    private static void connected(Transport.Pub pub, Transport.Sub... subscriptions) {
        long deadline = deadline();
        while (System.nanoTime() < deadline) {
            boolean connected = Transport.publicationConnected(pub);
            for (var sub : subscriptions) connected &= Transport.subscriptionConnected(sub);
            if (connected) { LockSupport.parkNanos(10_000_000L); return; }
            LockSupport.parkNanos(1_000_000L);
        }
        throw new AssertionError("connection deadline");
    }

    private static List<Transport.Message> receive(Transport.Sub sub, int count) {
        List<Transport.Message> messages = new ArrayList<>();
        long deadline = deadline();
        while (messages.size() < count && System.nanoTime() < deadline) {
            int fragments = Transport.poll(sub, 100);
            Transport.Message message;
            while ((message = Transport.take(sub)) != null) messages.add(message);
            if (fragments == 0) LockSupport.parkNanos(1_000_000L);
        }
        check(messages.size() == count, "receive count=" + messages.size() + " expected=" + count);
        return messages;
    }

    private static String resolvedChannel(Transport.Sub sub) {
        long deadline = deadline();
        String channel;
        while ((channel = Transport.subscriptionChannel(sub)).isEmpty()) {
            check(System.nanoTime() < deadline, "channel endpoint resolution deadline");
            LockSupport.parkNanos(1_000_000L);
        }
        return channel;
    }

    private Process nativePeer(String... args) throws Exception {
        List<String> command = new ArrayList<>(List.of("java", "--add-opens=java.base/jdk.internal.misc=ALL-UNNAMED",
            "-cp", System.getProperty("java.class.path"), NativePeer.class.getName()));
        command.addAll(List.of(args));
        Path output = runtime.resolve("native-" + args[0] + "-" + System.nanoTime() + ".log");
        return new ProcessBuilder(command).redirectErrorStream(true).redirectOutput(output.toFile()).start();
    }

    private static void waitFile(Path file, Process process) throws Exception {
        long deadline = deadline();
        while (!Files.exists(file)) {
            check(process.isAlive(), "native consumer exited before ready");
            check(System.nanoTime() < deadline, "native consumer readiness deadline");
            Thread.sleep(2);
        }
    }

    private static void processSucceeded(Process process, String name) throws Exception {
        check(process.waitFor(12, TimeUnit.SECONDS), name + " deadline");
        check(process.exitValue() == 0, name + " failed exit=" + process.exitValue());
    }

    private static void stop(Process process) throws Exception {
        if (process.isAlive()) {
            process.destroy();
            if (!process.waitFor(2, TimeUnit.SECONDS)) {
                process.destroyForcibly();
                process.waitFor(2, TimeUnit.SECONDS);
            }
        }
    }
}
