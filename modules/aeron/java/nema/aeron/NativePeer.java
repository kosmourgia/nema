package nema.aeron;

import io.aeron.Aeron;
import io.aeron.FragmentAssembler;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import org.agrona.concurrent.SleepingIdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

/** Independent Java interoperability fixture. Intentionally never calls Transport. */
public final class NativePeer {
    private NativePeer() { }

    /** send[-owned]|receive[-owned] DRIVER CHANNEL STREAM HEX COUNT [READY_FILE] */
    public static void main(String[] args) throws Exception {
        if (args.length < 6) {
            throw new IllegalArgumentException("send[-owned]|receive[-owned] DRIVER CHANNEL STREAM HEX COUNT [READY_FILE]");
        }
        String operation = args[0];
        String directory = Path.of(args[1]).toAbsolutePath().toString();
        String channel = args[2];
        int stream = Integer.parseInt(args[3]);
        byte[] expected = HexFormat.of().parseHex(args[4]);
        int count = Integer.parseInt(args[5]);
        if (count <= 0) throw new IllegalArgumentException("count must be positive");
        MediaDriver driver = null;
        if (operation.endsWith("-owned")) {
            Path path = Path.of(directory);
            if (Files.exists(path)) throw new IllegalArgumentException("owned driver requires fresh directory");
            Files.createDirectories(path.getParent());
            driver = MediaDriver.launch(new MediaDriver.Context().aeronDirectoryName(directory)
                .dirDeleteOnStart(false).dirDeleteOnShutdown(false).warnIfDirectoryExists(false)
                .threadingMode(ThreadingMode.SHARED)
                .sharedIdleStrategy(new SleepingIdleStrategy(1_000_000L))
                .publicationTermBufferLength(1024 * 1024).ipcTermBufferLength(1024 * 1024)
                .mtuLength(1408).ipcMtuLength(1408));
        }
        try (Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(directory)
            .driverTimeoutMs(2000).idleStrategy(new SleepingIdleStrategy(1_000_000L)))) {
            long deadline = System.nanoTime() + 10_000_000_000L;
            if (operation.startsWith("send")) {
                try (Publication publication = aeron.addExclusivePublication(channel, stream)) {
                    UnsafeBuffer buffer = new UnsafeBuffer(expected);
                    for (int i = 0; i < count;) {
                        long result = publication.offer(buffer);
                        if (result > 0) i++;
                        else if (result == Publication.CLOSED || result == Publication.MAX_POSITION_EXCEEDED) {
                            throw new IllegalStateException("native offer terminal result=" + result);
                        } else pause(deadline);
                    }
                    // Give the receiver an opportunity to drain before closing the image.
                    LockSupport.parkNanos(200_000_000L);
                    System.out.println("NATIVE_SENT count=" + count + " position=" + publication.position());
                }
            } else if (operation.startsWith("receive")) {
                AtomicInteger received = new AtomicInteger();
                AtomicInteger mismatch = new AtomicInteger();
                try (Subscription subscription = aeron.addSubscription(channel, stream)) {
                    FragmentAssembler assembler = new FragmentAssembler((buffer, offset, length, header) -> {
                        byte[] bytes = new byte[length];
                        buffer.getBytes(offset, bytes);
                        if (!Arrays.equals(expected, bytes)) mismatch.incrementAndGet();
                        received.incrementAndGet();
                    });
                    if (args.length > 6) {
                        String resolved;
                        while ((resolved = subscription.tryResolveChannelEndpointPort()) == null) pause(deadline);
                        Path ready = Path.of(args[6]);
                        Path pending = ready.resolveSibling(ready.getFileName() + ".tmp");
                        Files.writeString(pending, resolved);
                        Files.move(pending, ready, StandardCopyOption.ATOMIC_MOVE);
                    }
                    while (received.get() < count) {
                        if (subscription.poll(assembler, 32) == 0) pause(deadline);
                    }
                    long quietDeadline = System.nanoTime() + 100_000_000L;
                    while (System.nanoTime() < quietDeadline) {
                        subscription.poll(assembler, 32);
                        LockSupport.parkNanos(1_000_000L);
                    }
                    if (mismatch.get() != 0 || received.get() != count) {
                        throw new AssertionError("native receiver mismatch=" + mismatch + " received=" + received);
                    }
                    System.out.println("NATIVE_RECEIVED count=" + received + " bytes=" + expected.length);
                }
            } else throw new IllegalArgumentException("unknown operation: " + operation);
        } finally { if (driver != null) driver.close(); }
    }

    private static void pause(long deadline) {
        if (System.nanoTime() >= deadline) throw new IllegalStateException("native peer deadline exceeded");
        LockSupport.parkNanos(1_000_000L);
    }
}
