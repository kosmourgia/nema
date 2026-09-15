package nema.aeron;

import java.nio.file.Files;
import java.nio.file.Path;
import io.aeron.exceptions.AeronException;

/** Focused independent regression probe for Archive ownership/control isolation. */
public final class ReviewChecks {
    public static void main(String[] args) throws Exception {
        Path root = args.length == 0 ? Path.of(System.getenv("NEMA_AERON_RUN")) : Path.of(args[0]);
        Files.createDirectories(root);
        Path run = Files.createTempDirectory(root, "archive-control-review-");
        String driverDirectory = run.resolve("driver").toString();
        Path storage = run.resolve("first");
        try (Transport.Driver driver = Transport.ownedDriver(driverDirectory)) {
            try (ArchiveBridge first = ArchiveBridge.openOwned(driverDirectory, storage.toString(), 81001, 65536, 0)) {
                try {
                    ArchiveBridge second = ArchiveBridge.openOwned(driverDirectory, run.resolve("second").toString(), 81002, 65536, 0);
                    second.close();
                    throw new AssertionError("second Archive sharing control streams was accepted");
                } catch (IllegalStateException expected) { }
                if (first.archiveId() != 81001) throw new AssertionError("Archive client identity changed");
            }
            Path catalog = storage.resolve("archive.catalog");
            Path saved = storage.resolve("archive.catalog.saved");
            Files.move(catalog, saved);
            try {
                ArchiveBridge replacement = ArchiveBridge.openOwned(driverDirectory, storage.toString(), 81001, 65536, 0);
                replacement.close();
                throw new AssertionError("recording IDs could be reused with an old incarnation");
            } catch (IllegalStateException expected) {
                if (!expected.getMessage().contains("no catalog")) throw expected;
            } finally { Files.move(saved, catalog); }
            try (ArchiveBridge restored = ArchiveBridge.openOwned(driverDirectory, storage.toString(), 81001, 65536, 0)) {
                if (restored.archiveId() != 81001) throw new AssertionError("restored identity changed");
                var errorHandler = ArchiveBridge.class.getDeclaredMethod("onError", Throwable.class);
                errorHandler.setAccessible(true);
                errorHandler.invoke(restored, new AeronException("review warning", AeronException.Category.WARN));
                boolean sawWarning = false;
                String diagnostic;
                while (!(diagnostic = restored.diagnostic()).isEmpty())
                    sawWarning |= diagnostic.contains("review warning");
                if (!restored.failure().isEmpty() || !sawWarning)
                    throw new AssertionError("warning must remain inspectable without becoming worker failure: " + restored.failure());
                errorHandler.invoke(restored, new AeronException("review failure", AeronException.Category.ERROR));
                if (!restored.failure().contains("review failure")) throw new AssertionError("error must fail worker state");
            }
        }
        System.out.println("PASS Archive control isolation, missing catalog rejects incarnation reuse, restored pair reopens run=" + run);
    }
}
