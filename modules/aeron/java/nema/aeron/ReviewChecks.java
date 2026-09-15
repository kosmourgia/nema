package nema.aeron;

import java.nio.file.Files;
import java.nio.file.Path;

/** Focused independent regression probe for Archive ownership/control isolation. */
public final class ReviewChecks {
    public static void main(String[] args) throws Exception {
        Path root = args.length == 0 ? Path.of(System.getenv("NEMA_AERON_RUN")) : Path.of(args[0]);
        Files.createDirectories(root);
        Path run = Files.createTempDirectory(root, "archive-control-review-");
        String driverDirectory = run.resolve("driver").toString();
        try (Transport.Driver driver = Transport.ownedDriver(driverDirectory);
             ArchiveBridge first = ArchiveBridge.openOwned(driverDirectory, run.resolve("first").toString(), 81001, 65536, 0);
             ArchiveBridge second = ArchiveBridge.openOwned(driverDirectory, run.resolve("second").toString(), 81002, 65536, 0)) {
            System.out.println("first requested=81001 attached=" + first.archiveId() + " incarnation=" + first.incarnation());
            System.out.println("second requested=81002 attached=" + second.archiveId() + " incarnation=" + second.incarnation());
            if (first.archiveId() != 81001 || second.archiveId() != 81002)
                throw new AssertionError("Archive client attached to a different owned server");
        }
        System.out.println("PASS independent Archive control identity run=" + run);
    }
}
