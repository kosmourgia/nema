package nema.aeron;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/** OS-only helpers for the Flix example; no transport or retention policy. */
public final class LabFiles {
    private LabFiles() { }
    public static long storageBytes(String directory) throws IOException {
        try (var files = Files.walk(Path.of(directory))) {
            long total = 0;
            for (Path file : files.filter(Files::isRegularFile).toList())
                total = Math.addExact(total, Files.size(file));
            return total;
        }
    }
    public static void haltAfterCommit() { Runtime.getRuntime().halt(73); }
    /** Pure diagnostic JSON escaping, including every JSON control character. */
    public static String jsonEscape(String value) {
        StringBuilder out = new StringBuilder();
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if (c == '"' || c == '\\') out.append('\\').append(c);
            else if (c < 0x20) {
                out.append("\\u00");
                out.append("0123456789abcdef".charAt(c >>> 4));
                out.append("0123456789abcdef".charAt(c & 15));
            } else out.append(c);
        }
        return out.toString();
    }
}
