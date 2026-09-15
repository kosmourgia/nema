package nema.aeron;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HexFormat;

/** Tiny byte/file helpers. Transport operations and polling stay in Flix. */
public final class InteropSupport {
    private InteropSupport() { }

    public static byte[] decodeHex(String hex) { return HexFormat.of().parseHex(hex); }
    public static boolean exists(String path) { return Files.exists(Path.of(path)); }
    public static String read(String path) throws Exception { return Files.readString(Path.of(path)).strip(); }
    public static void ready(String path, String value) throws Exception {
        Files.writeString(Path.of(path), value + "\n");
    }
    public static long nanoTime() { return System.nanoTime(); }
    public static void fail(String message) { throw new IllegalStateException(message); }

    /** Deterministic opaque binary plus UTF-8, large enough to fragment on UDP. */
    public static byte[] payload() {
        byte[] bytes = new byte[16_384];
        for (int i = 0; i < bytes.length; i++) bytes[i] = (byte)(i * 31 + 7);
        byte[] utf8 = "Nema λ / 雪 / \u0000 opaque".getBytes(StandardCharsets.UTF_8);
        System.arraycopy(utf8, 0, bytes, 17, utf8.length);
        return bytes;
    }
    public static void main(String[] args) { System.out.println(HexFormat.of().formatHex(payload())); }
}
