package zygon.flix;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.StandardProtocolFamily;
import java.net.UnixDomainSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.UUID;

/** OS/JSON boundary. Flix owns typed reduction, effects, CSP and Datalog. */
public final class Bridge implements AutoCloseable {
    private static final int MAX_FRAME = 65536;
    private final SocketChannel channel;
    private final Selector selector;
    private final SelectionKey key;
    private long serial;

    public Bridge(String path) throws IOException {
        channel = SocketChannel.open(StandardProtocolFamily.UNIX);
        channel.connect(UnixDomainSocketAddress.of(path));
        channel.configureBlocking(false);
        selector = Selector.open();
        key = channel.register(selector, 0);
    }
    public static String environment(String name, String fallback) {
        String value = System.getenv(name);
        return value == null ? fallback : value;
    }
    public static String fresh(String prefix) { return prefix + UUID.randomUUID(); }
    public static void pause() throws InterruptedException { Thread.sleep(20); }
    public static void require(boolean condition, String message) throws IOException {
        if (!condition) throw new IOException(message);
    }
    private void ready(int operation, long deadline) throws IOException {
        key.interestOps(operation);
        while (true) {
            long remaining = deadline - System.nanoTime();
            if (remaining <= 0) throw new IOException("bounded socket operation timed out; write is not retried");
            int selected = selector.select(Math.max(1, remaining / 1_000_000));
            selector.selectedKeys().clear();
            if (selected > 0 && (key.readyOps() & operation) != 0) return;
        }
    }
    private synchronized Doc call(String method, JsonObject params) throws IOException {
        String id = Long.toString(++serial);
        JsonObject request = new JsonObject();
        request.addProperty("version", 1);
        request.addProperty("id", id);
        request.addProperty("method", method);
        request.add("params", params);
        byte[] encoded = (request + "\n").getBytes(StandardCharsets.UTF_8);
        if (encoded.length > MAX_FRAME) throw new IOException("outgoing frame exceeds 65536 bytes");
        long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
        ByteBuffer outbound = ByteBuffer.wrap(encoded);
        while (outbound.hasRemaining()) {
            ready(SelectionKey.OP_WRITE, deadline);
            channel.write(outbound);
        }
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        ByteBuffer one = ByteBuffer.allocate(1);
        while (true) {
            ready(SelectionKey.OP_READ, deadline);
            one.clear();
            int count = channel.read(one);
            if (count < 0) throw new EOFException("connection lost; invocation outcome may be unknown");
            if (count == 0) continue;
            byte value = one.array()[0];
            bytes.write(value);
            if (bytes.size() > MAX_FRAME) throw new IOException("incoming frame exceeds 65536 bytes");
            if (value == '\n') break;
        }
        JsonObject reply = JsonParser.parseString(bytes.toString(StandardCharsets.UTF_8)).getAsJsonObject();
        if (!reply.has("version") || reply.get("version").getAsInt() != 1 || !reply.has("id") || !reply.get("id").getAsString().equals(id))
            throw new IOException("response version/correlation mismatch");
        if (reply.has("error")) throw new IOException("zygons: " + reply.get("error"));
        if (!reply.has("result")) throw new IOException("response has no disposition");
        return new Doc(reply.getAsJsonObject("result"));
    }
    private static JsonObject identity(String id, String incarnation) {
        JsonObject ref = new JsonObject();
        ref.addProperty("id", id);
        ref.addProperty("incarnation", incarnation);
        return ref;
    }
    public Doc register(String id, String incarnation, String kind, String medium, String mode) throws IOException {
        JsonObject registration = identity(id, incarnation);
        registration.addProperty("kind", kind);
        registration.addProperty("medium", medium);
        registration.addProperty("mode", mode);
        registration.add("operations", new JsonArray());
        registration.add("parent", null);
        JsonObject params = new JsonObject();
        params.add("registration", registration);
        return call("register", params).object("registration");
    }
    public Doc inspect() throws IOException { return call("inspect", new JsonObject()); }
    public Doc subscribe(String cursor) throws IOException {
        JsonObject params = new JsonObject();
        params.addProperty("cursor", cursor);
        params.addProperty("limit", 32);
        return call("subscribe", params);
    }
    public Doc invoke(String requesterId, String requesterIncarnation, String targetId,
                      String targetIncarnation, String operation, String arguments, String correlation) throws IOException {
        JsonObject params = new JsonObject();
        params.add("requester", identity(requesterId, requesterIncarnation));
        params.add("target", identity(targetId, targetIncarnation));
        params.addProperty("operation", operation);
        params.add("arguments", JsonParser.parseString(arguments).getAsJsonObject());
        params.addProperty("correlation", correlation);
        params.addProperty("timeoutMs", 30000);
        return call("invoke", params);
    }
    public void unregister(String id, String incarnation) throws IOException {
        JsonObject params = identity(id, incarnation);
        params.addProperty("reason", "Flix participant finished");
        call("unregister", params);
    }
    @Override public void close() throws IOException { channel.close(); selector.close(); }

    /** Raw JSON remains available beside the intentionally small typed subset. */
    public static final class Doc {
        private final JsonObject value;
        public Doc(String json) { value = JsonParser.parseString(json).getAsJsonObject(); }
        private Doc(JsonObject value) { this.value = value; }
        public String string(String key) { return value.get(key).getAsString(); }
        public boolean bool(String key) { return value.get(key).getAsBoolean(); }
        public boolean has(String key) { return value.has(key) && !value.get(key).isJsonNull(); }
        public String json(String key) { JsonElement field = value.get(key); return field == null ? "null" : field.toString(); }
        public String raw() { return value.toString(); }
        public Doc object(String key) { return new Doc(value.getAsJsonObject(key)); }
        public int count(String key) { return value.getAsJsonArray(key).size(); }
        public Doc item(String key, int index) { return new Doc(value.getAsJsonArray(key).get(index).getAsJsonObject()); }
        public String stringItem(String key, int index) { return value.getAsJsonArray(key).get(index).getAsString(); }
    }
}
