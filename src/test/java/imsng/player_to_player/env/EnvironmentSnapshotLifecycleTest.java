package imsng.player_to_player.env;

import com.google.gson.JsonObject;
import imsng.player_to_player.compute.ComputeTable;
import imsng.player_to_player.config.GlobalConfig;
import imsng.player_to_player.netproto.ControlConnection;
import imsng.player_to_player.netproto.ControlMessage;
import imsng.player_to_player.netproto.MessageType;
import imsng.player_to_player.netproto.Protocol;
import imsng.player_to_player.server.HelloHandler;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class EnvironmentSnapshotLifecycleTest {

    @TempDir
    Path tempDir;

    @Test
    void helloAdvertisesCurrentImmutableSnapshot() throws Exception {
        Path sourceRoot = tempDir.resolve("source");
        Path file = sourceRoot.resolve("world/level.dat");
        Files.createDirectories(file.getParent());
        Files.writeString(file, "world-bootstrap", StandardCharsets.UTF_8);

        EnvironmentSnapshotManager manager = new EnvironmentSnapshotManager(
                sourceRoot, tempDir.resolve("repository"), List.of(),
                Duration.ofMillis(20), Duration.ofSeconds(30), Duration.ofMinutes(10));
        try {
            manager.start();
            await(() -> manager.current() != null);

            CapturingConnection connection = new CapturingConnection();
            HelloHandler handler = new HelloHandler(
                    new GlobalConfig(), new ComputeTable(), manager, "world");
            JsonObject hello = new JsonObject();
            hello.addProperty("version", Protocol.VERSION);
            hello.addProperty("clientId", UUID.randomUUID().toString());
            hello.addProperty("playerName", "test-player");
            hello.addProperty("mode", "client");

            handler.handle(connection, ControlMessage.of(MessageType.HELLO, hello));

            assertTrue(connection.sent.json().get("envReady").getAsBoolean());
            assertEquals(manager.current().snapshotId(),
                    connection.sent.json().get("envHash").getAsString());
        } finally {
            manager.stop();
        }
    }

    private static void await(BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
        while (!condition.getAsBoolean() && System.nanoTime() < deadline) {
            Thread.sleep(20);
        }
        if (!condition.getAsBoolean()) {
            throw new AssertionError("等待条件超时");
        }
    }

    private static final class CapturingConnection implements ControlConnection {
        private ControlMessage sent;
        private UUID peerId;

        @Override
        public void send(ControlMessage message) {
            sent = message;
        }

        @Override
        public CompletableFuture<ControlMessage> request(ControlMessage message) {
            throw new UnsupportedOperationException();
        }

        @Override
        public SocketAddress remoteAddress() {
            return new InetSocketAddress("127.0.0.1", 25580);
        }

        @Override
        public UUID peerId() {
            return peerId;
        }

        @Override
        public void setPeerId(UUID peerId) {
            this.peerId = peerId;
        }

        @Override
        public boolean isOpen() {
            return true;
        }

        @Override
        public void close() {
        }
    }
}
