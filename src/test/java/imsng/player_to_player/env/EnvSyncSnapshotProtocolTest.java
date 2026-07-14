package imsng.player_to_player.env;

import com.google.gson.JsonObject;
import imsng.player_to_player.config.GlobalConfig;
import imsng.player_to_player.netproto.ControlConnection;
import imsng.player_to_player.netproto.ControlMessage;
import imsng.player_to_player.netproto.HandlerRegistry;
import imsng.player_to_player.netproto.MessageHandler;
import imsng.player_to_player.netproto.MessageType;
import imsng.player_to_player.netproto.Protocol;
import imsng.player_to_player.util.Sha256;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class EnvSyncSnapshotProtocolTest {

    @TempDir
    Path tempDir;

    @Test
    void manifestAndFileResponseUseTheSameSnapshotId() throws Exception {
        Path sourceRoot = tempDir.resolve("source");
        Path repositoryRoot = tempDir.resolve("repository");
        Path file = sourceRoot.resolve("world/level.dat");
        Files.createDirectories(file.getParent());
        Files.writeString(file, "snapshot-bytes", StandardCharsets.UTF_8);

        EnvironmentSnapshotManager manager = new EnvironmentSnapshotManager(
                sourceRoot, repositoryRoot, List.of(),
                Duration.ofMillis(20), Duration.ofSeconds(30), Duration.ofMinutes(10));
        try {
            manager.start();
            await(() -> manager.current() != null);

            FakeRegistry registry = new FakeRegistry();
            GlobalConfig config = new GlobalConfig();
            EnvSyncServerHandlers.register(registry, manager, config);
            FakeConnection connection = new FakeConnection();

            JsonObject manifestRequest = new JsonObject();
            manifestRequest.addProperty("target", ModPrefixResolver.Target.PRIMARY_CLIENT.name());
            registry.handle(MessageType.ENV_MANIFEST_REQUEST,
                    ControlMessage.of(MessageType.ENV_MANIFEST_REQUEST, manifestRequest),
                    connection);
            ControlMessage manifestResponse = connection.take();
            String snapshotId = manifestResponse.json().get("snapshotId").getAsString();

            JsonObject fileRequest = new JsonObject();
            fileRequest.addProperty("snapshotId", snapshotId);
            fileRequest.addProperty("path", "world/level.dat");
            fileRequest.addProperty("offset", 0);
            registry.handle(MessageType.ENV_FILE_REQUEST,
                    ControlMessage.of(MessageType.ENV_FILE_REQUEST, fileRequest), connection);
            ControlMessage fileResponse = connection.take();

            assertEquals(2, Protocol.VERSION);
            assertFalse(snapshotId.isBlank());
            assertEquals(snapshotId,
                    fileResponse.json().get("snapshotId").getAsString());
            assertArrayEquals("snapshot-bytes".getBytes(StandardCharsets.UTF_8),
                    fileResponse.binary());
        } finally {
            manager.stop();
        }
    }

    @Test
    void clientSendsManifestSnapshotIdWithEveryFileRequest() throws Exception {
        byte[] payload = "client-download".getBytes(StandardCharsets.UTF_8);
        String snapshotId = "snapshot-v2";
        EnvironmentManifest manifest = new EnvironmentManifest(Map.of(
                "config/file.txt",
                new EnvironmentManifest.Entry(Sha256.hex(payload), payload.length)));
        AtomicBoolean snapshotIdObserved = new AtomicBoolean();

        ControlConnection connection = new ControlConnection() {
            @Override
            public void send(ControlMessage message) {
            }

            @Override
            public CompletableFuture<ControlMessage> request(ControlMessage message) {
                if (message.type() == MessageType.ENV_MANIFEST_REQUEST) {
                    JsonObject json = new JsonObject();
                    json.addProperty("snapshotId", snapshotId);
                    json.addProperty("filteredHash", manifest.globalHash());
                    json.add("manifest", manifest.toJson());
                    return CompletableFuture.completedFuture(
                            ControlMessage.of(MessageType.ENV_MANIFEST, json));
                }
                if (message.type() == MessageType.ENV_FILE_REQUEST) {
                    snapshotIdObserved.set(snapshotId.equals(
                            message.json().get("snapshotId").getAsString()));
                    JsonObject json = new JsonObject();
                    json.addProperty("snapshotId", snapshotId);
                    json.addProperty("path", "config/file.txt");
                    json.addProperty("offset", 0);
                    json.addProperty("total", payload.length);
                    json.addProperty("last", true);
                    return CompletableFuture.completedFuture(
                            ControlMessage.of(MessageType.ENV_FILE_DATA, json, payload));
                }
                throw new AssertionError("意外请求: " + message.type());
            }

            @Override
            public SocketAddress remoteAddress() {
                return new InetSocketAddress("127.0.0.1", 25580);
            }

            @Override
            public UUID peerId() {
                return null;
            }

            @Override
            public void setPeerId(UUID peerId) {
            }

            @Override
            public boolean isOpen() {
                return true;
            }

            @Override
            public void close() {
            }
        };

        GlobalConfig config = new GlobalConfig();
        new EnvSyncClient(connection, config)
                .syncTo(tempDir.resolve("client-environment"),
                        ModPrefixResolver.Target.PRIMARY_CLIENT)
                .get(5, TimeUnit.SECONDS);

        assertTrue(snapshotIdObserved.get());
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

    private static final class FakeRegistry implements HandlerRegistry {
        private final Map<MessageType, MessageHandler> handlers =
                new EnumMap<>(MessageType.class);

        @Override
        public void on(MessageType type, MessageHandler handler) {
            handlers.put(type, handler);
        }

        void handle(MessageType type, ControlMessage message, ControlConnection connection) {
            handlers.get(type).handle(connection, message);
        }
    }

    private static final class FakeConnection implements ControlConnection {
        private final BlockingQueue<ControlMessage> sent = new LinkedBlockingQueue<>();

        @Override
        public void send(ControlMessage message) {
            sent.add(message);
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
            return null;
        }

        @Override
        public void setPeerId(UUID peerId) {
        }

        @Override
        public boolean isOpen() {
            return true;
        }

        @Override
        public void close() {
        }

        ControlMessage take() throws InterruptedException {
            ControlMessage message = sent.poll(5, TimeUnit.SECONDS);
            if (message == null) {
                throw new AssertionError("等待协议应答超时");
            }
            return message;
        }
    }
}
