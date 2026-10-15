package imsng.player_to_player.server;

import com.google.gson.JsonObject;
import imsng.player_to_player.compute.ComputeTable;
import imsng.player_to_player.config.GlobalConfig;
import imsng.player_to_player.env.EnvironmentSnapshotManager;
import imsng.player_to_player.netproto.ControlConnection;
import imsng.player_to_player.netproto.ControlMessage;
import imsng.player_to_player.netproto.MessageType;
import imsng.player_to_player.netproto.Protocol;
import imsng.player_to_player.registry.PlayerTable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.SocketAddress;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class HelloSessionGuardTest {

    private static final UUID CLIENT =
            UUID.fromString("9207ce8d-2b04-4fd8-a9f7-a437ce1211a0");

    @TempDir
    Path temporary;

    @AfterEach
    void clearOnlineSessions() {
        HelloHandler.clearAll();
    }

    @Test
    void oldConnectionCannotWriteAfterReplacementHello() {
        HelloHandler handler = handler();
        FakeConnection oldConnection = new FakeConnection();
        FakeConnection newConnection = new FakeConnection();
        handler.handle(oldConnection, hello());
        assertTrue(HelloHandler.withCurrentSession(oldConnection, () -> true));

        handler.handle(newConnection, hello());

        assertFalse(HelloHandler.unregisterIfCurrent(CLIENT, oldConnection));
        assertFalse(HelloHandler.withCurrentSession(oldConnection, () -> true));
        assertTrue(HelloHandler.withCurrentSession(newConnection, () -> true));
    }

    @Test
    void replacementHelloWaitsForStartedOldSessionWrite() throws Exception {
        HelloHandler handler = handler();
        FakeConnection oldConnection = new FakeConnection();
        FakeConnection newConnection = new FakeConnection();
        handler.handle(oldConnection, hello());
        CountDownLatch writeEntered = new CountDownLatch(1);
        CountDownLatch allowWrite = new CountDownLatch(1);

        CompletableFuture<Boolean> oldWrite = CompletableFuture.supplyAsync(() ->
                HelloHandler.withCurrentSession(oldConnection, () -> {
                    writeEntered.countDown();
                    try {
                        return allowWrite.await(5, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return false;
                    }
                }));
        assertTrue(writeEntered.await(5, TimeUnit.SECONDS));
        CompletableFuture<Void> replacement = CompletableFuture.runAsync(
                () -> handler.handle(newConnection, hello()));
        assertFalse(replacement.isDone());

        allowWrite.countDown();

        assertTrue(oldWrite.get(5, TimeUnit.SECONDS));
        replacement.get(5, TimeUnit.SECONDS);
        assertTrue(HelloHandler.withCurrentSession(newConnection, () -> true));
    }

    @Test
    void replacementHelloWaitsForDisconnectStatePublication() throws Exception {
        HelloHandler handler = handler();
        FakeConnection oldConnection = new FakeConnection();
        CountDownLatch replacementIdentified = new CountDownLatch(1);
        FakeConnection newConnection = new FakeConnection(replacementIdentified);
        handler.handle(oldConnection, hello());
        CountDownLatch publicationEntered = new CountDownLatch(1);
        CountDownLatch allowPublication = new CountDownLatch(1);
        AtomicBoolean published = new AtomicBoolean();

        CompletableFuture<Boolean> removal = CompletableFuture.supplyAsync(() ->
                HelloHandler.unregisterIfCurrent(CLIENT, oldConnection, () -> {
                    publicationEntered.countDown();
                    try {
                        if (!allowPublication.await(5, TimeUnit.SECONDS)) {
                            throw new IllegalStateException("等待测试发布许可超时");
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException("等待测试发布许可被中断", e);
                    }
                    published.set(true);
                }));
        assertTrue(publicationEntered.await(5, TimeUnit.SECONDS));

        CompletableFuture<Void> replacement = CompletableFuture.runAsync(
                () -> handler.handle(newConnection, hello()));
        assertTrue(replacementIdentified.await(5, TimeUnit.SECONDS));
        assertFalse(replacement.isDone());
        assertFalse(published.get());

        allowPublication.countDown();

        assertTrue(removal.get(5, TimeUnit.SECONDS));
        replacement.get(5, TimeUnit.SECONDS);
        assertTrue(published.get());
        assertTrue(HelloHandler.withCurrentSession(newConnection, () -> true));
    }

    private HelloHandler handler() {
        EnvironmentSnapshotManager snapshots = new EnvironmentSnapshotManager(
                temporary.resolve("source"), temporary.resolve("repository"),
                List.of(), 10);
        return new HelloHandler(
                new GlobalConfig(), new ComputeTable(), snapshots, "world",
                new PlayerTable(), null);
    }

    private static ControlMessage hello() {
        JsonObject json = new JsonObject();
        json.addProperty("version", Protocol.VERSION);
        json.addProperty("clientId", CLIENT.toString());
        json.addProperty("playerName", "test");
        json.addProperty("mode", "client");
        return ControlMessage.of(MessageType.HELLO, json);
    }

    private static final class FakeConnection implements ControlConnection {
        private final CountDownLatch peerIdSet;
        private UUID peerId;
        private boolean open = true;

        private FakeConnection() {
            this(null);
        }

        private FakeConnection(CountDownLatch peerIdSet) {
            this.peerIdSet = peerIdSet;
        }

        @Override
        public void send(ControlMessage message) {
        }

        @Override
        public CompletableFuture<ControlMessage> request(ControlMessage message) {
            throw new UnsupportedOperationException();
        }

        @Override
        public SocketAddress remoteAddress() {
            return null;
        }

        @Override
        public UUID peerId() {
            return peerId;
        }

        @Override
        public void setPeerId(UUID peerId) {
            this.peerId = peerId;
            if (peerIdSet != null) {
                peerIdSet.countDown();
            }
        }

        @Override
        public boolean isOpen() {
            return open;
        }

        @Override
        public void close() {
            open = false;
        }
    }
}
