package imsng.player_to_player.group;

import com.google.gson.JsonObject;
import imsng.player_to_player.netproto.ControlConnection;
import imsng.player_to_player.netproto.ControlMessage;
import imsng.player_to_player.netproto.MessageType;
import imsng.player_to_player.registry.ChunkKey;
import net.minecraft.nbt.CompoundTag;
import org.junit.jupiter.api.Test;

import java.net.SocketAddress;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ChunkUploadServiceTest {

    private static final UUID GROUP_ID =
            UUID.fromString("9207ce8d-2b04-4fd8-a9f7-a437ce1211a0");
    private static final ChunkKey KEY =
            new ChunkKey("minecraft:overworld", -7, 0);

    @Test
    void successfulAcknowledgementClearsCurrentSnapshot() {
        FakeConnection connection = new FakeConnection();
        connection.responses.add(success());
        ChunkUploadService service = new ChunkUploadService(connection, GROUP_ID,
                Runnable::run, ignored -> {
        }, key -> true);

        service.enqueue(KEY, chunkAt(1));

        assertNull(service.latestSnapshot(KEY));
        assertEquals(1, connection.requests.get());
    }

    @Test
    void successfulAcknowledgementNotifiesServerDataKnownExactlyOnce() {
        FakeConnection connection = new FakeConnection();
        connection.responses.add(success());
        AtomicInteger notifications = new AtomicInteger();
        ChunkUploadService service = new ChunkUploadService(connection, GROUP_ID,
                Runnable::run, ignored -> {
                }, key -> true, ignored -> notifications.incrementAndGet());

        service.enqueue(KEY, chunkAt(1));

        assertEquals(1, notifications.get());
    }

    @Test
    void failedOrNegativeAcknowledgementDoesNotNotifyServerDataKnown() {
        AtomicInteger notifications = new AtomicInteger();
        FakeConnection exceptionalConnection = new FakeConnection();
        exceptionalConnection.responses.add(CompletableFuture.failedFuture(
                new java.io.IOException("offline")));
        ChunkUploadService exceptional = new ChunkUploadService(exceptionalConnection, GROUP_ID,
                Runnable::run, ignored -> {
                }, key -> true, ignored -> notifications.incrementAndGet());
        exceptional.enqueue(KEY, chunkAt(1));

        FakeConnection negativeConnection = new FakeConnection();
        negativeConnection.responses.add(negative());
        ChunkUploadService negative = new ChunkUploadService(negativeConnection, GROUP_ID,
                Runnable::run, ignored -> {
                }, key -> true, ignored -> notifications.incrementAndGet());
        negative.enqueue(KEY, chunkAt(2));

        assertEquals(0, notifications.get());
    }

    @Test
    void olderAcknowledgementDoesNotClearNewerSnapshot() throws Exception {
        FakeConnection connection = new FakeConnection();
        CompletableFuture<ControlMessage> oldAcknowledgement = new CompletableFuture<>();
        connection.responses.add(oldAcknowledgement);
        // 第二版返回负 ACK，让工作线程在确认旧版后停止，测试可以精确观察新版状态。
        connection.responses.add(negative());
        ExecutorService executor = Executors.newSingleThreadExecutor();
        AtomicInteger notifications = new AtomicInteger();
        ChunkUploadService service = new ChunkUploadService(connection, GROUP_ID,
                executor, ignored -> {
        }, key -> true, ignored -> notifications.incrementAndGet());
        try {
            service.enqueue(KEY, chunkAt(1));
            assertTrue(connection.awaitRequests(1), "旧版本应已进入在途请求");

            service.enqueue(KEY, chunkAt(9));
            oldAcknowledgement.complete(success().join());
            assertTrue(connection.awaitRequests(2), "旧 ACK 后应继续尝试发送新版本");

            assertEquals(9, service.latestSnapshot(KEY).getInt("value"));
            assertEquals(1, notifications.get(), "旧版本正 ACK 仍应建立服务端数据基线");
        } finally {
            service.shutdown();
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    @Test
    void exceptionalAcknowledgementKeepsLatestSnapshot() {
        FakeConnection connection = new FakeConnection();
        connection.responses.add(CompletableFuture.failedFuture(
                new java.io.IOException("offline")));
        ChunkUploadService service = new ChunkUploadService(connection, GROUP_ID,
                Runnable::run, ignored -> {
        }, key -> true);

        service.enqueue(KEY, chunkAt(1));

        assertEquals(1, service.latestSnapshot(KEY).getInt("value"));
    }

    @Test
    void negativeAcknowledgementKeepsLatestSnapshot() {
        FakeConnection connection = new FakeConnection();
        connection.responses.add(negative());
        ChunkUploadService service = new ChunkUploadService(connection, GROUP_ID,
                Runnable::run, ignored -> {
        }, key -> true);

        service.enqueue(KEY, chunkAt(1));

        assertEquals(1, service.latestSnapshot(KEY).getInt("value"));
    }

    @Test
    void staleUnclaimedCleanupDoesNotDeleteSnapshotFromAReclaimedChunk() throws Exception {
        FakeConnection connection = new FakeConnection();
        connection.responses.add(negative());
        CompletableFuture<Void> predicateEntered = new CompletableFuture<>();
        CompletableFuture<Void> continuePredicate = new CompletableFuture<>();
        AtomicInteger predicateCalls = new AtomicInteger();
        ExecutorService executor = Executors.newSingleThreadExecutor();
        ChunkUploadService service = new ChunkUploadService(connection, GROUP_ID,
                executor, ignored -> {
        }, key -> {
            if (predicateCalls.incrementAndGet() == 1) {
                // 第一版 A 正在检查旧 claim 时暂停，制造“释放后同坐标重新 claim”的窗口。
                predicateEntered.complete(null);
                continuePredicate.join();
                return false;
            }
            return true;
        });
        try {
            service.enqueue(KEY, chunkAt(1));
            predicateEntered.get(5, TimeUnit.SECONDS);

            // A 的旧 claim 已失效，但同坐标已经属于新 claim，并产生了独立快照 B。
            service.enqueue(KEY, chunkAt(8));
            continuePredicate.complete(null);
            executor.shutdown();
            assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));

            CompoundTag replacement = service.latestUnacknowledged(KEY);
            assertNotNull(replacement, "旧 A 的清理不能删除重新 claim 后入队的 B");
            assertEquals(8, replacement.getInt("value"));
            assertEquals(1, connection.requests.get());
        } finally {
            continuePredicate.complete(null);
            service.shutdown();
            executor.shutdownNow();
        }
    }

    @Test
    void unacknowledgedQueriesAndForgetUseTheSameAtomicState() {
        FakeConnection connection = new FakeConnection();
        connection.responses.add(negative());
        ChunkUploadService service = new ChunkUploadService(connection, GROUP_ID,
                Runnable::run, ignored -> {
        }, key -> true);

        service.enqueue(KEY, chunkAt(3));
        assertTrue(service.hasUnacknowledged(KEY));
        assertEquals(3, service.latestUnacknowledged(KEY).getInt("value"));

        service.forget(KEY);
        assertTrue(!service.hasUnacknowledged(KEY));
        assertNull(service.latestUnacknowledged(KEY));
    }

    @Test
    void forgetPreventsAnAlreadyScheduledRetryFromRevivingTheChunk() {
        FakeConnection connection = new FakeConnection();
        connection.responses.add(CompletableFuture.failedFuture(
                new java.io.IOException("offline")));
        AtomicReference<Runnable> scheduledRetry = new AtomicReference<>();
        ChunkUploadService service = new ChunkUploadService(connection, GROUP_ID,
                Runnable::run, scheduledRetry::set, key -> true);

        service.enqueue(KEY, chunkAt(4));
        service.forget(KEY);
        scheduledRetry.get().run();

        assertEquals(1, connection.requests.get());
        assertTrue(!service.hasUnacknowledged(KEY));
    }

    @Test
    void exitFreezePreventsAnAlreadyScheduledRetryFromSending() {
        FakeConnection connection = new FakeConnection();
        connection.responses.add(CompletableFuture.failedFuture(
                new java.io.IOException("offline")));
        AtomicReference<Runnable> scheduledRetry = new AtomicReference<>();
        ChunkUploadService service = new ChunkUploadService(connection, GROUP_ID,
                Runnable::run, scheduledRetry::set, key -> true);

        service.enqueue(KEY, chunkAt(10));
        service.freezeForExit();
        scheduledRetry.get().run();

        assertEquals(1, connection.requests.get());
        assertEquals(10, service.latestUnacknowledged(KEY).getInt("value"));
    }

    @Test
    void exitFreezeRecordsNewSnapshotsWithoutStartingNormalUploads() {
        FakeConnection connection = new FakeConnection();
        ChunkUploadService service = new ChunkUploadService(connection, GROUP_ID,
                Runnable::run, ignored -> {
        }, key -> true);

        service.freezeForExit();
        service.enqueue(KEY, chunkAt(5));

        assertEquals(0, connection.requests.get());
        assertTrue(service.hasUnacknowledged(KEY));
        assertEquals(5, service.latestUnacknowledged(KEY).getInt("value"));
    }

    @Test
    void inFlightAcknowledgementCannotSendReplacementAfterExitFreeze() throws Exception {
        FakeConnection connection = new FakeConnection();
        CompletableFuture<ControlMessage> oldAcknowledgement = new CompletableFuture<>();
        connection.responses.add(oldAcknowledgement);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        ChunkUploadService service = new ChunkUploadService(connection, GROUP_ID,
                executor, ignored -> {
        }, key -> true);
        try {
            service.enqueue(KEY, chunkAt(6));
            assertTrue(connection.awaitRequests(1), "旧版本应已进入在途请求");
            service.freezeForExit();
            service.enqueue(KEY, chunkAt(7));

            oldAcknowledgement.complete(success().join());
            executor.shutdown();
            assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));

            assertEquals(1, connection.requests.get());
            assertEquals(7, service.latestUnacknowledged(KEY).getInt("value"));
        } finally {
            service.shutdown();
            executor.shutdownNow();
        }
    }

    private static CompletableFuture<ControlMessage> success() {
        JsonObject json = new JsonObject();
        json.addProperty("written", true);
        return CompletableFuture.completedFuture(
                ControlMessage.of(MessageType.CHUNK_DATA_UPLOAD, json));
    }

    private static CompletableFuture<ControlMessage> negative() {
        JsonObject json = new JsonObject();
        json.addProperty("written", false);
        return CompletableFuture.completedFuture(
                ControlMessage.of(MessageType.CHUNK_DATA_UPLOAD, json));
    }

    private static CompoundTag chunkAt(int value) {
        CompoundTag tag = new CompoundTag();
        tag.putInt("xPos", KEY.x());
        tag.putInt("zPos", KEY.z());
        tag.putInt("value", value);
        return tag;
    }

    private static final class FakeConnection implements ControlConnection {
        private final Queue<CompletableFuture<ControlMessage>> responses =
                new ArrayDeque<>();
        private final AtomicInteger requests = new AtomicInteger();

        @Override
        public void send(ControlMessage message) {
        }

        @Override
        public CompletableFuture<ControlMessage> request(ControlMessage message) {
            synchronized (this) {
                requests.incrementAndGet();
                notifyAll();
            }
            return responses.remove();
        }

        private synchronized boolean awaitRequests(int expected) throws InterruptedException {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (requests.get() < expected) {
                long remaining = deadline - System.nanoTime();
                if (remaining <= 0) {
                    return false;
                }
                TimeUnit.NANOSECONDS.timedWait(this, remaining);
            }
            return true;
        }

        @Override
        public SocketAddress remoteAddress() {
            return null;
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
    }
}
