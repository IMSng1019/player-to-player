package imsng.player_to_player.group;

import com.google.gson.JsonObject;
import imsng.player_to_player.netproto.ControlConnection;
import imsng.player_to_player.netproto.ControlMessage;
import imsng.player_to_player.netproto.MessageType;
import net.minecraft.nbt.CompoundTag;
import net.minecraft.nbt.DoubleTag;
import net.minecraft.nbt.ListTag;
import org.junit.jupiter.api.Test;

import java.net.SocketAddress;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PlayerStateUploadServiceTest {

    private static final UUID PLAYER_ID =
            UUID.fromString("9207ce8d-2b04-4fd8-a9f7-a437ce1211a0");

    @Test
    void confirmedDuplicateIsSkippedAndChangedStateIsUploaded() {
        FakeConnection connection = new FakeConnection();
        connection.responses.add(success());
        connection.responses.add(success());
        PlayerStateUploadService service = new PlayerStateUploadService(
                connection, Runnable::run, ignored -> {
        });

        service.offer(PLAYER_ID, playerAt(1.0));
        service.offer(PLAYER_ID, playerAt(1.0));
        service.offer(PLAYER_ID, playerAt(2.0));

        assertEquals(2, connection.requests);
    }

    @Test
    void failedStateRemainsReplaceableByNewerState() throws Exception {
        FakeConnection connection = new FakeConnection();
        connection.responses.add(CompletableFuture.failedFuture(
                new java.io.IOException("offline")));
        connection.responses.add(success());
        PlayerStateUploadService service = new PlayerStateUploadService(
                connection, Runnable::run, ignored -> {
        });

        service.offer(PLAYER_ID, playerAt(1.0));
        service.offer(PLAYER_ID, playerAt(9.0));

        CompoundTag uploaded = PlayerStateNbt.decodeValidated(
                connection.lastRequest.binary());
        assertEquals(9.0, uploaded.getList("Pos", 6).getDouble(0), 0.0);
        assertEquals(2, connection.requests);
    }

    @Test
    void finalSnapshotWaitsForWorkerAndDurableAcknowledgement() {
        FakeConnection connection = new FakeConnection();
        connection.responses.add(success());
        Queue<Runnable> tasks = new ArrayDeque<>();
        PlayerStateUploadService service = new PlayerStateUploadService(connection, tasks::add, ignored -> { });
        CompletableFuture<Void> durable = service.offerConfirmed(PLAYER_ID, playerAt(9.0));
        assertFalse(durable.isDone(), "分离不能把已入队当作已经写盘确认");
        tasks.remove().run();
        assertTrue(durable.isDone());
        assertFalse(durable.isCompletedExceptionally());
    }

    @Test
    void shutdownFailsUnconfirmedFinalSnapshotAndPreventsQueuedSend() {
        FakeConnection connection = new FakeConnection();
        Queue<Runnable> tasks = new ArrayDeque<>();
        PlayerStateUploadService service = new PlayerStateUploadService(connection, tasks::add, ignored -> { });
        CompletableFuture<Void> durable = service.offerConfirmed(PLAYER_ID, playerAt(9.0));
        service.shutdown();
        tasks.remove().run();
        assertTrue(durable.isCompletedExceptionally());
        assertEquals(0, connection.requests);
    }

    @Test
    void uploadSequenceSurvivesHostInstanceReplacementAndRetry() {
        FakeConnection connection = new FakeConnection();
        connection.responses.add(CompletableFuture.failedFuture(new java.io.IOException("timeout")));
        connection.responses.add(success());
        connection.responses.add(success());
        Queue<Runnable> retry = new ArrayDeque<>();
        PlayerStateUploadService oldHost = new PlayerStateUploadService(connection, Runnable::run, retry::add);
        oldHost.offer(PLAYER_ID, playerAt(1));
        long first = imsng.player_to_player.util.JsonUtil.getLong(connection.lastRequest.json(), "uploadSequence", 0);
        assertTrue(first > 0, "每个真实上行都必须携带有效序号");
        retry.remove().run();
        assertEquals(first, imsng.player_to_player.util.JsonUtil.getLong(connection.lastRequest.json(), "uploadSequence", 0));
        oldHost.shutdown();
        PlayerStateUploadService newHost = new PlayerStateUploadService(connection, Runnable::run, ignored -> { });
        newHost.offer(PLAYER_ID, playerAt(2));
        long next = imsng.player_to_player.util.JsonUtil.getLong(connection.lastRequest.json(), "uploadSequence", 0);
        assertTrue(next > first, "同控制连接主副切换后不能从1重新编号");
    }

    @Test
    void finalReturnToConfirmedValueStillSendsBarrierAfterIntermediateTimeout() {
        FakeConnection connection = new FakeConnection();
        connection.responses.add(success());
        connection.responses.add(CompletableFuture.failedFuture(new java.util.concurrent.TimeoutException("旧B仍可能在服务端队列")));
        connection.responses.add(success());
        Queue<Runnable> tasks = new ArrayDeque<>();
        PlayerStateUploadService service = new PlayerStateUploadService(connection, tasks::add, ignored -> { });
        service.offer(PLAYER_ID, playerAt(1));
        tasks.remove().run();
        service.offer(PLAYER_ID, playerAt(2));
        tasks.remove().run();
        long uncertainSequence = imsng.player_to_player.util.JsonUtil.getLong(connection.lastRequest.json(), "uploadSequence", 0);

        CompletableFuture<Void> finalAck = service.offerConfirmed(PLAYER_ID, playerAt(1));
        assertFalse(finalAck.isDone());
        tasks.remove().run();

        assertEquals(3, connection.requests, "最终A虽然等于旧confirmed，但必须用新序号挡住超时B");
        assertTrue(imsng.player_to_player.util.JsonUtil.getLong(connection.lastRequest.json(), "uploadSequence", 0) > uncertainSequence);
        assertTrue(finalAck.isDone());
        assertFalse(finalAck.isCompletedExceptionally());
    }

    @Test
    void ordinaryReturnToConfirmedValueAlsoResendsAfterIntermediateTimeout() {
        FakeConnection connection = new FakeConnection();
        connection.responses.add(success());
        connection.responses.add(CompletableFuture.failedFuture(new java.util.concurrent.TimeoutException("未知写入结果")));
        connection.responses.add(success());
        PlayerStateUploadService service = new PlayerStateUploadService(connection, Runnable::run, ignored -> { });
        service.offer(PLAYER_ID, playerAt(1));
        service.offer(PLAYER_ID, playerAt(2));
        service.offer(PLAYER_ID, playerAt(1));

        assertEquals(3, connection.requests, "实时回到A也不能以旧confirmed去重");
        service.offer(PLAYER_ID, playerAt(1));
        assertEquals(3, connection.requests, "最新A收到ACK后可恢复正常去重");
    }

    private static CompletableFuture<ControlMessage> success() {
        JsonObject json = new JsonObject();
        json.addProperty("written", true);
        return CompletableFuture.completedFuture(
                ControlMessage.of(MessageType.PLAYER_DATA, json));
    }

    private static CompoundTag playerAt(double x) {
        CompoundTag tag = new CompoundTag();
        tag.putString("Dimension", "minecraft:overworld");
        ListTag pos = new ListTag();
        pos.add(DoubleTag.valueOf(x));
        pos.add(DoubleTag.valueOf(80.0));
        pos.add(DoubleTag.valueOf(2.0));
        tag.put("Pos", pos);
        return tag;
    }

    private static final class FakeConnection implements ControlConnection {
        private final Queue<CompletableFuture<ControlMessage>> responses =
                new ArrayDeque<>();
        private int requests;
        private ControlMessage lastRequest;

        @Override
        public void send(ControlMessage message) {
        }

        @Override
        public CompletableFuture<ControlMessage> request(ControlMessage message) {
            requests++;
            lastRequest = message;
            return responses.remove();
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

