package imsng.player_to_player.group;

import com.google.gson.JsonObject;
import imsng.player_to_player.netproto.ControlConnection;
import imsng.player_to_player.netproto.ControlMessage;
import imsng.player_to_player.netproto.MessageType;
import imsng.player_to_player.registry.ChunkKey;
import org.junit.jupiter.api.Test;

import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;

class ChunkClaimWakeTest {
    private static final UUID GROUP = UUID.randomUUID();
    private static final UUID BLOCKER = UUID.randomUUID();
    private static final ChunkKey KEY = new ChunkKey("minecraft:overworld", 4, 5);

    @Test
    void ownershipEventRetriesImmediatelyAndInvalidatesOldTimer() {
        FakeConnection connection = new FakeConnection();
        List<Runnable> timers = new ArrayList<>();
        ChunkClaimClient client = new ChunkClaimClient(connection, GROUP, 10, timers::add);
        var outcome = client.claimWithRetry(KEY);
        connection.responses.get(0).complete(denied(BLOCKER));
        assertEquals(1, timers.size());

        client.wakeBlockedClaims(UUID.randomUUID());
        assertEquals(1, connection.responses.size());
        client.wakeBlockedClaims(BLOCKER);
        assertEquals(2, connection.responses.size());
        assertFalse(outcome.isDone(), "通知不是授予，必须等待重新申请的权威应答");
        timers.get(0).run();
        assertEquals(2, connection.responses.size());
        connection.responses.get(1).complete(granted());
        assertTrue(outcome.join().hasServerData());
        timers.get(0).run();
        client.wakeBlockedClaims(null);
        assertEquals(2, connection.responses.size(), "旧定时器不能让已成功申请复活");
    }

    @Test
    void duplicateWakeDuringInflightProducesAtMostOneFollowup() {
        FakeConnection connection = new FakeConnection();
        List<Runnable> timers = new ArrayList<>();
        ChunkClaimClient client = new ChunkClaimClient(connection, GROUP, 10, timers::add);
        client.claimWithRetry(KEY);
        connection.responses.get(0).complete(denied(BLOCKER));
        client.wakeBlockedClaims(BLOCKER);
        client.wakeBlockedClaims(BLOCKER);
        client.wakeBlockedClaims(BLOCKER);
        assertEquals(2, connection.responses.size(), "每个key最多一个在途请求");
        connection.responses.get(1).complete(denied(BLOCKER));
        assertEquals(3, connection.responses.size(), "在途时的多个事件合并为一次后续尝试");
        connection.responses.get(2).complete(denied(BLOCKER));
        timers.get(0).run();
        assertEquals(3, connection.responses.size());
        timers.get(timers.size() - 1).run();
        assertEquals(4, connection.responses.size(), "事件唤醒后仍保留定时兜底");
    }

    @Test
    void lateGrantAndTimerCannotReviveAnExitPreparedClaim() {
        FakeConnection connection = new FakeConnection();
        List<Runnable> timers = new ArrayList<>();
        ChunkClaimClient client = new ChunkClaimClient(connection, GROUP, 10, timers::add);
        var outcome = client.claimWithRetry(KEY);
        connection.responses.get(0).complete(denied(BLOCKER));
        client.wakeBlockedClaims(BLOCKER);
        client.beginExitPreparation();
        connection.responses.get(1).complete(granted());
        timers.get(0).run();
        client.wakeBlockedClaims(null);
        assertTrue(outcome.isCompletedExceptionally());
        assertFalse(client.isClaimed(KEY));
        assertNull(client.diagnostics(KEY));
        assertEquals(2, connection.responses.size());
    }

    @Test
    void concurrentCallersStillShareOneClaimAndNetworkFailureKeepsFallback() {
        FakeConnection connection = new FakeConnection();
        List<Runnable> timers = new ArrayList<>();
        ChunkClaimClient client = new ChunkClaimClient(connection, GROUP, 10, timers::add);
        var first = client.claimWithRetry(KEY);
        assertSame(first, client.claimWithRetry(KEY));
        connection.responses.get(0).completeExceptionally(new IllegalStateException("network"));
        assertEquals(1, timers.size());
        client.wakeBlockedClaims(BLOCKER);
        assertEquals(1, connection.responses.size(), "通信错误不能冒充该组拒绝");
        timers.get(0).run();
        assertEquals(2, connection.responses.size());
        connection.responses.get(1).complete(granted());
        assertTrue(first.isDone());
    }

    @Test
    void staleTimerSchedulingFailureCannotCancelAnAlreadyWokenAttempt() {
        FakeConnection connection = new FakeConnection();
        java.util.concurrent.atomic.AtomicReference<ChunkClaimClient> holder =
                new java.util.concurrent.atomic.AtomicReference<>();
        ChunkClaimClient client = new ChunkClaimClient(connection, GROUP, 10, task -> {
            // 在安排旧重试和报告调度失败之间插入权威通知，模拟真实线程交错。
            holder.get().wakeBlockedClaims(BLOCKER);
            throw new IllegalStateException("scheduler stopped");
        });
        holder.set(client);
        var result = client.claimWithRetry(KEY);
        connection.responses.get(0).complete(denied(BLOCKER));
        assertEquals(2, connection.responses.size());
        connection.responses.get(1).complete(granted());
        assertTrue(result.join().hasServerData());
        assertTrue(client.isClaimed(KEY));
    }

    private static ControlMessage denied(UUID group) {
        JsonObject json = new JsonObject();
        json.addProperty("granted", false);
        json.addProperty("blockingGroup", group.toString());
        json.addProperty("blockingChunk", KEY.asString());
        return ControlMessage.of(MessageType.CHUNK_CLAIM_RESPONSE, json);
    }

    private static ControlMessage granted() {
        JsonObject json = new JsonObject();
        json.addProperty("granted", true);
        json.addProperty("hasServerData", true);
        return ControlMessage.of(MessageType.CHUNK_CLAIM_RESPONSE, json);
    }

    private static final class FakeConnection implements ControlConnection {
        private final List<CompletableFuture<ControlMessage>> responses = new ArrayList<>();
        @Override public CompletableFuture<ControlMessage> request(ControlMessage message) {
            assertEquals(MessageType.CHUNK_CLAIM_REQUEST, message.type());
            CompletableFuture<ControlMessage> response = new CompletableFuture<>();
            responses.add(response);
            return response;
        }
        @Override public void send(ControlMessage message) { }
        @Override public SocketAddress remoteAddress() { return null; }
        @Override public UUID peerId() { return GROUP; }
        @Override public void setPeerId(UUID peerId) { }
        @Override public boolean isOpen() { return true; }
        @Override public void close() { }
    }
}
