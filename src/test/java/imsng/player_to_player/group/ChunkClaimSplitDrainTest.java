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

/** 分离阶段保留旧主合法写权，只有原版卸载与未知网络结果都排空才允许新组接管。 */
class ChunkClaimSplitDrainTest {
    private static final UUID GROUP = UUID.randomUUID();
    private static final ChunkKey ISLAND = new ChunkKey("minecraft:overworld", 10, 20);
    private static final ChunkKey OTHER = new ChunkKey("minecraft:overworld", 200, 300);

    @Test
    void loadedClaimsRemainOwnedUntilTheirNormalFinalReleaseAck() {
        Connection connection = new Connection();
        ChunkClaimClient client = client(connection, new ArrayList<>());
        client.claimWithRetry(ISLAND);
        connection.reply(0, granted());
        client.beginSplitDrain(ISLAND::equals);

        assertTrue(client.isClaimed(ISLAND), "分离准备不能提前忘记正在运行区块的写权限");
        assertFalse(client.isSplitDrained());
        assertTrue(client.claimWithRetry(ISLAND).isCompletedExceptionally());
        client.claimWithRetry(OTHER);
        assertEquals(2, connection.requests.size(), "另一片区域仍可正常申请");
        var released = client.releaseWhenConfirmed(ISLAND, new byte[]{1, 2});
        assertFalse(client.isSplitDrained());
        connection.reply(2, releaseAck());
        assertTrue(released.isDone());
        assertTrue(client.isSplitDrained());
        client.cancelSplitDrain();
        assertFalse(client.claimWithRetry(ISLAND).isDone(), "撤销围栏后新的holder可重新申请");
    }

    @Test
    void lateGrantIsReleasedWithoutMaterializingAndBlocksCommitUntilAck() {
        Connection connection = new Connection();
        ChunkClaimClient client = client(connection, new ArrayList<>());
        var load = client.claimWithRetry(ISLAND);
        client.beginSplitDrain(ISLAND::equals);
        assertTrue(load.isCompletedExceptionally());
        assertFalse(client.isSplitDrained(), "在途claim可能已授予，不能直接宣布排空");

        connection.reply(0, granted());

        assertEquals(MessageType.CHUNK_RELEASE, connection.requests.get(1).type());
        assertEquals(0, connection.requests.get(1).binary().length, "尚未交给原版的claim不应伪造区块数据");
        assertTrue(client.isClaimed(ISLAND));
        assertFalse(client.isSplitDrained());
        connection.reply(1, releaseAck());
        assertTrue(client.isSplitDrained());
        assertFalse(client.isClaimed(ISLAND));
    }

    @Test
    void networkTimeoutBeforeDrainRetainsTombstoneUntilAuthoritativeResult() {
        Connection connection = new Connection();
        List<Runnable> timers = new ArrayList<>();
        ChunkClaimClient client = client(connection, timers);
        var load = client.claimWithRetry(ISLAND);
        connection.responses.get(0).completeExceptionally(new IllegalStateException("timeout"));
        client.beginSplitDrain(ISLAND::equals);
        assertTrue(load.isCompletedExceptionally());
        assertFalse(client.isSplitDrained(), "超时并不表示服务端没有授予");
        timers.get(0).run();
        assertEquals(2, connection.requests.size());
        connection.reply(1, denied());
        assertTrue(client.isSplitDrained());
    }

    @Test
    void deniedRetryIsCancelledWithoutASecondClaimOrMerge() {
        Connection connection = new Connection();
        List<Runnable> timers = new ArrayList<>();
        ChunkClaimClient client = client(connection, timers);
        var load = client.claimWithRetry(ISLAND);
        connection.reply(0, denied());
        client.beginSplitDrain(ISLAND::equals);
        timers.get(0).run();
        assertTrue(load.isCompletedExceptionally());
        assertTrue(client.isSplitDrained());
        assertEquals(1, connection.requests.size());
    }

    @Test
    void cancellingFenceDoesNotReuseAnUnconfirmedLateGrant() {
        Connection connection = new Connection();
        ChunkClaimClient client = client(connection, new ArrayList<>());
        client.claimWithRetry(ISLAND);
        client.beginSplitDrain(ISLAND::equals);
        client.cancelSplitDrain();
        connection.reply(0, granted());
        assertTrue(client.claimWithRetry(ISLAND).isCompletedExceptionally(),
                "撤销分离不等于旧release已完成，不能让新holder复用将被释放的占用");
        assertEquals(2, connection.requests.size());
        connection.reply(1, releaseAck());
        assertFalse(client.claimWithRetry(ISLAND).isDone());
    }

    @Test
    void pendingFetchIsCancelledAndItsUnmaterializedClaimIsReliablyReleased() {
        Connection connection = new Connection();
        ChunkClaimClient client = client(connection, new ArrayList<>());
        client.claimWithRetry(ISLAND);
        connection.reply(0, granted());
        var fetch = client.fetchChunkData(ISLAND);
        assertTrue(client.trackedKeysSnapshot().contains(ISLAND));

        client.beginSplitDrain(ISLAND::equals);

        assertTrue(fetch.isCompletedExceptionally());
        assertEquals(MessageType.CHUNK_RELEASE, connection.requests.get(2).type());
        assertFalse(client.isSplitDrained());
        connection.reply(2, releaseAck());
        assertTrue(client.isSplitDrained());
    }

    private static ChunkClaimClient client(Connection connection, List<Runnable> timers) {
        return new ChunkClaimClient(connection, GROUP, 1, timers::add);
    }

    private static ControlMessage granted() {
        JsonObject json = new JsonObject();
        json.addProperty("granted", true);
        json.addProperty("hasServerData", true);
        return ControlMessage.of(MessageType.CHUNK_CLAIM_RESPONSE, json);
    }

    private static ControlMessage denied() {
        JsonObject json = new JsonObject();
        json.addProperty("granted", false);
        return ControlMessage.of(MessageType.CHUNK_CLAIM_RESPONSE, json);
    }

    private static ControlMessage releaseAck() {
        JsonObject json = new JsonObject();
        json.addProperty("released", true);
        json.addProperty("finalDataWritten", true);
        return ControlMessage.of(MessageType.CHUNK_RELEASE_ACK, json);
    }

    private static final class Connection implements ControlConnection {
        final List<ControlMessage> requests = new ArrayList<>();
        final List<CompletableFuture<ControlMessage>> responses = new ArrayList<>();
        @Override public CompletableFuture<ControlMessage> request(ControlMessage message) {
            requests.add(message);
            var response = new CompletableFuture<ControlMessage>();
            responses.add(response);
            return response;
        }
        void reply(int index, ControlMessage message) { responses.get(index).complete(message); }
        @Override public void send(ControlMessage message) { }
        @Override public SocketAddress remoteAddress() { return null; }
        @Override public UUID peerId() { return GROUP; }
        @Override public void setPeerId(UUID peerId) { }
        @Override public boolean isOpen() { return true; }
        @Override public void close() { }
    }
}
