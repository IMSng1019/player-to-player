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

/** 合并提交后的旧组不再持有写权限，未决加载不能继续等待或被迟到应答复活。 */
class ChunkClaimRetirementTest {
    private static final ChunkKey KEY = new ChunkKey("minecraft:overworld", 3, 4);
    private static final UUID GROUP = UUID.randomUUID();

    @Test
    void transferFinishesPendingLoadAndDisablesTimersLateGrantsAndNewClaims() {
        Connection connection = new Connection();
        List<Runnable> timers = new ArrayList<>();
        ChunkClaimClient claims = new ChunkClaimClient(connection, GROUP, 1, timers::add);
        var loading = claims.claimWithRetry(KEY);
        connection.replies.get(0).complete(reply(false));
        timers.get(0).run();
        assertEquals(2, connection.replies.size());

        claims.forgetAllLocal();

        assertTrue(loading.isCompletedExceptionally(), "转交全部所有权时必须结束旧ChunkMap正在等待的加载");
        assertTrue(ChunkClaimClient.isTransferredFailure(
                assertThrows(java.util.concurrent.CompletionException.class, loading::join)),
                "迁移加载必须携带明确标记，供Mixin返回UNLOADED而非生成空ProtoChunk");
        connection.replies.get(1).complete(reply(true));
        timers.get(0).run();
        claims.wakeBlockedClaims(null);
        assertFalse(claims.isClaimed(KEY), "迟到grant不得恢复旧组写权限");
        assertTrue(claims.claimWithRetry(KEY).isCompletedExceptionally());
        assertEquals(2, connection.replies.size(), "旧定时器、通知和新请求均不得继续发claim");
    }

    @Test
    void transferAlsoFinishesInflightServerDataFetch() {
        Connection connection = new Connection();
        ChunkClaimClient claims = new ChunkClaimClient(connection, GROUP, 1, task -> { });
        var claimed = claims.claimWithRetry(KEY);
        connection.replies.get(0).complete(reply(true));
        assertTrue(claimed.join().hasServerData());
        var loading = claims.fetchChunkData(KEY);
        assertFalse(loading.isDone());

        claims.forgetAllLocal();

        assertTrue(loading.isCompletedExceptionally(), "已获grant但仍在拉取NBT的加载也不能阻塞停服");
        assertFalse(claims.isClaimed(KEY));
        assertTrue(claims.fetchChunkData(KEY).isCompletedExceptionally());
        assertEquals(2, connection.replies.size());
    }

    @Test
    void unrelatedDiskAndNetworkFailuresMustNotBeClassifiedAsTransfer() {
        assertFalse(ChunkClaimClient.isTransferredFailure(new java.io.IOException("disk")));
        assertFalse(ChunkClaimClient.isTransferredFailure(
                new java.util.concurrent.CompletionException(new IllegalStateException("network"))));
    }

    private static ControlMessage reply(boolean granted) {
        JsonObject json = new JsonObject();
        json.addProperty("granted", granted);
        json.addProperty("hasServerData", true);
        return ControlMessage.of(MessageType.CHUNK_CLAIM_RESPONSE, json);
    }

    private static final class Connection implements ControlConnection {
        final List<CompletableFuture<ControlMessage>> replies = new ArrayList<>();
        @Override public CompletableFuture<ControlMessage> request(ControlMessage message) {
            var reply = new CompletableFuture<ControlMessage>();
            replies.add(reply);
            return reply;
        }
        @Override public void send(ControlMessage message) { }
        @Override public SocketAddress remoteAddress() { return null; }
        @Override public UUID peerId() { return GROUP; }
        @Override public void setPeerId(UUID peerId) { }
        @Override public boolean isOpen() { return true; }
        @Override public void close() { }
    }
}
