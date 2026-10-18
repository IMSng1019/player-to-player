package imsng.player_to_player.group;

import com.google.gson.JsonObject;
import imsng.player_to_player.netproto.ControlConnection;
import imsng.player_to_player.netproto.ControlMessage;
import imsng.player_to_player.netproto.MessageType;
import imsng.player_to_player.registry.ChunkKey;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.net.SocketAddress;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link ChunkClaimClient} 的只读申请诊断测试。
 *
 * <p>诊断是登录看门狗的证据来源：它必须在不改变 claim 状态机的前提下，记录
 * 最近一次拒绝的阻塞组、阻塞区块、尝试次数与首次/最近拒绝时间；授予后清除，
 * 避免把过期信息带进下一次加载。
 */
class ChunkClaimDiagnosticsTest {

    private static final UUID GROUP_ID =
            UUID.fromString("9207ce8d-2b04-4fd8-a9f7-a437ce1211a0");
    private static final UUID BLOCKING_GROUP =
            UUID.fromString("914b5cdf-0000-4000-8000-000000000001");
    private static final ChunkKey KEY =
            new ChunkKey("minecraft:overworld", 187, 187);

    @Test
    @DisplayName("被拒后记录阻塞组/阻塞区块/尝试次数/首次拒绝时间")
    void denialIsRecorded() {
        FakeConnection connection = new FakeConnection();
        AtomicReference<Runnable> retry = new AtomicReference<>();
        ChunkClaimClient client = new ChunkClaimClient(
                connection, GROUP_ID, 1, retry::set);

        CompletableFuture<ChunkClaimClient.ClaimOutcome> pending =
                client.claimWithRetry(KEY);

        ChunkClaimClient.ClaimDiagnostics diagnostics = client.diagnostics(KEY);
        assertNotNull(diagnostics, "被拒后必须有诊断");
        assertEquals(BLOCKING_GROUP.toString(), diagnostics.blockingGroup());
        assertEquals("minecraft:overworld;187;187", diagnostics.blockingChunk());
        assertEquals(1, diagnostics.attempts());
        assertTrue(diagnostics.firstDeniedAtMillis() > 0);
        assertEquals(diagnostics.firstDeniedAtMillis(), diagnostics.lastDeniedAtMillis());
        assertTrue(!pending.isDone(), "被拒的 claim 必须继续保持未决");
    }

    @Test
    @DisplayName("重试再次被拒：次数累加，首次拒绝时间保持不变")
    void repeatedDenialAccumulatesAttempts() {
        FakeConnection connection = new FakeConnection();
        AtomicReference<Runnable> retry = new AtomicReference<>();
        ChunkClaimClient client = new ChunkClaimClient(
                connection, GROUP_ID, 1, retry::set);
        client.claimWithRetry(KEY);

        long firstDeniedAt = client.diagnostics(KEY).firstDeniedAtMillis();
        retry.get().run();

        ChunkClaimClient.ClaimDiagnostics diagnostics = client.diagnostics(KEY);
        assertEquals(2, diagnostics.attempts());
        assertEquals(firstDeniedAt, diagnostics.firstDeniedAtMillis());
        assertTrue(diagnostics.lastDeniedAtMillis() >= firstDeniedAt);
    }

    @Test
    @DisplayName("授予后诊断立即清除，不把旧的阻塞组带到下一次加载")
    void grantClearsDiagnostics() {
        FakeConnection connection = new FakeConnection();
        AtomicReference<Runnable> retry = new AtomicReference<>();
        ChunkClaimClient client = new ChunkClaimClient(
                connection, GROUP_ID, 1, retry::set);
        CompletableFuture<ChunkClaimClient.ClaimOutcome> pending =
                client.claimWithRetry(KEY);
        assertNotNull(client.diagnostics(KEY));

        connection.grantNext = true;
        retry.get().run();

        assertNull(client.diagnostics(KEY));
        assertTrue(pending.isDone());
        assertTrue(client.isClaimed(KEY));
    }

    @Test
    @DisplayName("forgetLocal 清除诊断；shutdown 清空全部诊断")
    void localForgetAndShutdownClearDiagnostics() {
        FakeConnection connection = new FakeConnection();
        AtomicReference<Runnable> retry = new AtomicReference<>();
        ChunkClaimClient client = new ChunkClaimClient(
                connection, GROUP_ID, 1, retry::set);
        client.claimWithRetry(KEY);
        assertNotNull(client.diagnostics(KEY));

        client.forgetLocal(KEY);
        assertNull(client.diagnostics(KEY));

        // 未决 claim 仍在重试：再被拒一次即重新产生诊断
        retry.get().run();
        assertNotNull(client.diagnostics(KEY));
        client.shutdown();
        assertNull(client.diagnostics(KEY));
    }

    private static final class FakeConnection implements ControlConnection {
        private final AtomicInteger claimRequests = new AtomicInteger();
        private volatile boolean grantNext;

        @Override
        public void send(ControlMessage message) {
        }

        @Override
        public CompletableFuture<ControlMessage> request(ControlMessage message) {
            if (message.type() != MessageType.CHUNK_CLAIM_REQUEST) {
                throw new AssertionError("不应发送 " + message.type());
            }
            claimRequests.incrementAndGet();
            JsonObject json = new JsonObject();
            if (grantNext) {
                json.addProperty("granted", true);
                json.addProperty("hasServerData", false);
            } else {
                json.addProperty("granted", false);
                json.addProperty("blockingGroup", BLOCKING_GROUP.toString());
                json.addProperty("blockingChunk", "minecraft:overworld;187;187");
            }
            return CompletableFuture.completedFuture(
                    ControlMessage.of(MessageType.CHUNK_CLAIM_RESPONSE, json));
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
