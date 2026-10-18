package imsng.player_to_player.group;

import imsng.player_to_player.registry.ChunkKey;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link LoginLoadGuard} 的登录窗口、诊断与硬超时测试（不依赖 Minecraft 运行时）。
 */
class LoginLoadGuardTest {

    private static final UUID PLAYER_ID =
            UUID.fromString("f0e1d2c3-b4a5-4678-9012-3456789abcde");
    private static final ChunkKey KEY =
            new ChunkKey("minecraft:overworld", 187, 187);

    private ScheduledExecutorService scheduler;

    @BeforeEach
    void setUp() {
        scheduler = Executors.newSingleThreadScheduledExecutor();
        LoginLoadGuard.resetForTest();
        // 把 5s/60s 压缩成 5ms/30ms，测试无需真等一分钟
        LoginLoadGuard.configureForTest(scheduler, 5L, 30L);
    }

    @AfterEach
    void tearDown() {
        LoginLoadGuard.resetForTest();
        scheduler.shutdownNow();
    }

    @Test
    @DisplayName("登录期区块加载超时 -> 异常完成，绝不返回本地生成")
    void pendingLoginLoadFailsAfterHardTimeout() {
        LoginLoadGuard.beginLogin(PLAYER_ID, "tester",
                "minecraft:overworld", 187, 187);
        CompletableFuture<String> future = new CompletableFuture<>();

        LoginLoadGuard.guard(KEY, future, () -> "阻塞组=test-group", true);

        ExecutionException failure = assertThrows(ExecutionException.class,
                () -> future.get(2, TimeUnit.SECONDS));
        assertNotNull(failure.getCause());
        assertTrue(failure.getCause().getMessage().contains(KEY.asString()),
                "超时异常必须带上区块键，便于定位: " + failure.getCause().getMessage());
        assertTrue(failure.getCause().getMessage().contains("登录期区块加载超时"),
                "异常语义必须是登录期加载超时: " + failure.getCause().getMessage());
    }

    @Test
    @DisplayName("已完成的区块加载不会被看门狗误杀")
    void completedFutureSurvivesWatchdog() throws Exception {
        LoginLoadGuard.beginLogin(PLAYER_ID, "tester",
                "minecraft:overworld", 187, 187);
        CompletableFuture<String> future = new CompletableFuture<>();
        LoginLoadGuard.guard(KEY, future, () -> "无阻塞", true);
        future.complete("loaded");

        TimeUnit.MILLISECONDS.sleep(120); // 越过 warn/timeout 两个时间点

        assertEquals("loaded", future.get());
        assertFalse(future.isCompletedExceptionally());
    }

    @Test
    @DisplayName("非主线程/异步加载只告警不硬超时")
    void asyncLoadOnlyWarns() throws Exception {
        LoginLoadGuard.beginLogin(PLAYER_ID, "tester",
                "minecraft:overworld", 187, 187);
        CompletableFuture<String> future = new CompletableFuture<>();
        LoginLoadGuard.guard(KEY, future, () -> "无阻塞", false);

        TimeUnit.MILLISECONDS.sleep(120);

        assertFalse(future.isDone(), "不武装硬超时时 future 必须保持原状");
        future.complete("later");
    }

    @Test
    @DisplayName("登录窗口之外的加载不武装看门狗")
    void guardIsInactiveOutsideLoginWindow() throws Exception {
        CompletableFuture<String> future = new CompletableFuture<>();
        LoginLoadGuard.guard(KEY, future, () -> "无", true);

        TimeUnit.MILLISECONDS.sleep(120);

        assertFalse(future.isDone());
        future.complete("later");
    }

    @Test
    @DisplayName("首批 claim 只保留前 20 个，登录结束时清空")
    void recentClaimsAreBoundedAndCleared() {
        assertFalse(LoginLoadGuard.isLoginPhase());
        LoginLoadGuard.beginLogin(PLAYER_ID, "tester",
                "minecraft:overworld", 187, 187);
        assertTrue(LoginLoadGuard.isLoginPhase());

        for (int i = 0; i < 25; i++) {
            LoginLoadGuard.recordClaim(new ChunkKey("minecraft:overworld", i, 0));
        }
        List<ChunkKey> claims = LoginLoadGuard.recentClaimsSnapshot();
        assertEquals(20, claims.size());
        assertEquals(new ChunkKey("minecraft:overworld", 0, 0), claims.get(0));
        assertEquals(new ChunkKey("minecraft:overworld", 19, 0), claims.get(19));

        LoginLoadGuard.endLogin(PLAYER_ID, "JOIN");
        assertFalse(LoginLoadGuard.isLoginPhase());
        assertTrue(LoginLoadGuard.recentClaimsSnapshot().isEmpty());
    }

    @Test
    @DisplayName("看门狗只认当前登录玩家：其他玩家的结束事件不清窗口")
    void endLoginIgnoresOtherPlayers() {
        LoginLoadGuard.beginLogin(PLAYER_ID, "tester",
                "minecraft:overworld", 187, 187);
        LoginLoadGuard.endLogin(UUID.randomUUID(), "DISCONNECT");
        assertTrue(LoginLoadGuard.isLoginPhase());
    }

    @Test
    @DisplayName("诊断供给器抛错时超时仍然生效")
    void brokenDiagnosticsSupplierDoesNotPreventTimeout() {
        LoginLoadGuard.beginLogin(PLAYER_ID, "tester",
                "minecraft:overworld", 187, 187);
        CompletableFuture<String> future = new CompletableFuture<>();
        LoginLoadGuard.guard(KEY, future, () -> {
            throw new IllegalStateException("diagnostics boom");
        }, true);

        assertThrows(ExecutionException.class,
                () -> future.get(2, TimeUnit.SECONDS));
    }

    @Test
    @DisplayName("超时 future 不会被二次完成")
    void timeoutCompletesOnlyOnce() throws Exception {
        LoginLoadGuard.beginLogin(PLAYER_ID, "tester",
                "minecraft:overworld", 187, 187);
        CompletableFuture<String> future = new CompletableFuture<>();
        LoginLoadGuard.guard(KEY, future, () -> "无", true);

        assertThrows(ExecutionException.class,
                () -> future.get(2, TimeUnit.SECONDS));
        // 外部随后尝试完成：被 CompletableFuture 拒绝，状态仍是异常
        assertFalse(future.complete("late"));
        assertTrue(future.isCompletedExceptionally());
    }

    @Test
    @DisplayName("超时异常必须是 RuntimeException 且不能是 IOException")
    void timeoutCauseMustNotBeIOException() {
        LoginLoadGuard.beginLogin(PLAYER_ID, "tester",
                "minecraft:overworld", 187, 187);
        CompletableFuture<String> future = new CompletableFuture<>();
        LoginLoadGuard.guard(KEY, future, () -> "无", true);
        ExecutionException failure = assertThrows(ExecutionException.class,
                () -> future.get(2, TimeUnit.SECONDS));
        assertTrue(failure.getCause() instanceof LoginLoadGuard.LoginLoadTimeoutException,
                "超时必须使用专用的 Runtime 异常: " + failure.getCause());
        assertTrue(failure.getCause() instanceof RuntimeException,
                "非受检异常才能沿 CompletableFuture 链路传递");
        assertFalse(failure.getCause() instanceof java.io.IOException,
                "IOException 会被原版 scheduleChunkLoad 当成读盘失败并生成空 ProtoChunk，"
                        + "随后按种子生成本地数据；超时必须使用非 IOException");
    }

    @Test
    @DisplayName("JOIN 结束窗口后，已武装的硬超时不得再杀 future")
    void endLoginDisarmsPendingHardTimeout() throws Exception {
        LoginLoadGuard.beginLogin(PLAYER_ID, "tester",
                "minecraft:overworld", 187, 187);
        CompletableFuture<String> future = new CompletableFuture<>();
        LoginLoadGuard.guard(KEY, future, () -> "无", true);
        LoginLoadGuard.endLogin(PLAYER_ID, "JOIN");

        TimeUnit.MILLISECONDS.sleep(120);

        assertFalse(future.isDone(),
                "窗口结束后主线程已不可能阻塞在这次登录上，定时器必须失效");
        future.complete("later");
    }

    @Test
    @DisplayName("新登录覆盖上下文后，旧窗口的定时器不得误杀新窗口的 future")
    void newLoginContextDisarmsStaleTimeout() throws Exception {
        LoginLoadGuard.beginLogin(PLAYER_ID, "tester",
                "minecraft:overworld", 187, 187);
        CompletableFuture<String> staleFuture = new CompletableFuture<>();
        LoginLoadGuard.guard(KEY, staleFuture, () -> "无", true);

        UUID otherPlayer = UUID.randomUUID();
        LoginLoadGuard.beginLogin(otherPlayer, "other",
                "minecraft:overworld", 10, 10);

        TimeUnit.MILLISECONDS.sleep(120);

        assertFalse(staleFuture.isDone(), "旧上下文的定时器必须因身份不匹配而失效");
        staleFuture.complete("later");
    }
}
