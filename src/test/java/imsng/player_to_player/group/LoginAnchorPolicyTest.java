package imsng.player_to_player.group;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 登录锚点策略的纯函数测试（不构造 Minecraft 对象）。
 *
 * <p>规范依据：
 * <ul>
 *   <li>AGENTS.md L76 / prompt L44："装载了该模组服务器不加载出生点常加载区块"；</li>
 *   <li>AGENTS.md L87："玩家加入世界……确认玩家所需要加载的未知区块是否被加载"——
 *       登录锚点是玩家真实位置，不是世界共享出生点；</li>
 *   <li>AGENTS.md L96：申请/合并未完成时区块不可进入、视野空白，但服务端主线程
 *       不能被无限期挂住。</li>
 * </ul>
 */
class LoginAnchorPolicyTest {

    @Test
    @DisplayName("受管组世界 + disableSpawnChunks=true：取消原版出生点 fudge")
    void managedLevelSkipsSpawnFudgeWhenDisabled() {
        assertTrue(LoginAnchorPolicy.shouldSkipSpawnFudge(true, true));
    }

    @Test
    @DisplayName("受管组世界 + disableSpawnChunks=false：仍跳过（防止重新引入登录挂起）")
    void managedLevelStillSkipsSpawnFudgeWhenConfigDisabled() {
        assertTrue(LoginAnchorPolicy.shouldSkipSpawnFudge(true, false),
                "受管世界的出生点加载会与 claim 门控冲突，配置逃生口不能覆盖这条路径");
    }

    @Test
    @DisplayName("非受管世界（单人/专用服/未接管集成服）绝不干预原版")
    void unmanagedLevelNeverSkipsSpawnFudge() {
        assertFalse(LoginAnchorPolicy.shouldSkipSpawnFudge(false, true));
        assertFalse(LoginAnchorPolicy.shouldSkipSpawnFudge(false, false));
    }

    @Test
    @DisplayName("只有登录窗口 + 服务端主线程的同步加载才武装硬超时")
    void syncWatchdogOnlyOnServerThreadDuringLogin() {
        assertTrue(LoginAnchorPolicy.shouldArmSyncWatchdog(true, true));
        assertFalse(LoginAnchorPolicy.shouldArmSyncWatchdog(false, true));
        assertFalse(LoginAnchorPolicy.shouldArmSyncWatchdog(true, false));
        assertFalse(LoginAnchorPolicy.shouldArmSyncWatchdog(false, false));
    }

    @Test
    @DisplayName("共享出生点区块判定按方块坐标 >> 4")
    void sharedSpawnChunkUsesBlockToChunkShift() {
        assertTrue(LoginAnchorPolicy.isSharedSpawnChunk(0, 0, 0, 0));
        assertTrue(LoginAnchorPolicy.isSharedSpawnChunk(15, -1, 0, -1));
        assertFalse(LoginAnchorPolicy.isSharedSpawnChunk(16, 0, 0, 0));
        assertFalse(LoginAnchorPolicy.isSharedSpawnChunk(0, 16, 0, 0));
        assertTrue(LoginAnchorPolicy.isSharedSpawnChunk(-17, -17, -2, -2));
    }

    @Test
    @DisplayName("视野外判定：矩形边界内算视野内，跨维度一律算视野外")
    void outsideRenderAreaUsesRectAndDimension() {
        // 玩家区块 (187,187)，视距 10：Δ=10 恰好在边界内，Δ=11 才在视野外
        assertFalse(LoginAnchorPolicy.isOutsideRenderArea(
                "minecraft:overworld", "minecraft:overworld", 197, 187, 187, 187, 10));
        assertTrue(LoginAnchorPolicy.isOutsideRenderArea(
                "minecraft:overworld", "minecraft:overworld", 198, 187, 187, 187, 10));
        assertFalse(LoginAnchorPolicy.isOutsideRenderArea(
                "minecraft:overworld", "minecraft:overworld", 177, 187, 187, 187, 10));
        assertTrue(LoginAnchorPolicy.isOutsideRenderArea(
                "minecraft:overworld", "minecraft:overworld", 176, 187, 187, 187, 10));
        assertFalse(LoginAnchorPolicy.isOutsideRenderArea(
                "minecraft:overworld", "minecraft:overworld", 177, 177, 187, 187, 10));
        assertTrue(LoginAnchorPolicy.isOutsideRenderArea(
                "minecraft:the_nether", "minecraft:overworld", 187, 187, 187, 187, 10));
        assertTrue(LoginAnchorPolicy.isOutsideRenderArea(
                null, "minecraft:overworld", 187, 187, 187, 187, 10));
    }
}
