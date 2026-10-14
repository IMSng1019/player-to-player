package imsng.player_to_player.group;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link SplitMonitor#blocksIntersect} 的几何判据测试。
 *
 * <p>规范原文（player_to_player-prompt.txt）：
 * <ul>
 *   <li>"周围四个区块：指东西南北四个直接相邻区块"——即四邻<b>十字形</b>扩张；</li>
 *   <li>"渲染区块的周围 4 个区块的周围 4 个区块无交集并且时长达到 10s
 *       则将其分为两个组客户端"——十字形扩张两次 = <b>曼哈顿距离 ≤ 2</b>。</li>
 * </ul>
 *
 * <p>因此"相交"当且仅当两轴区间距 {@code gapX ≤ 2 且 gapZ ≤ 2}。历史上这里曾
 * 把判据写成"切比雪夫方块"（对角方向也被算作相交），使该分离的组不分离、
 * 被迫继续合并；本测试把正确几何钉死，防止回退。</p>
 *
 * <p>测试环境无法构造 {@code ServerPlayer}，故判据被抽成纯静态方法。</p>
 */
class SplitMonitorGeometryTest {

    /** 规范视距（区块）：10 → 渲染矩形半边长 10，边长 21。 */
    private static final int VD = 10;

    @Test
    @DisplayName("渲染区域重叠：任何轴上都无间隙，视为相交")
    void overlappingRegionsIntersect() {
        assertTrue(SplitMonitor.blocksIntersect(0, 0, 0, 0, VD), "同点必然相交");
        // 位移 2*VD = 20 时两矩形仍恰好共享边（间隙 0）
        assertTrue(SplitMonitor.blocksIntersect(0, 0, 2 * VD, 0, VD), "X 轴刚好贴边算相交");
        assertTrue(SplitMonitor.blocksIntersect(0, 0, 0, 2 * VD, VD), "Z 轴刚好贴边算相交");
        assertTrue(SplitMonitor.blocksIntersect(0, 0, 2 * VD, 2 * VD, VD), "两轴都贴边算相交");
    }

    @Test
    @DisplayName("轴向间隙 ≤ 2：仍落在周围4格×2的十字缓冲带内，视为相交")
    void axisGapWithinBufferIntersects() {
        for (int gap = 0; gap <= 2; gap++) {
            int d = 2 * VD + gap;
            assertTrue(SplitMonitor.blocksIntersect(0, 0, d, 0, VD),
                    "X 轴间隙 " + gap + " 应相交");
            assertTrue(SplitMonitor.blocksIntersect(0, 0, 0, d, VD),
                    "Z 轴间隙 " + gap + " 应相交");
            // 另一轴有间隙但未超缓冲带：仍相交（十字带是二维的）
            assertTrue(SplitMonitor.blocksIntersect(0, 0, d, d, VD),
                    "两轴间隙均为 " + gap + " 应相交");
        }
    }

    @Test
    @DisplayName("任一轴间隙 > 2：十字缓冲带已断开，视为无交集（可分离）")
    void gapBeyondBufferSeparates() {
        int d = 2 * VD + 3;
        assertFalse(SplitMonitor.blocksIntersect(0, 0, d, 0, VD), "X 轴间隙 3 应分离");
        assertFalse(SplitMonitor.blocksIntersect(0, 0, 0, d, VD), "Z 轴间隙 3 应分离");
        assertFalse(SplitMonitor.blocksIntersect(0, 0, d, d, VD), "两轴间隙均 3 应分离");
        // 远距离同理
        assertFalse(SplitMonitor.blocksIntersect(0, 0, 10_000, 10_000, VD));
    }

    @Test
    @DisplayName("对角方向不得按切比雪夫方块判定：轴距刚好 3/3 时必须分离")
    void diagonalMustNotUseChebyshevSquare() {
        // 曾经的错误实现：max(gapX, gapZ) 写成 sum(gapX, gapZ) 之外，
        // 更早的版本把判据当作"5×5 方块扩张"，此时对角 3/3 会被误判为相交。
        // 曼哈顿语义下两轴间隙 3 已使十字带断开（3 > 2），必须分离。
        assertFalse(SplitMonitor.blocksIntersect(0, 0, 2 * VD + 3, 2 * VD + 3, VD),
                "对角 3/3：十字带断开，必须分离");
        assertTrue(SplitMonitor.blocksIntersect(0, 0, 2 * VD + 2, 2 * VD + 2, VD),
                "对角 2/2：十字带仍相连");
    }

    @Test
    @DisplayName("对称性：判据与参数顺序无关")
    void predicateIsSymmetric() {
        int[][] cases = {{0, 0, 20, 0}, {0, 0, 22, 0}, {0, 0, 23, 0},
                {5, -7, 28, 15}, {-100, 40, -77, 42}, {0, 0, 21, 21}};
        for (int[] c : cases) {
            boolean forward = SplitMonitor.blocksIntersect(c[0], c[1], c[2], c[3], VD);
            boolean backward = SplitMonitor.blocksIntersect(c[2], c[3], c[0], c[1], VD);
            org.junit.jupiter.api.Assertions.assertEquals(forward, backward,
                    "(" + c[0] + "," + c[1] + ") vs (" + c[2] + "," + c[3] + ") 应对称");
        }
    }

    @Test
    @DisplayName("视距为 0（极端配置）时退化为单区块十字带")
    void zeroViewDistanceDegradesGracefully() {
        assertTrue(SplitMonitor.blocksIntersect(0, 0, 2, 0, 0), "间隙 2 相交");
        assertFalse(SplitMonitor.blocksIntersect(0, 0, 3, 0, 0), "间隙 3 分离");
    }
}
