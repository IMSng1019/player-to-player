package imsng.player_to_player.group;

import imsng.player_to_player.registry.ChunkKey;
import imsng.player_to_player.registry.SplitRegion;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class SplitRegionPlanTest {
    private static final String DIM = "minecraft:overworld";

    @Test
    void departureIncludesActualDependencyRingBeyondRenderRectangle() {
        Set<ChunkKey> tracked = rectangle(-20, 20, -20, 20);
        tracked.addAll(rectangle(100, 140, 100, 140));
        List<SplitRegion> result = SplitRegionPlan.plan(tracked,
                List.of(region(-12, 12, -12, 12)),
                List.of(region(108, 132, 108, 132)), List.of()).orElseThrow();
        assertTrue(result.stream().anyMatch(r -> r.contains(new ChunkKey(DIM, 20, 20))),
                "旧实现只交接625格渲染方形，会把外围依赖留给旧组");
        assertFalse(result.stream().anyMatch(r -> r.contains(new ChunkKey(DIM, 120, 120))));
    }

    @Test
    void connectedDependencyChainIntoRemainingPlayerPreventsSplit() {
        Set<ChunkKey> tracked = rectangle(0, 50, 0, 0);
        assertTrue(SplitRegionPlan.plan(tracked,
                List.of(region(0, 2, 0, 0)),
                List.of(region(48, 50, 0, 0)), List.of()).isEmpty());
    }

    @Test
    void diagonalGenerationDependenciesRemainInSameIsland() {
        Set<ChunkKey> tracked = Set.of(new ChunkKey(DIM, 0, 0),
                new ChunkKey(DIM, 1, 1), new ChunkKey(DIM, 2, 2));
        List<SplitRegion> result = SplitRegionPlan.plan(tracked,
                List.of(region(0, 0, 0, 0)), List.of(), List.of()).orElseThrow();
        assertTrue(result.stream().anyMatch(r -> r.contains(new ChunkKey(DIM, 2, 2))));
    }

    @Test
    void specialTicketInfluencePreventsPartialHandoff() {
        assertTrue(SplitRegionPlan.plan(rectangle(0, 20, 0, 20),
                List.of(region(0, 4, 0, 4)), List.of(),
                List.of(region(19, 30, 19, 30))).isEmpty());
    }

    @Test
    void differentDimensionsAreIndependentAndMissingClaimsCannotBeTransferred() {
        SplitRegion nether = new SplitRegion("minecraft:the_nether", 0, 5, 0, 5);
        assertTrue(SplitRegionPlan.plan(rectangle(0, 5, 0, 5),
                List.of(region(0, 5, 0, 5)), List.of(nether), List.of()).isPresent());
        assertTrue(SplitRegionPlan.plan(Set.of(),
                List.of(region(0, 5, 0, 5)), List.of(), List.of()).isEmpty());
    }

    @Test
    void generationBridgeCanBeReservedWholeWithoutPretendingItIsAlreadyTwoIslands() {
        Set<ChunkKey> tracked = rectangle(-23, 55, -24, 57);
        List<SplitRegion> demand = List.of(region(-24, 24, -25, 23), region(8, 56, 10, 58));
        assertTrue(SplitRegionPlan.plan(tracked, List.of(demand.get(1)),
                List.of(demand.get(0)), List.of()).isEmpty());
        List<SplitRegion> rebuilt = SplitRegionPlan.planReload(tracked, demand, List.of()).orElseThrow();
        assertEquals(List.of(region(-24, 56, -25, 58)), rebuilt,
                "必须把留组区域和生成桥一起预约，等全部旧玩家/holder退出后才提交");
        assertTrue(tracked.stream().allMatch(key -> rebuilt.stream().anyMatch(r -> r.contains(key))));
    }

    @Test
    void fullReloadCannotDropIndependentTicketsOrAnUnboundedReservation() {
        Set<ChunkKey> tracked = Set.of(new ChunkKey(DIM, 0, 0), new ChunkKey(DIM, 20, 20));
        List<SplitRegion> demand = List.of(region(0, 0, 0, 0), region(20, 20, 20, 20));
        assertTrue(SplitRegionPlan.planReload(tracked, demand,
                List.of(new SplitRegion("minecraft:the_end", 0, 0, 0, 0))).isEmpty(),
                "玩家断开不能撤销独立的传送门/强加载票据，即使位于其他维度也不能假定全组会排空");
        assertTrue(SplitRegionPlan.planReload(tracked,
                List.of(region(0, 0, 0, 0), region(2000, 2000, 0, 0)), List.of()).isEmpty());
        assertTrue(SplitRegionPlan.planReload(Set.of(), demand, List.of()).isEmpty());
    }

    @Test
    void fullReloadReservesEachDimensionSeparately() {
        Set<ChunkKey> tracked = Set.of(new ChunkKey(DIM, 0, 0), new ChunkKey("minecraft:the_nether", 8, 8));
        List<SplitRegion> result = SplitRegionPlan.planReload(tracked,
                List.of(region(-1, 1, -1, 1)), List.of()).orElseThrow();
        assertEquals(2, result.size());
        assertTrue(tracked.stream().allMatch(key -> result.stream().anyMatch(r -> r.contains(key))));
    }

    @Test
    void reloadDoesNotResolveAnOverlapOfTheNecessaryLiveLoadingEdges() {
        SplitRegion source = region(-14, 14, -15, 13);
        assertFalse(SplitRegionPlan.reloadDemandSeparated(List.of(region(10, 38, -14, 14)),
                List.of(source)), "必需边缘仍相连时不应反复断开玩家再合并");
        assertTrue(SplitRegionPlan.reloadDemandSeparated(List.of(region(18, 46, 20, 48)),
                List.of(source)), "现场坐标的必需边缘可分开，只有历史12格世代依赖仍把两方连接");
    }

    private static SplitRegion region(int minX, int maxX, int minZ, int maxZ) {
        return new SplitRegion(DIM, minX, maxX, minZ, maxZ);
    }

    private static Set<ChunkKey> rectangle(int minX, int maxX, int minZ, int maxZ) {
        Set<ChunkKey> result = new HashSet<>();
        for (int x = minX; x <= maxX; x++) {
            for (int z = minZ; z <= maxZ; z++) result.add(new ChunkKey(DIM, x, z));
        }
        return result;
    }
}
