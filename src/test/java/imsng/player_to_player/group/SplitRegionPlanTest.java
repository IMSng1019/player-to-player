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
