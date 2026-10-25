package imsng.player_to_player.metadata;

import net.minecraft.nbt.CompoundTag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/** 元数据规则不启动游戏，直接覆盖玩家隔离、启动约束及版本重放的重要边界。 */
class MetadataFieldPolicyTest {
    @Test
    void startupSnapshotKeepsIndependentPlayerAndExistingSimulationClock() {
        CompoundTag old = new CompoundTag();
        CompoundTag player = new CompoundTag(); player.putString("Inventory", "private");
        old.put("Player", player); old.putLong("Time", 8000); old.putLong("DayTime", 1);
        CompoundTag snapshot = new CompoundTag(); snapshot.putLong("Time", 10); snapshot.putLong("DayTime", 9000);
        snapshot.put("Player", new CompoundTag()); snapshot.putString("UnknownModField", "retained");
        CompoundTag merged = MetadataFieldPolicy.startupData(old, snapshot);
        assertEquals(8000, merged.getLong("Time"));
        assertEquals(player, merged.getCompound("Player"));
        assertEquals(9000, merged.getLong("DayTime"));
        assertEquals("retained", merged.getString("UnknownModField"));
        assertEquals(1, old.getLong("DayTime"));
    }

    @Test
    void runtimeChangesDistinguishRestartAndUnknownFields() {
        assertEquals(MetadataAdapter.Ownership.STARTUP, MetadataFieldPolicy.ownership("WorldGenSettings"));
        assertEquals(MetadataAdapter.Ownership.COORDINATOR, MetadataFieldPolicy.ownership("GameRules"));
        assertEquals(MetadataAdapter.Ownership.SIMULATION_GROUP, MetadataFieldPolicy.ownership("DragonFight"));
        assertEquals(MetadataAdapter.Ownership.PRESERVED, MetadataFieldPolicy.ownership("a_mod:field"));
        assertEquals(MetadataAdapter.Ownership.PLAYER, MetadataFieldPolicy.ownership("Player"));
        assertEquals(MetadataAdapter.Ownership.LOCAL_CLOCK, MetadataFieldPolicy.ownership("Time"));
    }

    @Test
    void generationOrDatapackChangeNeedsRestartButGlobalRuntimeChangesDoNot() {
        CompoundTag before = new CompoundTag();
        CompoundTag generation = new CompoundTag(); generation.putLong("seed", 17);
        before.put("WorldGenSettings", generation); before.putByte("Difficulty", (byte) 1);
        CompoundTag after = before.copy(); after.putByte("Difficulty", (byte) 3);
        assertTrue(MetadataFieldPolicy.restartFields(before, after).isEmpty());
        after.getCompound("WorldGenSettings").putLong("seed", 18);
        assertEquals(java.util.Set.of("WorldGenSettings"), MetadataFieldPolicy.restartFields(before, after));
        after.remove("WorldGenSettings");
        assertEquals(java.util.Set.of("WorldGenSettings"), MetadataFieldPolicy.restartFields(before, after));
    }

    @Test
    void boundaryReconstructionDeductsElapsedTimeAndDoesNotOvershoot() {
        assertEquals(3000, MetadataFieldPolicy.remainingLerp(5000, 1000, 3000));
        assertEquals(0, MetadataFieldPolicy.remainingLerp(5000, 1000, 9999));
        assertEquals(5000, MetadataFieldPolicy.remainingLerp(5000, 3000, 1000));
    }

    @Test
    void sleepingPercentageUsesAllEligiblePlayersAndRequiresOneDeepSleeper() {
        assertFalse(MetadataFieldPolicy.enoughSleeping(3, 1, 50));
        assertTrue(MetadataFieldPolicy.enoughSleeping(3, 2, 50));
        assertFalse(MetadataFieldPolicy.enoughSleeping(0, 0, 0));
        assertFalse(MetadataFieldPolicy.enoughSleeping(2, 0, 0));
        assertTrue(MetadataFieldPolicy.enoughSleeping(2, 1, 0));
        assertFalse(MetadataFieldPolicy.enoughSleeping(2, 2, 101));
    }
}
