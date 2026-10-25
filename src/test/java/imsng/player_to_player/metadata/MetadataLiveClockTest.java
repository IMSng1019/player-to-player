package imsng.player_to_player.metadata;

import net.minecraft.nbt.CompoundTag;
import org.junit.jupiter.api.Test;
import java.util.Set;
import static org.junit.jupiter.api.Assertions.*;

class MetadataLiveClockTest {
    @Test void discreteSimulationCommitDoesNotRollBackNaturalClockSinceCheckpoint() {
        CompoundTag live = new CompoundTag();
        live.putLong("DayTime", 1059); live.putInt("rainTime", 941);
        live.putString("mod:removed", "old");
        CompoundTag old = new CompoundTag(); old.putInt("deleted", 1); live.put("mod:simulation", old);
        CompoundTag delta = new CompoundTag(); CompoundTag state = new CompoundTag(); state.putInt("current", 9);
        delta.put("mod:simulation", state);
        MetadataPatch patch = new MetadataPatch("world", 10, 11, 2, "change", delta, Set.of("mod:removed"));
        CompoundTag applied = MetadataService.overlayCommittedFields(live, patch);
        assertEquals(1059, applied.getLong("DayTime"));
        assertEquals(941, applied.getInt("rainTime"));
        assertEquals(9, applied.getCompound("mod:simulation").getInt("current"));
        assertFalse(applied.getCompound("mod:simulation").contains("deleted"));
        assertFalse(applied.contains("mod:removed"));
        assertTrue(live.contains("mod:removed"));
    }
}
