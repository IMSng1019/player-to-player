package imsng.player_to_player.metadata;

import org.junit.jupiter.api.Test;
import java.util.List;
import java.util.UUID;
import static org.junit.jupiter.api.Assertions.*;

class MetadataSleepConsensusTest {
    @Test void countsEligiblePlayersAcrossGroupsAndRoundsUp() {
        var a = new MetadataSleepConsensus.Player(UUID.randomUUID(), "minecraft:overworld", true, true, true);
        var b = new MetadataSleepConsensus.Player(UUID.randomUUID(), "minecraft:overworld", true, false, true);
        var c = new MetadataSleepConsensus.Player(UUID.randomUUID(), "minecraft:overworld", true, false, true);
        assertFalse(MetadataSleepConsensus.canSkip(List.of(a, b, c), "minecraft:overworld", 50));
        assertTrue(MetadataSleepConsensus.canSkip(List.of(a, b), "minecraft:overworld", 50));
    }
    @Test void spectatorsDuplicatesAndOtherDimensionsDoNotIncreaseCount() {
        var id = UUID.randomUUID();
        var sleeping = new MetadataSleepConsensus.Player(id, "minecraft:overworld", true, true, true);
        var spectator = new MetadataSleepConsensus.Player(UUID.randomUUID(), "minecraft:overworld", false, false, true);
        var nether = new MetadataSleepConsensus.Player(UUID.randomUUID(), "minecraft:the_nether", true, false, false);
        assertTrue(MetadataSleepConsensus.canSkip(List.of(sleeping, sleeping, spectator, nether), "minecraft:overworld", 100));
        assertFalse(MetadataSleepConsensus.canSkip(List.of(), "minecraft:overworld", 0));
        assertFalse(MetadataSleepConsensus.canSkip(List.of(sleeping), "minecraft:overworld", 101));
    }
}
