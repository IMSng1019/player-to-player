package imsng.player_to_player.runtime;

import imsng.player_to_player.metadata.MetadataSnapshot;
import net.minecraft.nbt.CompoundTag;
import org.junit.jupiter.api.Test;
import java.util.HashMap;
import java.util.Map;
import static org.junit.jupiter.api.Assertions.*;

class WorkerMetadataPropertiesTest {
    @Test void latestMetadataWinsOverEnvironmentStartupDefaults() {
        var properties = new HashMap<>(Map.of("difficulty", "easy", "gamemode", "survival", "view-distance", "5"));
        var data = new CompoundTag(); data.putByte("Difficulty", (byte) 3); data.putInt("GameType", 2); data.putBoolean("hardcore", true);
        WorkerProcess.applyMetadataProperties(properties, new MetadataSnapshot("world", 9, 3, data));
        assertEquals("hard", properties.get("difficulty")); assertEquals("adventure", properties.get("gamemode"));
        assertEquals("true", properties.get("hardcore")); assertEquals("5", properties.get("view-distance"));
    }
}
