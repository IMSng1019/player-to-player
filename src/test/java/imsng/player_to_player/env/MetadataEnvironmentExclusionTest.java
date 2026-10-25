package imsng.player_to_player.env;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

class MetadataEnvironmentExclusionTest {
    @TempDir Path directory;

    @Test void levelSaveDoesNotChangeEnvironmentVersion() throws Exception {
        Path world = directory.resolve("saves/actual-world");
        Files.createDirectories(world);
        Files.writeString(world.resolve("level.dat"), "before");
        Files.writeString(world.resolve("level.dat_old"), "previous");
        Files.writeString(world.resolve("settings.json"), "stable");
        List<String> excluded = List.of("saves/actual-world/level.dat", "saves/actual-world/level.dat_old");
        var before = EnvironmentScanner.scan(directory, excluded);
        Files.writeString(world.resolve("level.dat"), "new-daytime-weather-save");
        Files.writeString(world.resolve("level.dat_old"), "before");
        var after = EnvironmentScanner.scan(directory, excluded);
        assertEquals(before.globalHash(), after.globalHash());
        assertEquals(1, after.files().size());
    }
}
