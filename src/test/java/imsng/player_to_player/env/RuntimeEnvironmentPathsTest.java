package imsng.player_to_player.env;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.Path;
import static org.junit.jupiter.api.Assertions.*;

class RuntimeEnvironmentPathsTest {
    @TempDir Path temp;
    @Test void worldConfigurationResolvesToActualSaveRatherThanAbsentRuntimeWorld() throws Exception {
        Path game = temp.resolve("runtime");
        Path save = temp.resolve("data/display-name");
        Class<?> type = assertDoesNotThrow(() -> Class.forName("imsng.player_to_player.env.RuntimeEnvironmentPaths"));
        Path config = (Path) type.getMethod("resolve", Path.class, Path.class, String.class, String.class)
                .invoke(null, game, save, "actual-world", "actual-world/serverconfig/mod.json");
        assertEquals(save.resolve("serverconfig/mod.json").toAbsolutePath(), config);
        Path common = (Path) type.getMethod("resolve", Path.class, Path.class, String.class, String.class)
                .invoke(null, game, save, "actual-world", "config/mod.json");
        assertEquals(game.resolve("config/mod.json").toAbsolutePath(), common);
    }
}
