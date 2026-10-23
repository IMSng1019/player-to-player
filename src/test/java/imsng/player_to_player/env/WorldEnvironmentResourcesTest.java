package imsng.player_to_player.env;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.Files;
import java.nio.file.Path;
import static org.junit.jupiter.api.Assertions.*;

class WorldEnvironmentResourcesTest {
    @TempDir Path temp;
    @Test
    void refreshesManagedResourcesWithoutResettingWorldState() throws Exception {
        Path env = temp.resolve("environment");
        Path save = temp.resolve("save");
        write(env.resolve("datapacks/new.zip"), "new-pack");
        write(env.resolve("serverconfig/game.json"), "new-config");
        write(env.resolve("level.dat"), "old-remote-state");
        write(save.resolve("level.dat"), "live-state");
        write(save.resolve("datapacks/removed.zip"), "old-pack");
        write(save.resolve("data/mod.dat"), "live-mod-state");
        Class<?> sync = assertDoesNotThrow(() -> Class.forName("imsng.player_to_player.env.WorldEnvironmentResources"));
        sync.getMethod("sync", Path.class, Path.class).invoke(null, env, save);
        assertEquals("live-state", Files.readString(save.resolve("level.dat")));
        assertEquals("live-mod-state", Files.readString(save.resolve("data/mod.dat")));
        assertEquals("new-pack", Files.readString(save.resolve("datapacks/new.zip")));
        assertEquals("new-config", Files.readString(save.resolve("serverconfig/game.json")));
        assertFalse(Files.exists(save.resolve("datapacks/removed.zip")));
    }
    private static void write(Path path, String value) throws Exception {
        Files.createDirectories(path.getParent()); Files.writeString(path, value);
    }
}
