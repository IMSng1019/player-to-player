package imsng.player_to_player.server;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.Files;
import java.nio.file.Path;
import static org.junit.jupiter.api.Assertions.*;

class EnvironmentGameSettingsTest {
    @TempDir Path root;
    @Test void exportsGameplayPropertiesWithoutAdministrativeSecrets() throws Exception {
        Files.writeString(root.resolve("server.properties"), "enable-command-block=true\nmax-players=40\n"
                + "op-permission-level=3\ngenerate-structures=false\nlevel-type=minecraft:flat\n"
                + "rcon.password=private\nserver-port=25566\nonline-mode=true\n");
        var exported = EnvironmentPreflightServer.readGameSettings(root);
        assertEquals("true", exported.get("enable-command-block"));
        assertEquals("40", exported.get("max-players"));
        assertEquals("3", exported.get("op-permission-level"));
        assertEquals("false", exported.get("generate-structures"));
        assertFalse(exported.containsKey("rcon.password"));
        assertFalse(exported.containsKey("server-port"));
        assertFalse(exported.containsKey("online-mode"));
    }
}
