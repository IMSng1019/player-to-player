package imsng.player_to_player.env;

import imsng.player_to_player.util.Sha256;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import static org.junit.jupiter.api.Assertions.*;

class ClientRuntimeStartupTest {
    @TempDir Path directory;

    @Test void changedConfigurationBeforeStartupCannotPassWithOnlyVersionFlag() throws Exception {
        Path config = directory.resolve("config/game.json");
        Files.createDirectories(config.getParent());
        Files.writeString(config, "required");
        var required = new EnvironmentManifest(Map.of("config/game.json",
                new EnvironmentManifest.Entry(Sha256.hexOfFile(config), Files.size(config))));
        Files.writeString(config, "edited");
        var startup = ClientRuntimeStartup.capture(directory, required);
        assertFalse(startup.matches(required, "world"));
    }

    @Test void snapshotRepresentsStartupBytesInsteadOfLaterDiskChanges() throws Exception {
        Path config = directory.resolve("game.json");
        Files.writeString(config, "loaded");
        var required = new EnvironmentManifest(Map.of("game.json",
                new EnvironmentManifest.Entry(Sha256.hexOfFile(config), Files.size(config))));
        var startup = ClientRuntimeStartup.capture(directory, required);
        Files.writeString(config, "changed-after-loading");
        assertTrue(startup.matches(required, "world"));
        var changed = new EnvironmentManifest(Map.of("game.json",
                new EnvironmentManifest.Entry(Sha256.hexOfFile(config), Files.size(config))));
        assertFalse(startup.matches(changed, "world"));
    }
}
