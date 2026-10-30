package imsng.player_to_player.env;

import org.junit.jupiter.api.Test;
import java.util.Map;
import static org.junit.jupiter.api.Assertions.*;

class EnvironmentRuntimeIdentityTest {
    private static final EnvironmentManifest.Entry A = new EnvironmentManifest.Entry("a".repeat(64), 1);
    private static final EnvironmentManifest.Entry B = new EnvironmentManifest.Entry("b".repeat(64), 2);

    @Test void personalOptionsDoNotChangeTheServerRuntimeVersion() {
        var baseline = new EnvironmentManifest(Map.of("config/game.json", A));
        var withOptions = new EnvironmentManifest(Map.of("config/game.json", A,
                "options.txt", B, "OPTIONSOF.TXT", A, "optionsshaders.txt", B));
        assertEquals(EnvironmentRuntimeIdentity.hash(baseline, "world"),
                EnvironmentRuntimeIdentity.hash(withOptions, "world"));
        assertTrue(EnvironmentRuntimeIdentity.isRuntimePath("config/options.txt", "world"));
    }

    @Test void worldSavesDoNotInvalidateLoginButDatapacksDo() {
        var before = new EnvironmentManifest(Map.of("saves/actual/level.dat", A,
                "saves/actual/playerdata/a.dat", A, "saves/actual/datapacks/rules.zip", A,
                "config/game.json", A, "mods/game.jar", A));
        var saved = new EnvironmentManifest(Map.of("saves/actual/level.dat", B,
                "saves/actual/playerdata/a.dat", B, "saves/actual/datapacks/rules.zip", A,
                "config/game.json", A, "mods/game.jar", A));
        assertEquals(EnvironmentRuntimeIdentity.hash(before, "saves/actual"),
                EnvironmentRuntimeIdentity.hash(saved, "saves/actual"));
        var changed = new EnvironmentManifest(Map.of("saves/actual/datapacks/rules.zip", B,
                "config/game.json", A, "mods/game.jar", A));
        assertNotEquals(EnvironmentRuntimeIdentity.hash(before, "saves/actual"),
                EnvironmentRuntimeIdentity.hash(changed, "saves/actual"));
    }

    @Test void customModWorldDataIsMutableButServerconfigIsRuntime() {
        assertFalse(EnvironmentRuntimeIdentity.isRuntimePath("world/custommod/state.bin", "world"));
        assertTrue(EnvironmentRuntimeIdentity.isRuntimePath("world/serverconfig/a.toml", "world"));
        assertTrue(EnvironmentRuntimeIdentity.isRuntimePath("world/datapacks/a.zip", "world"));
        assertTrue(EnvironmentRuntimeIdentity.isRuntimePath("config/state.json", "world"));
    }

    @Test void customWorldMetadataCannotBypassUploadFilterByChangingPathCase() {
        assertFalse(EnvironmentRuntimeIdentity.isRuntimePath("saves/CUSTOM/level.dat", "saves/custom"));
        assertFalse(EnvironmentRuntimeIdentity.isRuntimePath("SAVES/custom/LEVEL.DAT_OLD", "saves/custom"));
        assertFalse(EnvironmentRuntimeIdentity.isRuntimePath("saves/CUSTOM/session.lock", "saves/custom"));
        assertTrue(EnvironmentRuntimeIdentity.isRuntimePath("saves/custom/serverconfig/mod.toml", "saves/custom"));
    }
}
