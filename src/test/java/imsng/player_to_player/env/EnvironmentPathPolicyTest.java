package imsng.player_to_player.env;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class EnvironmentPathPolicyTest {

    @Test
    void includesStableEnvironmentAndWorldBootstrapFiles() {
        EnvironmentPathPolicy policy = EnvironmentPathPolicy.create(List.of());

        assertTrue(policy.includes("mods/example.jar"));
        assertTrue(policy.includes("world/level.dat"));
    }

    @Test
    void excludesBuiltInRuntimeAndPrivatePaths() {
        EnvironmentPathPolicy policy = EnvironmentPathPolicy.create(List.of());

        assertFalse(policy.includes("logs/latest.log"));
        assertFalse(policy.includes("player-to-player/config.json"));
        assertFalse(policy.includes("world/region/r.0.0.mca"));
        assertFalse(policy.includes("world/level.dat.p2pdl.tmp"));
    }

    @Test
    void configuredExclusionsAreNormalizedAndCaseInsensitive() {
        EnvironmentPathPolicy policy = EnvironmentPathPolicy.create(
                List.of("Custom\\Secrets", "./Cache/"));

        assertFalse(policy.includes("custom/secrets/token.txt"));
        assertFalse(policy.includes("CACHE/data.bin"));
        assertTrue(policy.includes("custom/public/readme.txt"));
    }
}
