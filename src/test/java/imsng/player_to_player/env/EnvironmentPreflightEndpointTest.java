package imsng.player_to_player.env;

import org.junit.jupiter.api.Test;
import java.util.Map;
import static org.junit.jupiter.api.Assertions.*;

class EnvironmentPreflightEndpointTest {
    private EnvironmentPreflightInfo info(String world, String version) {
        return new EnvironmentPreflightInfo(25580, 0, world, "world", "a".repeat(64), version.repeat(64), Map.of());
    }
    @Test void fallbackCannotDownloadFromAnotherWorldOrEnvironmentOnSameHost() {
        var login = info("one", "a");
        assertTrue(login.sameRuntime(info("one", "a")));
        assertFalse(login.sameRuntime(info("two", "a")));
        assertFalse(login.sameRuntime(info("one", "b")));
        var anotherInstance = new EnvironmentPreflightInfo(25580, 0, "one", "world",
                "a".repeat(64), "a".repeat(64), Map.of(), "11111111-1111-1111-1111-111111111111");
        assertFalse(login.sameRuntime(anotherInstance));
    }
}
