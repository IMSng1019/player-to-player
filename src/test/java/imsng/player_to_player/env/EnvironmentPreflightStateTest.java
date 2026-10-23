package imsng.player_to_player.env;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class EnvironmentPreflightStateTest {
    @Test void ipv6AddressKeepsUnambiguousNonDefaultPort() {
        assertEquals("[2001:db8::1]:25570", EnvironmentPreflightState.addressKey("2001:DB8::1", 25570));
        assertEquals("example.test:25570", EnvironmentPreflightState.addressKey("Example.Test", 25570));
    }
    @Test void completionCannotResumeCanceledOrSupersededServer() {
        var state = new EnvironmentPreflightState();
        long first = state.begin("host:25565");
        long second = state.begin("host:25566");
        assertFalse(state.ready(first, "r1"));
        assertTrue(state.ready(second, "r2"));
        assertFalse(state.matches("host:25565", "r2"));
        assertTrue(state.matches("host:25566", "r2"));
        state.cancel();
        assertFalse(state.matches("host:25566", "r2"));
        assertFalse(state.ready(second, "r2"));
    }

    @Test void runtimeChangeRequiresNewPreparation() {
        var state = new EnvironmentPreflightState();
        long epoch = state.begin("host:25565");
        assertTrue(state.ready(epoch, "runtimeA"));
        assertTrue(state.matches("host:25565", "runtimeA"));
        assertFalse(state.matches("host:25565", "runtimeB"));
    }
}
