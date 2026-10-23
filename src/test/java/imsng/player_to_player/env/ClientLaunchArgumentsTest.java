package imsng.player_to_player.env;

import org.junit.jupiter.api.Test;
import java.nio.file.Path;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

class ClientLaunchArgumentsTest {
    @Test void preservesAuthenticationButReplacesDirectoryAndQuickplay() {
        var args = List.of("-Xmx2G", "-cp", "base.jar", "net.fabricmc.loader.impl.launch.knot.KnotClient",
                "--username", "Alice", "--accessToken", "test-only", "--gameDir", "old",
                "--quickPlayMultiplayer", "old:25565", "--server", "old", "--port", "25565");
        var rewritten = ClientLaunchArguments.rewrite(args, Path.of("new-instance"), "new:25570");
        assertTrue(rewritten.contains("test-only"));
        assertEquals("new:25570", rewritten.get(rewritten.indexOf("--quickPlayMultiplayer") + 1));
        assertFalse(rewritten.contains("--server"));
        assertFalse(rewritten.contains("--port"));
        assertEquals(Path.of("new-instance").toAbsolutePath().toString(),
                rewritten.get(rewritten.indexOf("--gameDir") + 1));
    }

    @Test void relativeClasspathAndAssetsAreResolvedAgainstOriginalWorkingDirectory() {
        var args = List.of("-cp", "lib/base.jar", "net.fabricmc.loader.impl.launch.knot.KnotClient",
                "--assetsDir", "assets", "--gameDir=old");
        var rewritten = ClientLaunchArguments.rewrite(args, Path.of("isolated"), "host:25565");
        assertFalse(rewritten.contains("--gameDir=old"));
        assertEquals(Path.of("assets").toAbsolutePath().toString(),
                rewritten.get(rewritten.indexOf("--assetsDir") + 1));
        assertTrue(Path.of(rewritten.get(rewritten.indexOf("-cp") + 1)).isAbsolute());
    }

    @Test void authenticationAgentKeepsOptionsAndUsesAbsoluteJarInNewWorkingDirectory() {
        var rewritten = ClientLaunchArguments.rewrite(List.of("-javaagent:auth/agent.jar=https://login.example.test",
                "net.fabricmc.loader.impl.launch.knot.KnotClient"), Path.of("isolated"), "host:25565");
        assertTrue(rewritten.contains("-javaagent:" + Path.of("auth/agent.jar").toAbsolutePath()
                + "=https://login.example.test"));
    }
}
