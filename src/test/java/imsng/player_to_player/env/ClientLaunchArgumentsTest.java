package imsng.player_to_player.env;

import org.junit.jupiter.api.Test;
import java.nio.file.Path;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

class ClientLaunchArgumentsTest {
    @Test void manualRetryUsesCurrentAccountInsteadOfPersistedLaunchIdentity() {
        var args = List.of("--username", "old", "--accessToken=old-token", "--uuid", "old-id",
                "--userType", "mojang", "--version", "1.20.4", "--xuid", "old-xuid");
        var replaced = ClientLaunchArguments.withIdentity(args, java.util.Map.of("--username", "新玩家",
                "--accessToken", "new-token", "--uuid", "new-id", "--userType", "msa", "--xuid", ""));
        assertFalse(replaced.contains("old"));
        assertFalse(replaced.stream().anyMatch(s -> s.contains("old-token")));
        assertEquals("new-token", replaced.get(replaced.indexOf("--accessToken") + 1));
        assertEquals("新玩家", replaced.get(replaced.indexOf("--username") + 1));
        assertEquals("1.20.4", replaced.get(replaced.indexOf("--version") + 1));
    }

    @Test void isolatedLaunchCannotKeepOldFabricModDirectoryOrInjectedMods() {
        Path game = Path.of("中文 游戏目录").toAbsolutePath();
        var rewritten = ClientLaunchArguments.rewrite(List.of("-Dfabric.modsFolder=old/mods",
                "-Dfabric.addMods=extra.jar", "-Dfabric.gameJarPath.client=versions/client.jar",
                "net.fabricmc.loader.impl.launch.knot.KnotClient", "--assetsDir=资源 assets",
                "--accessToken", "test token", "--uuid", "test-uuid", "--userType", "msa"),
                game, "[2001:db8::1]:25579");
        assertTrue(rewritten.contains("-Dfabric.modsFolder=" + game.resolve("mods")));
        assertFalse(rewritten.stream().anyMatch(s -> s.startsWith("-Dfabric.addMods=")));
        assertTrue(rewritten.contains("-Dfabric.gameJarPath.client="
                + Path.of("versions/client.jar").toAbsolutePath()));
        assertTrue(rewritten.contains("--assetsDir=" + Path.of("资源 assets").toAbsolutePath()));
        assertEquals(List.of("--accessToken", "test token", "--uuid", "test-uuid", "--userType", "msa"),
                rewritten.subList(rewritten.indexOf("--accessToken"), rewritten.indexOf("--accessToken") + 6));
        assertEquals("[2001:db8::1]:25579", rewritten.get(rewritten.indexOf("--quickPlayMultiplayer") + 1));
    }

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
