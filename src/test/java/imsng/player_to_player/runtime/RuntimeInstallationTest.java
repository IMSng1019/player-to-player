package imsng.player_to_player.runtime;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Properties;
import static org.junit.jupiter.api.Assertions.*;

class RuntimeInstallationTest {
    @TempDir Path temp;

    @Test
    void installsEnvironmentWithoutCopyingMutableWorldOrManagementSettings() throws Exception {
        Path env = temp.resolve("environment");
        write(env.resolve("mods/example.jar"), "mod");
        write(env.resolve("config/example.json"), "configuration");
        write(env.resolve("actual-save/level.dat"), "mutable-world");
        write(env.resolve("server.properties"), "rcon.password=secret");
        Path runtime = temp.resolve("runtime");
        RuntimeInstallation.install(env, runtime, "actual-save", Map.of("difficulty", "hard",
                "server-ip", "0.0.0.0", "enable-rcon", "true", "rcon.password", "secret"));
        assertEquals("configuration", Files.readString(runtime.resolve("config/example.json")));
        assertFalse(Files.exists(runtime.resolve("actual-save")));
        Properties settings = new Properties();
        try (var reader = Files.newBufferedReader(runtime.resolve("server.properties"))) { settings.load(reader); }
        assertEquals("hard", settings.getProperty("difficulty"));
        assertEquals("127.0.0.1", settings.getProperty("server-ip"));
        assertEquals("false", settings.getProperty("enable-rcon"));
        assertNull(settings.getProperty("rcon.password"));
    }

    @Test
    void replacingAnInstallationRemovesObsoleteMods() throws Exception {
        Path env = temp.resolve("environment");
        Path runtime = temp.resolve("runtime");
        write(env.resolve("mods/new.jar"), "new");
        write(runtime.resolve("mods/old.jar"), "old");
        RuntimeInstallation.install(env, runtime, "world", Map.of());
        assertFalse(Files.exists(runtime.resolve("mods/old.jar")));
        assertEquals("new", Files.readString(runtime.resolve("mods/new.jar")));
    }

    @Test
    void repeatedJoinsKeepOnlyOnePreviousRuntime() throws Exception {
        Path env = temp.resolve("environment");
        Path runtime = temp.resolve("runtime");
        write(env.resolve("mods/current.jar"), "mod");
        for (int attempt = 0; attempt < 4; attempt++) RuntimeInstallation.install(env, runtime, "world", Map.of());
        try (var siblings = Files.list(temp)) {
            assertEquals(1, siblings.filter(path -> path.getFileName().toString().startsWith("runtime.previous-")).count());
        }
    }

    @Test
    void refusesOverlappingSourceAndRuntimeDirectories() throws Exception {
        Path env = temp.resolve("environment");
        write(env.resolve("config/test"), "test");
        assertThrows(java.io.IOException.class,
                () -> RuntimeInstallation.install(env, env.resolve("runtime"), "world", Map.of()));
        assertEquals("test", Files.readString(env.resolve("config/test")));
    }

    @Test
    void failedInstallDoesNotModifyExistingRuntime() throws Exception {
        Path runtime = temp.resolve("runtime");
        write(runtime.resolve("config/old"), "old");
        assertThrows(java.io.IOException.class, () -> RuntimeInstallation.install(
                temp.resolve("missing"), runtime, "world", Map.of()));
        assertEquals("old", Files.readString(runtime.resolve("config/old")));
    }

    private static void write(Path path, String value) throws Exception {
        Files.createDirectories(path.getParent());
        Files.writeString(path, value);
    }
}
