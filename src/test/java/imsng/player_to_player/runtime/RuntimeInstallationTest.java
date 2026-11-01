package imsng.player_to_player.runtime;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
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

    @Test
    void installingUnchangedModsPreservesTheirModificationTime() throws Exception {
        Path env = temp.resolve("environment");
        Path runtime = temp.resolve("runtime");
        Path mod = env.resolve("mods/example.jar");
        write(mod, "mod");
        FileTime original = FileTime.fromMillis(1_700_000_000_000L);
        Files.setLastModifiedTime(mod, original);

        RuntimeInstallation.install(env, runtime, "world", Map.of());

        assertEquals(original, Files.getLastModifiedTime(runtime.resolve("mods/example.jar")),
                "未变化的模组必须保留修改时间，否则 Loader 会把每次进组都视为新文件而重建缓存");
    }

    @Test
    void sameRuntimeReusesOnlyLocalLoaderCachesAsIndependentCopies() throws Exception {
        Path env = temp.resolve("environment");
        Path runtime = temp.resolve("runtime");
        write(env.resolve("mods/example.jar"), "mod");
        write(env.resolve("config/example.json"), "authority-config");
        RuntimeInstallation.install(env, runtime, "world", Map.of(), "same-runtime");
        write(runtime.resolve(".fabric/remappedJars/game/server.jar"), "remapped-game");
        write(runtime.resolve(".fabric/processedMods/example.jar"), "processed-mod");
        write(runtime.resolve("config/example.json"), "runtime-mutated-config");
        write(runtime.resolve(".fabric/tmp/partial.jar"), "partial");
        write(runtime.resolve("mods/old.jar"), "obsolete");
        FileTime modified = FileTime.fromMillis(1_700_000_000_000L);
        Files.setLastModifiedTime(runtime.resolve(".fabric/processedMods/example.jar"), modified);
        RuntimeInstallation.markReady(runtime, "same-runtime");

        RuntimeInstallation.install(env, runtime, "world", Map.of(), "same-runtime");

        assertTrue(Files.exists(runtime.resolve(".fabric/remappedJars/game/server.jar")),
                "同一环境再次启动不得丢弃本机已生成的游戏重映射缓存");
        assertEquals("processed-mod", Files.readString(runtime.resolve(".fabric/processedMods/example.jar")));
        assertEquals(modified, Files.getLastModifiedTime(runtime.resolve(".fabric/processedMods/example.jar")));
        assertEquals("authority-config", Files.readString(runtime.resolve("config/example.json")));
        assertFalse(Files.exists(runtime.resolve(".fabric/tmp")));
        assertFalse(Files.exists(runtime.resolve("mods/old.jar")));
        try (var siblings = Files.list(temp)) {
            Path previous = siblings.filter(path -> path.getFileName().toString().startsWith("runtime.previous-"))
                    .findFirst().orElseThrow();
            Path cache = runtime.resolve(".fabric/processedMods/example.jar");
            Path oldCache = previous.resolve(".fabric/processedMods/example.jar");
            assertFalse(Files.isSameFile(cache, oldCache), "本地缓存也不能用硬链接破坏上一版运行副本");
            Files.writeString(cache, "changed");
            assertEquals("processed-mod", Files.readString(oldCache));
        }
    }

    @Test
    void changedRuntimeNeverReusesPreviousLoaderCaches() throws Exception {
        Path env = temp.resolve("environment");
        Path runtime = temp.resolve("runtime");
        write(env.resolve("mods/example.jar"), "mod");
        RuntimeInstallation.install(env, runtime, "world", Map.of(), "runtime-old");
        write(runtime.resolve(".fabric/remappedJars/game/server.jar"), "old-game");
        write(runtime.resolve(".fabric/processedMods/example.jar"), "old-mod");
        RuntimeInstallation.markReady(runtime, "runtime-old");

        RuntimeInstallation.install(env, runtime, "world", Map.of(), "runtime-new");

        assertFalse(Files.exists(runtime.resolve(".fabric/remappedJars")));
        assertFalse(Files.exists(runtime.resolve(".fabric/processedMods")));
    }

    @Test
    void incompleteStartupDoesNotReusePartiallyWrittenLoaderCaches() throws Exception {
        Path env = temp.resolve("environment");
        Path runtime = temp.resolve("runtime");
        write(env.resolve("mods/example.jar"), "mod");
        RuntimeInstallation.install(env, runtime, "world", Map.of(), "same-runtime");
        write(runtime.resolve(".fabric/remappedJars/game/server.jar"), "partial-remap");
        write(runtime.resolve(".fabric/processedMods/example.jar"), "partial-mod");

        RuntimeInstallation.install(env, runtime, "world", Map.of(), "same-runtime");

        assertFalse(Files.exists(runtime.resolve(".fabric/remappedJars")),
                "计算进程未完成真实 READY 时，下一次不能复用可能只写了一半的派生缓存");
        assertFalse(Files.exists(runtime.resolve(".fabric/processedMods")));
    }

    private static void write(Path path, String value) throws Exception {
        Files.createDirectories(path.getParent());
        Files.writeString(path, value);
    }
}
