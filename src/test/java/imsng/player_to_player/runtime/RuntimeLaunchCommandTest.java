package imsng.player_to_player.runtime;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.jar.Attributes;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;
import static org.junit.jupiter.api.Assertions.*;

@org.junit.jupiter.api.parallel.ResourceLock(org.junit.jupiter.api.parallel.Resources.SYSTEM_PROPERTIES)
class RuntimeLaunchCommandTest {
    @TempDir Path temp;
    private String developmentSpec;

    @org.junit.jupiter.api.BeforeEach
    void selectProductionLauncher() {
        // 同一轮可同时运行真实Loom子进程冒烟；本组测试专门验证正式发行JAR入口。
        developmentSpec = System.getProperty("p2p.worker.developmentSpec");
        System.clearProperty("p2p.worker.developmentSpec");
    }

    @org.junit.jupiter.api.AfterEach
    void restoreDevelopmentLauncher() {
        if (developmentSpec == null) System.clearProperty("p2p.worker.developmentSpec");
        else System.setProperty("p2p.worker.developmentSpec", developmentSpec);
    }

    @Test
    void buildsDirectJavaArgumentsWithSeparateWorldDirectory() throws Exception {
        launcher("fabric-server-launch.jar", "net.fabricmc.loader.impl.launch.server.FabricServerLauncher");
        Path saves = temp.resolve("保存目录");
        Path session = temp.resolve("session.json");
        List<String> command = RuntimeLaunchCommand.build(Path.of("java"), temp, saves, "my world", session);
        assertEquals("java", command.get(0));
        assertTrue(command.contains("-Dp2p.worker.session=" + session.toAbsolutePath()));
        assertEquals(temp.resolve("fabric-server-launch.jar").toAbsolutePath().toString(),
                command.get(command.indexOf("-jar") + 1));
        assertEquals(saves.toAbsolutePath().toString(), command.get(command.indexOf("--universe") + 1));
        assertEquals("my world", command.get(command.indexOf("--world") + 1));
        assertFalse(command.stream().anyMatch(value -> value.endsWith(".bat") || value.equals("cmd")));
    }

    @Test
    void missingServerLauncherFailsBeforeProcessStart() {
        assertThrows(java.io.IOException.class, () -> RuntimeLaunchCommand.build(
                Path.of("java"), temp, temp.resolve("saves"), "world", temp.resolve("session.json")));
    }

    @Test
    void acceptsOfficialVersionedLauncherBesideUnrelatedJarsAndOldCaches() throws Exception {
        Path expected = launcher("fabric-server-mc.1.20.4-loader.0.19.5-launcher.1.1.2.jar",
                "net.fabricmc.installer.ServerLauncher");
        launcher("p2p-auth-check.jar", "AuthConnectivityProbe");
        launcher("server.jar", "net.minecraft.bundler.Main");
        Files.writeString(temp.resolve("unrelated-broken.jar"), "not a jar");
        launcher(".fabric/server/old-loader.jar", "net.fabricmc.loader.impl.launch.server.FabricServerLauncher");
        assertEquals(expected, selectedLauncher());
    }

    @Test
    void acceptsRenamedFabricLauncherWithoutDependingOnFilename() throws Exception {
        Path expected = launcher("服务器 启动包.jar", "net.fabricmc.installer.ServerLauncher");
        assertEquals(expected, selectedLauncher());
    }

    @Test
    void canonicalFilenameDoesNotMakeAnUnrelatedJarAFabricLauncher() throws Exception {
        launcher("fabric-server-launch.jar", "AuthConnectivityProbe");
        assertThrows(java.io.IOException.class, this::selectedLauncher);
    }

    @Test
    void rejectsAmbiguousLaunchersInsteadOfChoosingAnArbitraryVersion() throws Exception {
        launcher("fabric-server-launch.jar", "net.fabricmc.loader.impl.launch.server.FabricServerLauncher");
        launcher("fabric-server-new.jar", "net.fabricmc.installer.ServerLauncher");
        var failure = assertThrows(java.io.IOException.class, this::selectedLauncher);
        assertTrue(failure.getMessage().contains("fabric-server-launch.jar"));
        assertTrue(failure.getMessage().contains("fabric-server-new.jar"));
    }

    @Test
    void missingManifestOrCachedLauncherAloneCannotBeUsed() throws Exception {
        try (var jar = new JarOutputStream(Files.newOutputStream(temp.resolve("fabric-server-launch.jar")))) { }
        launcher(".fabric/server/cached.jar", "net.fabricmc.loader.impl.launch.server.FabricServerLauncher");
        assertThrows(java.io.IOException.class, this::selectedLauncher);
    }

    private Path selectedLauncher() throws Exception {
        List<String> command = RuntimeLaunchCommand.build(Path.of("java"), temp,
                temp.resolve("saves"), "world", temp.resolve("session.json"));
        return Path.of(command.get(command.indexOf("-jar") + 1));
    }

    /** 使用真实 JAR 清单覆盖入口识别，避免纯文本假包掩盖发行环境的问题。 */
    private Path launcher(String filename, String mainClass) throws Exception {
        Path file = temp.resolve(filename).toAbsolutePath();
        Files.createDirectories(file.getParent());
        Manifest manifest = new Manifest();
        manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
        manifest.getMainAttributes().put(Attributes.Name.MAIN_CLASS, mainClass);
        try (var jar = new JarOutputStream(Files.newOutputStream(file), manifest)) { }
        return file;
    }
}
