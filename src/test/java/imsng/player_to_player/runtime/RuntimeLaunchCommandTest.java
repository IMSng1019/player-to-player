package imsng.player_to_player.runtime;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

@org.junit.jupiter.api.parallel.ResourceLock(org.junit.jupiter.api.parallel.Resources.SYSTEM_PROPERTIES)
class RuntimeLaunchCommandTest {
    @TempDir Path temp;
    private String developmentSpec;

    @org.junit.jupiter.api.BeforeEach
    void selectProductionLauncher() {
        // 同一轮可同时运行真实Loom子进程冒烟；这两个测试专门验证正式发行JAR入口。
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
        Files.writeString(temp.resolve("fabric-server-launch.jar"), "fixture");
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
}
