package imsng.player_to_player.runtime;

import imsng.player_to_player.env.EnvironmentManifest;
import imsng.player_to_player.util.Sha256;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.io.IOException;
import java.nio.file.*;
import java.util.Map;
import static org.junit.jupiter.api.Assertions.*;

class WorkerEnvironmentProofTest {
    @TempDir Path root;
    @Test void validatesRuntimeCopyAndRealSaveResourcesWhileLeavingMutableWorldDataAlone() throws Exception {
        Path runtime = root.resolve("runtime"), world = root.resolve("data/world");
        Files.createDirectories(runtime.resolve("config")); Files.createDirectories(world.resolve("serverconfig"));
        Files.writeString(runtime.resolve("config/test.json"), "config");
        Files.writeString(world.resolve("serverconfig/world.json"), "world-config");
        var manifest = new EnvironmentManifest(Map.of(
                "config/test.json", entry(runtime.resolve("config/test.json")),
                "world/serverconfig/world.json", entry(world.resolve("serverconfig/world.json")),
                "world/level.dat", new EnvironmentManifest.Entry("dynamic-is-not-overwritten", 100)));
        assertDoesNotThrow(() -> WorkerEnvironmentProof.verifyInstalled(runtime, world, "world", manifest));
        Files.writeString(world.resolve("serverconfig/world.json"), "modified");
        assertThrows(IOException.class, () -> WorkerEnvironmentProof.verifyInstalled(runtime, world, "world", manifest));
    }
    @Test void rejectsCopyThatChangedAfterDownloadValidation() throws Exception {
        Path runtime = root.resolve("runtime"); Files.createDirectories(runtime.resolve("mods"));
        Path file = runtime.resolve("mods/test.jar"); Files.writeString(file, "before");
        var manifest = new EnvironmentManifest(Map.of("mods/test.jar", entry(file)));
        Files.writeString(file, "after!");
        assertThrows(IOException.class, () -> WorkerEnvironmentProof.verifyInstalled(runtime, root.resolve("world"), "world", manifest));
    }
    private static EnvironmentManifest.Entry entry(Path file) throws IOException {
        return new EnvironmentManifest.Entry(Sha256.hexOfFile(file), Files.size(file));
    }
}
