package imsng.player_to_player.env;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

class EnvironmentSnapshotStoreTest {

    @TempDir
    Path tempDir;

    @Test
    void upgradedPolicyInvalidatesOldCustomWorldMetadataSnapshotsAndDirectReads() throws Exception {
        Path source = tempDir.resolve("source");
        Path repository = tempDir.resolve("repository");
        String oldPath = "saves/custom-world/level.dat";
        Files.createDirectories(source.resolve(oldPath).getParent());
        Files.writeString(source.resolve(oldPath), "old-protocol-world-metadata");
        // 模拟旧版/不知实际存档路径的中转端已经发布过该文件。
        var previous = new EnvironmentSnapshotStore(source, repository, List.of()).buildSnapshot();
        var upgraded = new EnvironmentSnapshotStore(source, repository,
                EnvironmentPathPolicy.worldMetadataExclusions("saves/custom-world"));
        assertTrue(upgraded.loadSnapshots().isEmpty(), "不能按旧snapshotId绕过当前排除规则");
        assertThrows(java.io.IOException.class, () -> upgraded.readAllBytes(previous, oldPath));
        assertTrue(upgraded.buildSnapshot().manifest().files().isEmpty());
    }

    @Test
    void publishedSnapshotKeepsBytesAfterLiveFileChanges() throws Exception {
        Path sourceRoot = tempDir.resolve("source");
        Path repositoryRoot = tempDir.resolve("repository");
        Path levelDat = sourceRoot.resolve("world/serverconfig/settings.json");
        Files.createDirectories(levelDat.getParent());
        Files.writeString(levelDat, "old", StandardCharsets.UTF_8);

        EnvironmentSnapshotStore store = new EnvironmentSnapshotStore(
                sourceRoot, repositoryRoot, List.of());
        EnvironmentSnapshot snapshot = store.buildSnapshot();

        Files.writeString(levelDat, "new-content", StandardCharsets.UTF_8);

        assertEquals("old", new String(
                store.readAllBytes(snapshot, "world/serverconfig/settings.json"), StandardCharsets.UTF_8));
    }

    @Test
    void persistedSnapshotCanBeLoadedAfterStoreRestart() throws Exception {
        Path sourceRoot = tempDir.resolve("source");
        Path repositoryRoot = tempDir.resolve("repository");
        Path config = sourceRoot.resolve("config/example.json");
        Files.createDirectories(config.getParent());
        Files.writeString(config, "stable", StandardCharsets.UTF_8);

        EnvironmentSnapshotStore firstStore = new EnvironmentSnapshotStore(
                sourceRoot, repositoryRoot, List.of());
        EnvironmentSnapshot published = firstStore.buildSnapshot();

        EnvironmentSnapshotStore restartedStore = new EnvironmentSnapshotStore(
                sourceRoot, repositoryRoot, List.of());
        Map<String, EnvironmentSnapshot> loaded = restartedStore.loadSnapshots();

        EnvironmentSnapshot restored = loaded.get(published.snapshotId());
        assertEquals("stable", new String(
                restartedStore.readAllBytes(restored, "config/example.json"),
                StandardCharsets.UTF_8));
    }

    @Test
    void readsRequestedRangeFromSnapshotBlob() throws Exception {
        Path sourceRoot = tempDir.resolve("source");
        Path repositoryRoot = tempDir.resolve("repository");
        Path file = sourceRoot.resolve("config/data.txt");
        Files.createDirectories(file.getParent());
        Files.writeString(file, "0123456789", StandardCharsets.UTF_8);

        EnvironmentSnapshotStore store = new EnvironmentSnapshotStore(
                sourceRoot, repositoryRoot, List.of());
        EnvironmentSnapshot snapshot = store.buildSnapshot();

        assertEquals("3456", new String(
                store.readChunk(snapshot, "config/data.txt", 3, 4),
                StandardCharsets.UTF_8));
    }
}
