package imsng.player_to_player.env;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/** 使用真实磁盘观察快照临时副本，防止“最终 Blob 去重”掩盖全量重复制。 */
class EnvironmentSnapshotIncrementalTest {
    @TempDir Path temporary;

    @Test
    void changingSmallConfigDoesNotRecopyUnchangedMod() throws Exception {
        Path source = source();
        Path repository = temporary.resolve("repository");
        EnvironmentSnapshotStore store = new EnvironmentSnapshotStore(source, repository, List.of());
        EnvironmentSnapshot first = store.buildSnapshot();
        try (WatchService watcher = stagingWatcher(repository)) {
            Files.writeString(source.resolve("config/example.json"), "second");
            EnvironmentSnapshot next = store.buildSnapshot();
            assertNotEquals(first.snapshotId(), next.snapshotId());
            assertEquals("second", new String(store.readAllBytes(next, "config/example.json")));
            assertEquals(1, copiedFiles(watcher), "只允许为变化配置创建副本，不应重复制模组");
        }
    }

    @Test
    void unchangedRefreshDoesNotCreateAnyTemporaryCopies() throws Exception {
        Path source = source();
        Path repository = temporary.resolve("repository");
        EnvironmentSnapshotStore store = new EnvironmentSnapshotStore(source, repository, List.of());
        EnvironmentSnapshot first = store.buildSnapshot();
        try (WatchService watcher = stagingWatcher(repository)) {
            assertEquals(first.snapshotId(), store.buildSnapshot().snapshotId());
            assertEquals(0, copiedFiles(watcher), "重复刷新应复用不可变内容");
        }
    }

    @Test
    void fullVerificationDetectsContentChangesWithPreservedAttributes() throws Exception {
        Path source = source();
        Path config = source.resolve("config/example.json");
        EnvironmentSnapshotStore store = new EnvironmentSnapshotStore(source,
                temporary.resolve("repository"), List.of());
        EnvironmentSnapshot first = store.buildSnapshot();
        var modified = Files.getLastModifiedTime(config);
        Files.writeString(config, "other"); // 与 first 同长，并恢复 mtime，不能只比较大小/时间。
        Files.setLastModifiedTime(config, modified);
        EnvironmentSnapshot next = store.buildSnapshot();
        assertNotEquals(first.snapshotId(), next.snapshotId());
        assertEquals("other", new String(store.readAllBytes(next, "config/example.json")));
    }

    @Test
    void missingReusedBlobIsRebuiltFromSource() throws Exception {
        Path source = source();
        EnvironmentSnapshotStore store = new EnvironmentSnapshotStore(source,
                temporary.resolve("repository"), List.of());
        EnvironmentSnapshot first = store.buildSnapshot();
        Files.delete(store.blobPath(first.manifest().files().get("config/example.json").sha256()));
        EnvironmentSnapshot next = store.buildSnapshot();
        assertEquals(first.snapshotId(), next.snapshotId());
        assertEquals("first", new String(store.readAllBytes(next, "config/example.json")));
    }

    @Test
    void directoryNotificationRevalidatesChildrenWithMatchingAttributes() throws Exception {
        Path source = source();
        Path config = source.resolve("config/example.json");
        EnvironmentSnapshotStore store = new EnvironmentSnapshotStore(source,
                temporary.resolve("repository"), List.of());
        EnvironmentSnapshot first = store.buildSnapshot();
        var modified = Files.getLastModifiedTime(config);
        Files.writeString(config, "other");
        Files.setLastModifiedTime(config, modified);
        EnvironmentSnapshot next = store.buildSnapshot(Set.of("config"), false);
        assertNotEquals(first.snapshotId(), next.snapshotId());
        assertEquals("other", new String(store.readAllBytes(next, "config/example.json")));
        assertEquals(1, store.lastBuildStats().copiedFiles());
        assertEquals(1, store.lastBuildStats().reusedFiles());
    }

    @Test
    void incrementalBuildRemovesDeletedFilesAndReadsRecreatedContent() throws Exception {
        Path source = source();
        Path config = source.resolve("config/example.json");
        EnvironmentSnapshotStore store = new EnvironmentSnapshotStore(source,
                temporary.resolve("repository"), List.of());
        store.buildSnapshot();
        Files.delete(config);
        EnvironmentSnapshot removed = store.buildSnapshot(Set.of("config/example.json"), false);
        assertFalse(removed.manifest().files().containsKey("config/example.json"));
        Files.writeString(config, "restored");
        EnvironmentSnapshot restored = store.buildSnapshot(Set.of("config/example.json"), false);
        assertEquals("restored", new String(store.readAllBytes(restored, "config/example.json")));
        assertEquals(1, store.lastBuildStats().copiedFiles());
    }

    @Test
    void invalidManifestIsQuarantinedInsteadOfReprocessedOnEveryStart() throws Exception {
        Path repository = temporary.resolve("repository");
        Files.createDirectories(repository.resolve("manifests"));
        Path invalid = repository.resolve("manifests/broken.json");
        Files.writeString(invalid, "not-json");
        EnvironmentSnapshotStore store = new EnvironmentSnapshotStore(source(), repository, List.of());
        assertTrue(store.loadSnapshots().isEmpty());
        assertFalse(Files.exists(invalid));
        try (var files = Files.list(repository.resolve("quarantine"))) {
            assertEquals("not-json", Files.readString(files.findFirst().orElseThrow()));
        }
        assertTrue(store.loadSnapshots().isEmpty());
    }

    private Path source() throws Exception {
        Path source = temporary.resolve("source");
        Files.createDirectories(source.resolve("mods"));
        Files.createDirectories(source.resolve("config"));
        Files.write(source.resolve("mods/example.jar"), new byte[1024 * 1024]);
        Files.writeString(source.resolve("config/example.json"), "first");
        return source;
    }

    private WatchService stagingWatcher(Path repository) throws Exception {
        WatchService watcher = repository.getFileSystem().newWatchService();
        repository.resolve("staging").register(watcher, StandardWatchEventKinds.ENTRY_CREATE);
        return watcher;
    }

    private int copiedFiles(WatchService watcher) throws Exception {
        int copies = 0;
        WatchKey key = watcher.poll(250, TimeUnit.MILLISECONDS);
        while (key != null) {
            for (var event : key.pollEvents()) {
                assertNotEquals(StandardWatchEventKinds.OVERFLOW, event.kind());
                if (event.kind() == StandardWatchEventKinds.ENTRY_CREATE) copies += event.count();
            }
            key.reset();
            key = watcher.poll(50, TimeUnit.MILLISECONDS);
        }
        return copies;
    }
}
