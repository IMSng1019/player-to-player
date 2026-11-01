package imsng.player_to_player.env;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class EnvironmentSnapshotManagerTest {

    @TempDir
    Path tempDir;

    @Test
    void reconciliationDueDuringBuildRunsAfterPublicationCompletes() throws Exception {
        Path source = tempDir.resolve("busy-reconciliation");
        Files.createDirectories(source);
        Files.writeString(source.resolve("config.json"), "first");
        EnvironmentSnapshotManager manager = new EnvironmentSnapshotManager(source,
                tempDir.resolve("repository"), List.of(), Duration.ofMillis(10),
                Duration.ofSeconds(30), Duration.ofMinutes(10));
        CountDownLatch publishing = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        manager.onPublished(snapshot -> {
            publishing.countDown();
            try { release.await(5, TimeUnit.SECONDS); }
            catch (InterruptedException error) { Thread.currentThread().interrupt(); }
        });
        try {
            manager.start();
            assertTrue(publishing.await(5, TimeUnit.SECONDS));
            // 用真实发布回调固定住一轮构建，只提前触发维护时点，不修改内部状态标志。
            var maintenance = EnvironmentSnapshotManager.class.getDeclaredMethod("runMaintenance");
            maintenance.setAccessible(true);
            maintenance.invoke(manager);
            var storage = EnvironmentSnapshotManager.class.getDeclaredField("store");
            storage.setAccessible(true);
            EnvironmentSnapshotStore store = (EnvironmentSnapshotStore) storage.get(manager);
            assertEquals(1, store.lastBuildStats().copiedFiles());
            release.countDown();
            // 第二轮无变化，应实际完成校验并复用首轮内容，不能把忙碌时的维护请求丢掉。
            await(() -> store.lastBuildStats().reusedFiles() == 1);
        } finally {
            release.countDown();
            manager.stop();
        }
    }

    @Test
    void continuousRefreshRequestsDoNotStarvePublication() throws Exception {
        Path source = tempDir.resolve("continuous");
        Files.createDirectories(source);
        Path file = source.resolve("config.json");
        Files.writeString(file, "first");
        EnvironmentSnapshotManager manager = new EnvironmentSnapshotManager(source,
                tempDir.resolve("repository"), List.of(), Duration.ofMillis(150),
                Duration.ofSeconds(30), Duration.ofMinutes(10));
        try {
            manager.start();
            await(() -> manager.current() != null);
            String first = manager.current().snapshotId();
            Files.writeString(file, "second");
            long deadline = System.nanoTime() + Duration.ofMillis(700).toNanos();
            while (System.nanoTime() < deadline) {
                manager.requestRefresh();
                Thread.sleep(10);
            }
            assertNotEquals(first, manager.current().snapshotId(), "连续通知不能无限重置防抖计时");
        } finally {
            manager.stop();
        }
    }

    @Test
    void periodicVerificationRepairsMissedChangeEvenWhenAttributesMatch() throws Exception {
        Path source = tempDir.resolve("missed-event");
        Files.createDirectories(source);
        Path file = source.resolve("config.json");
        Files.writeString(file, "first");
        EnvironmentSnapshotManager manager = new EnvironmentSnapshotManager(source,
                tempDir.resolve("repository"), List.of(), Duration.ofMillis(20),
                Duration.ofMillis(80), Duration.ofMinutes(10));
        try {
            manager.start();
            await(() -> manager.current() != null);
            String first = manager.current().snapshotId();
            manager.beginSourceUpdate(); // 停止监听，模拟文件系统漏事件。
            var modified = Files.getLastModifiedTime(file);
            Files.writeString(file, "other");
            Files.setLastModifiedTime(file, modified);
            manager.endSourceUpdate(false); // 不主动请求构建，必须由周期校验发现。
            await(() -> !first.equals(manager.current().snapshotId()));
        } finally {
            manager.stop();
        }
    }

    @Test
    void publicationListenersOnlyReceiveChangedVersionsAndCanUnsubscribe() throws Exception {
        Path source = tempDir.resolve("source-listener");
        Files.createDirectories(source.resolve("config"));
        Files.writeString(source.resolve("config/value.txt"), "first");
        EnvironmentSnapshotManager manager = new EnvironmentSnapshotManager(source,
                tempDir.resolve("repository-listener"), List.of(), Duration.ofMillis(10),
                Duration.ofSeconds(30), Duration.ofMinutes(10));
        List<String> observed = new CopyOnWriteArrayList<>();
        AutoCloseable broken = manager.onPublished(ignored -> { throw new IllegalStateException("listener failed"); });
        AutoCloseable subscription = manager.onPublished(snapshot -> observed.add(snapshot.snapshotId()));
        try {
            manager.start();
            await(() -> observed.size() == 1);
            manager.requestRefresh();
            Thread.sleep(100);
            assertEquals(1, observed.size());
            Files.writeString(source.resolve("config/value.txt"), "second");
            manager.requestRefresh();
            await(() -> observed.size() == 2);
            assertEquals(manager.current().snapshotId(), observed.get(1));
            subscription.close();
            String second = manager.current().snapshotId();
            Files.writeString(source.resolve("config/value.txt"), "third");
            manager.requestRefresh();
            await(() -> !manager.current().snapshotId().equals(second));
            assertEquals(2, observed.size());
        } finally {
            subscription.close();
            broken.close();
            manager.stop();
        }
    }

    @Test
    void publishesNewSnapshotAfterIncludedSourceFileChanges() throws Exception {
        Path sourceRoot = tempDir.resolve("source");
        Path repositoryRoot = tempDir.resolve("repository");
        Path levelDat = sourceRoot.resolve("world/serverconfig/settings.json");
        Files.createDirectories(levelDat.getParent());
        Files.writeString(levelDat, "before", StandardCharsets.UTF_8);

        EnvironmentSnapshotManager manager = new EnvironmentSnapshotManager(
                sourceRoot, repositoryRoot, List.of(),
                Duration.ofMillis(50), Duration.ofSeconds(30), Duration.ofMinutes(10));
        try {
            manager.start();
            await(() -> manager.current() != null);
            EnvironmentSnapshot first = manager.current();
            assertNotNull(first);

            Files.writeString(levelDat, "after-change", StandardCharsets.UTF_8);

            await(() -> manager.current() != null
                    && !manager.current().snapshotId().equals(first.snapshotId()));
            assertNotEquals(first.snapshotId(), manager.current().snapshotId());
        } finally {
            manager.stop();
        }
    }

    @Test
    void removesExpiredOldSnapshotButKeepsCurrentSnapshot() throws Exception {
        Path sourceRoot = tempDir.resolve("source-retention");
        Path repositoryRoot = tempDir.resolve("repository-retention");
        Path file = sourceRoot.resolve("config/value.txt");
        Files.createDirectories(file.getParent());
        Files.writeString(file, "v1", StandardCharsets.UTF_8);

        EnvironmentSnapshotManager manager = new EnvironmentSnapshotManager(
                sourceRoot, repositoryRoot, List.of(),
                Duration.ofMillis(20), Duration.ofMillis(40), Duration.ofMillis(120));
        try {
            manager.start();
            await(() -> manager.current() != null);
            String firstId = manager.current().snapshotId();

            Files.writeString(file, "version-two", StandardCharsets.UTF_8);
            await(() -> manager.current() != null
                    && !manager.current().snapshotId().equals(firstId));
            String currentId = manager.current().snapshotId();

            Path manifests = repositoryRoot.resolve("manifests");
            await(() -> !Files.exists(manifests.resolve(firstId + ".json")));

            assertTrue(Files.isRegularFile(manifests.resolve(currentId + ".json")));
        } finally {
            manager.stop();
        }
    }

    @Test
    void sourceUpdateCanFinishAfterManagerStops() throws Exception {
        Path sourceRoot = tempDir.resolve("source-stop");
        Path repositoryRoot = tempDir.resolve("repository-stop");
        Files.createDirectories(sourceRoot);

        EnvironmentSnapshotManager manager = new EnvironmentSnapshotManager(
                sourceRoot, repositoryRoot, List.of(),
                Duration.ofMillis(20), Duration.ofSeconds(30), Duration.ofMinutes(10));
        manager.start();
        await(() -> manager.current() != null);
        manager.beginSourceUpdate();

        manager.stop();

        assertDoesNotThrow(() -> manager.endSourceUpdate(false));
    }

    @Test
    void replacingSourceRootRegistersWatchesOnTheNewDirectory() throws Exception {
        Path source = tempDir.resolve("source-replaced");
        Files.createDirectories(source.resolve("config"));
        Files.writeString(source.resolve("config/value.txt"), "first");
        EnvironmentSnapshotManager manager = new EnvironmentSnapshotManager(source,
                tempDir.resolve("repository-replaced"), List.of(), Duration.ofMillis(10),
                Duration.ofSeconds(30), Duration.ofMinutes(10));
        try {
            manager.start();
            await(() -> manager.current() != null);
            String first = manager.current().snapshotId();
            manager.beginSourceUpdate();
            Files.move(source, tempDir.resolve("old-root"));
            Files.createDirectories(source.resolve("config"));
            Files.writeString(source.resolve("config/value.txt"), "second");
            manager.endSourceUpdate(true);
            await(() -> !manager.current().snapshotId().equals(first));
            String second = manager.current().snapshotId();
            Files.writeString(source.resolve("config/value.txt"), "third");
            await(() -> !manager.current().snapshotId().equals(second));
        } finally {
            manager.stop();
        }
    }

    @Test
    void sourceUpdateBatchKeepsOldSnapshotUntilSuccessfulEnd() throws Exception {
        Path sourceRoot = tempDir.resolve("source-batch");
        Path repositoryRoot = tempDir.resolve("repository-batch");
        Path file = sourceRoot.resolve("config/value.txt");
        Files.createDirectories(file.getParent());
        Files.writeString(file, "before", StandardCharsets.UTF_8);

        EnvironmentSnapshotManager manager = new EnvironmentSnapshotManager(
                sourceRoot, repositoryRoot, List.of(),
                Duration.ofMillis(30), Duration.ofSeconds(30), Duration.ofMinutes(10));
        try {
            manager.start();
            await(() -> manager.current() != null);
            String firstId = manager.current().snapshotId();

            manager.beginSourceUpdate();
            Files.writeString(file, "after-batch", StandardCharsets.UTF_8);
            Thread.sleep(150);

            assertEquals(firstId, manager.current().snapshotId());

            manager.endSourceUpdate(true);
            await(() -> !manager.current().snapshotId().equals(firstId));
        } finally {
            manager.stop();
        }
    }

    private static void await(BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
        while (!condition.getAsBoolean() && System.nanoTime() < deadline) {
            Thread.sleep(20);
        }
        if (!condition.getAsBoolean()) {
            throw new AssertionError("等待条件超时");
        }
    }
}
