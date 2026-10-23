package imsng.player_to_player.env;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
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
        Path levelDat = sourceRoot.resolve("world/level.dat");
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
