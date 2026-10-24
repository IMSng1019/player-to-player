package imsng.player_to_player.env.restart;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static imsng.player_to_player.env.restart.RestartTask.*;
import static org.junit.jupiter.api.Assertions.*;

class RestartTaskStoreTest {
    @TempDir Path root;

    @Test void persistedTaskContainsOnlyRecoveryMetadataAndPreservesUnicode() throws Exception {
        var store = new RestartTaskStore(root.resolve("共享 数据"));
        var task = store.create(UUID.randomUUID(), "[2001:db8::1]:25579", "a".repeat(64),
                root.resolve("游戏 空格"));
        assertEquals(task, new RestartTaskStore(store.dataRoot()).read(task.id()));
        String disk = Files.readString(store.taskFile(task.id()));
        assertFalse(disk.contains("command"));
        assertFalse(disk.contains("accessToken"));
        assertFalse(disk.contains("username"));
        assertFalse(disk.contains("javaagent"));
    }

    @Test void clientStartedAcknowledgmentCannotBeOverwrittenByTimeoutOrWrongRuntime() throws Exception {
        var store = new RestartTaskStore(root);
        var task = store.create(UUID.randomUUID(), "host:25565", "a".repeat(64), root.resolve("游戏"));
        store.update(task.id(), old -> old.stage(Stage.LAUNCHING, Failure.NONE));
        assertFalse(store.clientStarted(task.id(), task.directory(), "b".repeat(64)));
        assertFalse(store.clientStarted(task.id(), root.resolve("wrong"), task.runtimeHash()));
        assertTrue(store.clientStarted(task.id(), task.directory(), task.runtimeHash()));
        assertFalse(store.clientStarted(task.id(), root.resolve("wrong"), task.runtimeHash()));
        store.finishUnconfirmed(task.id(), Failure.INITIALIZATION_UNCONFIRMED);
        assertEquals(Stage.CLIENT_STARTED, store.read(task.id()).stage());
    }

    @Test void lateAcknowledgmentAfterFiveMinutesIsAccepted() throws Exception {
        var store = new RestartTaskStore(root);
        var task = store.create(UUID.randomUUID(), "host:25565", "a".repeat(64), root);
        store.update(task.id(), old -> old.stage(Stage.LAUNCHING, Failure.NONE));
        store.finishUnconfirmed(task.id(), Failure.INITIALIZATION_UNCONFIRMED);
        assertEquals(Stage.INITIALIZATION_UNCONFIRMED, store.read(task.id()).stage());
        assertTrue(store.clientStarted(task.id(), task.directory(), task.runtimeHash()));
    }

    @Test void concurrentReadersNeverSeePartialState() throws Exception {
        var store = new RestartTaskStore(root);
        var task = store.create(UUID.randomUUID(), "host:25565", "a".repeat(64), root);
        var writer = CompletableFuture.runAsync(() -> {
            try {
                for (int i = 0; i < 30; i++) store.update(task.id(), old -> old.stage(Stage.READY, Failure.NONE));
            } catch (Exception e) { throw new RuntimeException(e); }
        });
        for (int i = 0; i < 100; i++) assertEquals(task.chainId(), store.read(task.id()).chainId());
        writer.join();
    }
}
