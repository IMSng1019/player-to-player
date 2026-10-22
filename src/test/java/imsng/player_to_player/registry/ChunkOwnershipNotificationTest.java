package imsng.player_to_player.registry;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class ChunkOwnershipNotificationTest {
    private static final UUID GROUP = UUID.randomUUID();
    private static final UUID NEXT = UUID.randomUUID();
    private static final ChunkKey KEY = new ChunkKey("minecraft:overworld", 1, 2);
    @TempDir Path temporary;

    @Test
    void failedWalAppendDoesNotNotifyOrReleaseOwnership() throws Exception {
        Path file = temporary.resolve("registry.jsonl");
        ChunkRegistry registry = registry(file);
        registry.tryClaim(KEY, GROUP);
        List<UUID> changed = new ArrayList<>();
        registry.setOwnershipChangeListener(changed::add);
        Files.delete(file);
        Files.createDirectory(file);

        assertThrows(IllegalStateException.class, () -> registry.release(KEY, GROUP));
        assertTrue(changed.isEmpty());
        assertEquals(GROUP, registry.ownerOf(KEY));
    }

    @Test
    void releaseAndMigrationNotifyOnlyAfterDurableChange() {
        Path file = temporary.resolve("registry.jsonl");
        ChunkRegistry registry = registry(file);
        List<UUID> changed = new ArrayList<>();
        registry.setOwnershipChangeListener(group -> {
            ChunkRegistry replayed = registry(file);
            replayed.load();
            assertEquals(registry.ownerOf(KEY), replayed.ownerOf(KEY),
                    "通知发布前WAL必须足以重放已变更的归属");
            changed.add(group);
        });
        registry.tryClaim(KEY, GROUP);
        assertTrue(changed.isEmpty(), "授予不能伪装成释放通知");
        assertEquals(1, registry.migrateAll(GROUP, NEXT));
        assertEquals(List.of(GROUP), changed);
        assertFalse(registry.release(KEY, GROUP));
        assertTrue(registry.release(KEY, NEXT));
        assertEquals(List.of(GROUP, NEXT), changed);
    }

    @Test
    void successfulReceiptReplayAndEmptyExitDoNotDuplicateNotifications() {
        ChunkRegistry registry = registry(temporary.resolve("registry.jsonl"));
        List<UUID> changed = new ArrayList<>();
        registry.setOwnershipChangeListener(changed::add);
        registry.tryClaim(KEY, GROUP);
        ChunkReleaseReceipt receipt = new ChunkReleaseReceipt(UUID.randomUUID(), KEY, GROUP,
                0, imsng.player_to_player.util.Sha256.hex(new byte[0]), false);
        assertEquals(receipt, registry.completeRelease(receipt));
        assertEquals(receipt, registry.completeRelease(receipt));
        assertEquals(List.of(GROUP), changed);
        registry.tryClaim(KEY, GROUP);
        registry.freezeForExit(GROUP);
        assertEquals(1, registry.completeExitDurably(GROUP, Set.of(KEY)));
        assertEquals(0, registry.completeExitDurably(GROUP, Set.of(KEY)));
        assertEquals(List.of(GROUP, GROUP), changed);
    }

    @Test
    void listenerFailureCannotTurnDurableReleaseIntoFailure() {
        ChunkRegistry registry = registry(temporary.resolve("registry.jsonl"));
        registry.tryClaim(KEY, GROUP);
        registry.setOwnershipChangeListener(group -> { throw new IllegalStateException("network"); });
        assertEquals(1, registry.releaseAllDurably(GROUP));
        assertNull(registry.ownerOf(KEY));
    }

    private static ChunkRegistry registry(Path journal) {
        return new ChunkRegistry(new RegistryStore() {
            @Override public Map<ChunkKey, UUID> load() { return Map.of(); }
            @Override public void save(Map<ChunkKey, UUID> snapshot) { }
            @Override public String describe() { return "test"; }
        }, null, new RegistryMutationJournal(journal));
    }
}
