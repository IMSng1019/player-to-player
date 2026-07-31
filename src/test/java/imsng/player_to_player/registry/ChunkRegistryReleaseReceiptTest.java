package imsng.player_to_player.registry;

import imsng.player_to_player.util.Sha256;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ChunkRegistryReleaseReceiptTest {

    private static final UUID GROUP =
            UUID.fromString("9207ce8d-2b04-4fd8-a9f7-a437ce1211a0");
    private static final UUID RELEASE_ID =
            UUID.fromString("129c3954-0249-45da-b98f-739dc5dab118");
    private static final ChunkKey KEY =
            new ChunkKey("minecraft:overworld", 8, -11);

    @TempDir
    Path temporary;

    @Test
    void successfulReceiptSurvivesCheckpointAndServerRestart() {
        MemoryStore store = new MemoryStore();
        Path journalFile = temporary.resolve("mutations.jsonl");
        ChunkRegistry first = new ChunkRegistry(
                store, null, new RegistryMutationJournal(journalFile));
        assertTrue(first.tryClaim(KEY, GROUP).granted());
        byte[] payload = {1, 2, 3};
        ChunkReleaseReceipt expected = new ChunkReleaseReceipt(
                RELEASE_ID, KEY, GROUP, payload.length,
                Sha256.hex(payload), true);

        assertEquals(expected, first.completeRelease(expected));
        assertNull(first.ownerOf(KEY));
        first.persist();

        ChunkRegistry recovered = new ChunkRegistry(
                store, null, new RegistryMutationJournal(journalFile));
        recovered.load();

        assertEquals(expected, recovered.findReleaseReceipt(
                RELEASE_ID, KEY, GROUP, payload.length, Sha256.hex(payload)));
        assertEquals(expected, recovered.completeRelease(expected),
                "ACK 丢失后的相同操作必须重放第一次成功结果");
        assertNull(recovered.ownerOf(KEY));
    }

    @Test
    void clientConfirmationDurablyRemovesNoLongerNeededReceipt() {
        MemoryStore store = new MemoryStore();
        Path journalFile = temporary.resolve("mutations.jsonl");
        ChunkRegistry first = new ChunkRegistry(
                store, null, new RegistryMutationJournal(journalFile));
        assertTrue(first.tryClaim(KEY, GROUP).granted());
        ChunkReleaseReceipt receipt = new ChunkReleaseReceipt(
                RELEASE_ID, KEY, GROUP, 0, Sha256.hex(new byte[0]), false);
        assertEquals(receipt, first.completeRelease(receipt));

        assertTrue(first.confirmReleaseReceipt(RELEASE_ID, GROUP));
        assertTrue(first.confirmReleaseReceipt(RELEASE_ID, GROUP),
                "确认消息自身重放也必须幂等");
        first.persist();

        ChunkRegistry recovered = new ChunkRegistry(
                store, null, new RegistryMutationJournal(journalFile));
        recovered.load();

        assertNull(recovered.findReleaseReceipt(
                RELEASE_ID, KEY, GROUP, 0, Sha256.hex(new byte[0])));
    }

    @Test
    void jsonOwnershipSnapshotCannotDeleteDurableReceiptSidecar() {
        Path registryDir = temporary.resolve("registry");
        JsonRegistryStore store = new JsonRegistryStore(registryDir);
        Path journalFile = registryDir.resolve("mutations.jsonl");
        ChunkRegistry first = new ChunkRegistry(
                store, null, new RegistryMutationJournal(journalFile));
        assertTrue(first.tryClaim(KEY, GROUP).granted());
        byte[] payload = {4, 5, 6};
        ChunkReleaseReceipt expected = new ChunkReleaseReceipt(
                RELEASE_ID, KEY, GROUP, payload.length,
                Sha256.hex(payload), true);
        assertEquals(expected, first.completeRelease(expected));
        first.persist();

        // 模拟下一轮 checkpoint 已写 ownership 快照、但尚未重写回执快照时崩溃。
        // JsonRegistryStore.save 会清理旧维度 JSON，但绝不能误删回执 sidecar。
        store.save(Map.of());
        ChunkRegistry recovered = new ChunkRegistry(
                store, null, new RegistryMutationJournal(journalFile));
        recovered.load();

        assertEquals(expected, recovered.findReleaseReceipt(
                RELEASE_ID, KEY, GROUP, payload.length, Sha256.hex(payload)));
    }

    @Test
    void reusedReleaseIdCannotClaimThatDifferentPayloadWasWritten() {
        ChunkRegistry registry = new ChunkRegistry(new MemoryStore(), null);
        assertTrue(registry.tryClaim(KEY, GROUP).granted());
        byte[] original = {1};
        ChunkReleaseReceipt receipt = new ChunkReleaseReceipt(
                RELEASE_ID, KEY, GROUP, original.length,
                Sha256.hex(original), true);
        assertEquals(receipt, registry.completeRelease(receipt));

        byte[] conflicting = {2};
        assertThrows(IllegalArgumentException.class,
                () -> registry.findReleaseReceipt(
                        RELEASE_ID, KEY, GROUP, conflicting.length,
                        Sha256.hex(conflicting)));
    }

    @Test
    void successfulReceiptCannotReleaseNonEmptyPayloadWithoutWrittenProof() {
        byte[] payload = {9};

        assertThrows(IllegalArgumentException.class,
                () -> new ChunkReleaseReceipt(
                        RELEASE_ID, KEY, GROUP, payload.length,
                        Sha256.hex(payload), false));
    }

    @Test
    void terminatedGroupCannotLeaveUnconfirmableReceiptsBehind() {
        MemoryStore store = new MemoryStore();
        Path journalFile = temporary.resolve("mutations.jsonl");
        ChunkRegistry registry = new ChunkRegistry(
                store, null, new RegistryMutationJournal(journalFile));
        assertTrue(registry.tryClaim(KEY, GROUP).granted());
        ChunkReleaseReceipt receipt = new ChunkReleaseReceipt(
                RELEASE_ID, KEY, GROUP, 0, Sha256.hex(new byte[0]), false);
        assertEquals(receipt, registry.completeRelease(receipt));

        assertEquals(0, registry.releaseAllDurably(GROUP));

        ChunkRegistry recovered = new ChunkRegistry(
                store, null, new RegistryMutationJournal(journalFile));
        recovered.load();
        assertNull(recovered.findReleaseReceipt(
                RELEASE_ID, KEY, GROUP, 0, Sha256.hex(new byte[0])));
    }

    private static final class MemoryStore implements RegistryStore {
        private Map<ChunkKey, UUID> claims = new HashMap<>();

        @Override
        public Map<ChunkKey, UUID> load() {
            return new HashMap<>(claims);
        }

        @Override
        public void save(Map<ChunkKey, UUID> snapshot) {
            claims = new HashMap<>(snapshot);
        }

        @Override
        public String describe() {
            return "memory";
        }
    }
}
