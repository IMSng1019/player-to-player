package imsng.player_to_player.registry;

import com.google.gson.JsonObject;
import imsng.player_to_player.util.JsonUtil;
import imsng.player_to_player.util.Sha256;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RegistryMutationJournalTest {

    private static final UUID GROUP =
            UUID.fromString("9207ce8d-2b04-4fd8-a9f7-a437ce1211a0");
    private static final UUID OTHER =
            UUID.fromString("30c88a44-b992-4a3a-9f5b-6b64f176f501");
    private static final ChunkKey FIRST =
            new ChunkKey("minecraft:overworld", 10, 10);
    private static final ChunkKey SECOND =
            new ChunkKey("minecraft:overworld", 20, 20);
    private static final ChunkKey THIRD =
            new ChunkKey("minecraft:overworld", 30, 30);
    private static final ChunkKey CORRUPTED =
            new ChunkKey("minecraft:overworld", 20, 21);

    @TempDir
    Path temporary;

    @Test
    void acknowledgedClaimAndReleaseRecoverWithoutPeriodicSnapshot() {
        MemoryStore snapshot = new MemoryStore();
        Path journalFile = temporary.resolve("mutations.jsonl");
        ChunkRegistry first = new ChunkRegistry(
                snapshot, null, new RegistryMutationJournal(journalFile));
        assertTrue(first.tryClaim(FIRST, GROUP).granted());
        assertTrue(first.tryClaim(SECOND, GROUP).granted());
        assertTrue(first.release(FIRST, GROUP));

        ChunkRegistry recovered = new ChunkRegistry(
                snapshot, null, new RegistryMutationJournal(journalFile));
        recovered.load();

        assertEquals(null, recovered.ownerOf(FIRST));
        assertEquals(GROUP, recovered.ownerOf(SECOND));
    }

    @Test
    void journalReplaysReplaceAndMigrationInOrder() {
        RegistryMutationJournal journal = new RegistryMutationJournal(
                temporary.resolve("mutations.jsonl"));
        journal.appendPut(FIRST, GROUP);
        journal.appendReplaceGroup(GROUP, Set.of(FIRST, SECOND));
        journal.appendMigrateKeys(GROUP, OTHER, Set.of(SECOND));

        Map<ChunkKey, UUID> replayed = journal.replay(Map.of());

        assertEquals(GROUP, replayed.get(FIRST));
        assertEquals(OTHER, replayed.get(SECOND));
    }

    @Test
    void successfulCheckpointClearsJournal() {
        RegistryMutationJournal journal = new RegistryMutationJournal(
                temporary.resolve("mutations.jsonl"));
        journal.appendPut(FIRST, GROUP);
        assertTrue(journal.hasEntries());

        journal.clear();

        assertFalse(journal.hasEntries());
        assertTrue(journal.replay(Map.of()).isEmpty());
    }

    @Test
    void registryCheckpointMovesJournalStateIntoSnapshot() {
        MemoryStore snapshot = new MemoryStore();
        RegistryMutationJournal journal = new RegistryMutationJournal(
                temporary.resolve("mutations.jsonl"));
        ChunkRegistry registry = new ChunkRegistry(snapshot, null, journal);
        assertTrue(registry.tryClaim(FIRST, GROUP).granted());
        assertTrue(journal.hasEntries());

        registry.persist();

        assertFalse(journal.hasEntries());
        assertEquals(Map.of(FIRST, GROUP), snapshot.data);
    }

    @Test
    void incompleteTailIsTruncatedBeforeLaterAppend() throws Exception {
        Path journalFile = temporary.resolve("mutations.jsonl");
        RegistryMutationJournal journal = new RegistryMutationJournal(journalFile);
        journal.appendPut(FIRST, GROUP);
        long completeLength = Files.size(journalFile);
        Files.writeString(journalFile, "{\"version\":1",
                StandardCharsets.UTF_8, StandardOpenOption.APPEND);

        Map<ChunkKey, UUID> recovered =
                new RegistryMutationJournal(journalFile).replay(Map.of());

        assertEquals(Map.of(FIRST, GROUP), recovered);
        assertEquals(completeLength, Files.size(journalFile));

        RegistryMutationJournal reopened = new RegistryMutationJournal(journalFile);
        reopened.appendPut(SECOND, OTHER);
        assertEquals(Map.of(FIRST, GROUP, SECOND, OTHER),
                reopened.replay(Map.of()));
    }

    @Test
    void checksumMismatchInLastRecordIsTruncated() throws Exception {
        Path journalFile = temporary.resolve("mutations.jsonl");
        RegistryMutationJournal journal = new RegistryMutationJournal(journalFile);
        journal.appendPut(FIRST, GROUP);
        long firstRecordLength = Files.size(journalFile);
        journal.appendPut(SECOND, OTHER);
        corruptChunk(journalFile, SECOND, CORRUPTED);

        Map<ChunkKey, UUID> recovered =
                new RegistryMutationJournal(journalFile).replay(Map.of());

        assertEquals(Map.of(FIRST, GROUP), recovered);
        assertEquals(firstRecordLength, Files.size(journalFile));
    }

    @Test
    void checksumMismatchBeforeLaterRecordStopsStartup() throws Exception {
        Path journalFile = temporary.resolve("mutations.jsonl");
        RegistryMutationJournal journal = new RegistryMutationJournal(journalFile);
        journal.appendPut(FIRST, GROUP);
        journal.appendPut(SECOND, OTHER);
        journal.appendPut(THIRD, GROUP);
        corruptChunk(journalFile, SECOND, CORRUPTED);

        assertThrows(IllegalStateException.class,
                () -> new RegistryMutationJournal(journalFile).replay(Map.of()));
    }

    @Test
    void completeLastRecordWithUnsupportedVersionStopsStartup() throws Exception {
        Path journalFile = temporary.resolve("mutations.jsonl");
        RegistryMutationJournal journal = new RegistryMutationJournal(journalFile);
        journal.appendPut(FIRST, GROUP);
        JsonObject frame = readSingleFrame(journalFile);
        frame.addProperty("version", 2);
        writeSingleFrame(journalFile, frame);

        assertThrows(IllegalStateException.class,
                () -> new RegistryMutationJournal(journalFile).replay(Map.of()));
    }

    @Test
    void completeChecksummedLastRecordWithUnknownOperationStopsStartup()
            throws Exception {
        Path journalFile = temporary.resolve("mutations.jsonl");
        RegistryMutationJournal journal = new RegistryMutationJournal(journalFile);
        journal.appendPut(FIRST, GROUP);
        JsonObject frame = readSingleFrame(journalFile);
        JsonObject operation = frame.getAsJsonObject("operation");
        operation.addProperty("type", "future_operation");
        refreshIntegrity(frame, operation);
        writeSingleFrame(journalFile, frame);

        assertThrows(IllegalStateException.class,
                () -> new RegistryMutationJournal(journalFile).replay(Map.of()));
    }

    @Test
    void recordLargerThanConfiguredLimitStopsRecovery() throws Exception {
        Path journalFile = temporary.resolve("mutations.jsonl");
        Files.writeString(journalFile, "x".repeat(65), StandardCharsets.UTF_8);
        RegistryMutationJournal journal =
                new RegistryMutationJournal(journalFile, 64);

        assertThrows(IllegalStateException.class, () -> journal.replay(Map.of()));
        assertEquals(65, Files.size(journalFile));
    }

    @Test
    void checkpointMarkerConvergesBeforeAndAfterSnapshotWrite() {
        Path journalFile = temporary.resolve("mutations.jsonl");
        RegistryMutationJournal journal = new RegistryMutationJournal(journalFile);
        Map<ChunkKey, UUID> oldSnapshot =
                Map.of(FIRST, GROUP, SECOND, OTHER);
        journal.appendRemoveGroup(GROUP);
        journal.appendMigrateGroup(OTHER, GROUP);
        Map<ChunkKey, UUID> checkpoint = Map.of(SECOND, GROUP);

        journal.appendCheckpoint(checkpoint);

        // 崩溃点一：checkpoint WAL 已 force，但旧快照尚未替换。
        assertEquals(checkpoint, journal.replay(oldSnapshot));
        // 崩溃点二：新快照已替换，但 WAL 还未来得及 clear。
        assertEquals(checkpoint, journal.replay(checkpoint));
    }

    private static void corruptChunk(Path journalFile, ChunkKey expected,
                                     ChunkKey replacement) throws Exception {
        String original = Files.readString(journalFile, StandardCharsets.UTF_8);
        String corrupted = original.replace(
                expected.asString(), replacement.asString());
        assertFalse(original.equals(corrupted));
        Files.writeString(journalFile, corrupted, StandardCharsets.UTF_8,
                StandardOpenOption.TRUNCATE_EXISTING);
    }

    private static JsonObject readSingleFrame(Path journalFile) throws Exception {
        return JsonUtil.parseObject(Files.readString(
                journalFile, StandardCharsets.UTF_8).trim());
    }

    private static void writeSingleFrame(Path journalFile, JsonObject frame)
            throws Exception {
        Files.writeString(journalFile, JsonUtil.GSON.toJson(frame) + "\n",
                StandardCharsets.UTF_8, StandardOpenOption.TRUNCATE_EXISTING);
    }

    private static void refreshIntegrity(JsonObject frame, JsonObject operation) {
        byte[] payload = JsonUtil.GSON.toJson(operation).getBytes(
                StandardCharsets.UTF_8);
        frame.addProperty("payloadBytes", payload.length);
        frame.addProperty("sha256", Sha256.hex(payload));
    }

    private static final class MemoryStore implements RegistryStore {
        private Map<ChunkKey, UUID> data = new HashMap<>();

        @Override
        public Map<ChunkKey, UUID> load() {
            return new HashMap<>(data);
        }

        @Override
        public void save(Map<ChunkKey, UUID> snapshot) {
            data = new HashMap<>(snapshot);
        }

        @Override
        public String describe() {
            return "memory";
        }
    }
}
