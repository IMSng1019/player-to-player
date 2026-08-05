package imsng.player_to_player.server;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import imsng.player_to_player.group.ExitManifest;
import imsng.player_to_player.registry.ChunkKey;
import imsng.player_to_player.util.JsonUtil;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ExitTransactionStoreTest {

    private static final UUID SHUTDOWN_ID =
            UUID.fromString("c768829c-9160-43c7-a4fd-ecc2290936f4");
    private static final UUID CLIENT_ID =
            UUID.fromString("9207ce8d-2b04-4fd8-a9f7-a437ce1211a0");
    private static final ChunkKey OVERWORLD =
            new ChunkKey("minecraft:overworld", -7, 0);
    private static final ChunkKey NETHER =
            new ChunkKey("minecraft:the_nether", 3, 9);

    @TempDir
    Path tempDir;

    @Test
    void pendingAndCommittedMetadataRoundTrip() {
        ExitTransactionStore store = new ExitTransactionStore(tempDir);
        ExitManifest manifest = new ExitManifest(SHUTDOWN_ID, CLIENT_ID, CLIENT_ID,
                Set.of(CLIENT_ID), Set.of(OVERWORLD, NETHER));

        store.save(new ExitTransactionRecord(manifest, false));

        Map<UUID, ExitTransactionRecord> pending =
                new ExitTransactionStore(tempDir).load();
        assertEquals(manifest, pending.get(SHUTDOWN_ID).manifest());
        assertFalse(pending.get(SHUTDOWN_ID).committed());

        store.save(new ExitTransactionRecord(
                manifest, true, Set.of(OVERWORLD)));

        ExitTransactionRecord committed =
                new ExitTransactionStore(tempDir).load().get(SHUTDOWN_ID);
        assertTrue(committed.committed());
        assertEquals(Set.of(OVERWORLD, NETHER), committed.manifest().chunks());
        assertEquals(Set.of(OVERWORLD), committed.writtenChunks());
        assertTrue(committed.writtenChunksDeclared());
        assertFalse(committed.legacyDisposition());
    }

    @Test
    void eachStateTransitionUsesOneWalForceAndLatestRecordWins() {
        AtomicInteger forces = new AtomicInteger();
        ExitTransactionStore store = new ExitTransactionStore(
                tempDir, elapsedNanos -> forces.incrementAndGet());
        ExitManifest manifest = new ExitManifest(SHUTDOWN_ID, CLIENT_ID, CLIENT_ID,
                Set.of(CLIENT_ID), Set.of(OVERWORLD));

        store.save(new ExitTransactionRecord(manifest, false));
        store.save(new ExitTransactionRecord(
                manifest, true, Set.of(OVERWORLD)));

        assertEquals(2, forces.get());
        ExitTransactionRecord recovered =
                new ExitTransactionStore(tempDir).load().get(SHUTDOWN_ID);
        assertTrue(recovered.committed());
        assertEquals(Set.of(OVERWORLD), recovered.writtenChunks());
    }

    @Test
    void legacyCommittedRecordWithoutDataAppliedIsAlreadyApplied()
            throws Exception {
        JsonObject legacy = new JsonObject();
        legacy.addProperty("shutdownId", SHUTDOWN_ID.toString());
        legacy.addProperty("clientId", CLIENT_ID.toString());
        legacy.addProperty("groupId", CLIENT_ID.toString());
        legacy.addProperty("committed", true);
        legacy.addProperty("cleanupComplete", false);
        legacy.addProperty("abandoned", false);
        JsonArray players = new JsonArray();
        players.add(CLIENT_ID.toString());
        legacy.add("players", players);
        JsonArray chunks = new JsonArray();
        chunks.add(OVERWORLD.asString());
        legacy.add("chunks", chunks);
        Files.writeString(tempDir.resolve(SHUTDOWN_ID + ".json"),
                JsonUtil.GSON.toJson(legacy), StandardCharsets.UTF_8);

        ExitTransactionRecord recovered =
                new ExitTransactionStore(tempDir).load().get(SHUTDOWN_ID);

        assertTrue(recovered.committed());
        assertTrue(recovered.dataApplied());
        assertFalse(recovered.cleanupComplete());
        assertTrue(recovered.legacyDisposition());
        assertTrue(recovered.writtenChunks().isEmpty());
    }
}
