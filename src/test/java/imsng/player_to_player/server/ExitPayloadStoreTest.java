package imsng.player_to_player.server;

import imsng.player_to_player.group.ExitManifest;
import imsng.player_to_player.group.PlayerStateNbt;
import imsng.player_to_player.registry.ChunkKey;
import imsng.player_to_player.util.Sha256;
import net.minecraft.nbt.CompoundTag;
import net.minecraft.nbt.DoubleTag;
import net.minecraft.nbt.ListTag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ExitPayloadStoreTest {

    private static final UUID SHUTDOWN =
            UUID.fromString("c768829c-9160-43c7-a4fd-ecc2290936f4");
    private static final UUID CLIENT =
            UUID.fromString("9207ce8d-2b04-4fd8-a9f7-a437ce1211a0");
    private static final ChunkKey CHUNK =
            new ChunkKey("minecraft:overworld", -7, 0);
    private static final ChunkKey CLEAN =
            new ChunkKey("minecraft:overworld", -6, 0);

    @TempDir
    Path temporary;

    @Test
    void stagedPayloadsSurviveStoreRecreationAndApplyTogether() throws Exception {
        byte[] player = validPlayer();
        byte[] chunk = new byte[]{4, 5, 6};
        List<byte[]> writtenPlayers = new ArrayList<>();
        List<byte[]> writtenChunks = new ArrayList<>();
        AtomicBoolean flushed = new AtomicBoolean();
        ExitPayloadStore first = store(writtenPlayers, writtenChunks, flushed, false);

        assertTrue(first.stagePlayer(manifest(), CLIENT, player));
        assertTrue(first.stageChunk(manifest(), CHUNK, chunk, false));
        assertTrue(writtenPlayers.isEmpty());
        assertTrue(writtenChunks.isEmpty());

        ExitPayloadStore recovered = store(
                writtenPlayers, writtenChunks, flushed, false);
        recovered.apply(manifest(), explicit(Set.of(CHUNK)));

        assertArrayEquals(player, writtenPlayers.get(0));
        assertArrayEquals(chunk, writtenChunks.get(0));
        assertTrue(flushed.get());
        recovered.discard(manifest());
        assertFalse(Files.exists(temporary.resolve(SHUTDOWN.toString())));
    }

    @Test
    void discardingPartialPayloadNeverCallsOfficialWriters() throws Exception {
        List<byte[]> writtenPlayers = new ArrayList<>();
        List<byte[]> writtenChunks = new ArrayList<>();
        ExitPayloadStore store = store(
                writtenPlayers, writtenChunks, new AtomicBoolean(), false);

        assertTrue(store.stagePlayer(manifest(), CLIENT, validPlayer()));
        store.discard(manifest());

        assertTrue(writtenPlayers.isEmpty());
        assertTrue(writtenChunks.isEmpty());
        assertThrows(IllegalStateException.class,
                () -> store.apply(manifest(), explicit(Set.of(CHUNK))));
    }

    @Test
    void failedCommittedApplyCanReplayAllObjectsIdempotently() throws Exception {
        List<byte[]> writtenPlayers = new ArrayList<>();
        List<byte[]> writtenChunks = new ArrayList<>();
        AtomicBoolean failChunkOnce = new AtomicBoolean(true);
        ExitPayloadStore store = store(
                writtenPlayers, writtenChunks, new AtomicBoolean(), failChunkOnce);
        assertTrue(store.stagePlayer(manifest(), CLIENT, validPlayer()));
        assertTrue(store.stageChunk(manifest(), CHUNK, new byte[]{9}, false));

        assertThrows(IllegalStateException.class,
                () -> store.apply(manifest(), explicit(Set.of(CHUNK))));
        store.apply(manifest(), explicit(Set.of(CHUNK)));

        assertTrue(writtenPlayers.size() == 2);
        assertTrue(writtenChunks.size() == 1);
    }

    @Test
    void invalidPlayerPayloadIsRejectedBeforeDurableStage() {
        ExitPayloadStore store = store(
                new ArrayList<>(), new ArrayList<>(), new AtomicBoolean(), false);

        assertFalse(store.stagePlayer(manifest(), CLIENT, new byte[]{1, 2, 3}));
    }

    @Test
    void explicitPlanWritesOnlyDeclaredChunksAndFlushesOnce() throws Exception {
        List<ChunkKey> written = new ArrayList<>();
        AtomicInteger flushes = new AtomicInteger();
        ExitPayloadStore store = new ExitPayloadStore(temporary,
                (playerId, data) -> true,
                new ExitPayloadStore.ChunkAccess() {
                    @Override
                    public boolean validate(UUID groupId, ChunkKey key, byte[] data,
                                            boolean preserveExisting) {
                        return !preserveExisting && data != null;
                    }

                    @Override
                    public boolean write(UUID groupId, ChunkKey key, byte[] data,
                                         boolean preserveExisting) {
                        written.add(key);
                        return !preserveExisting && data != null;
                    }

                    @Override
                    public void flush() {
                        flushes.incrementAndGet();
                    }
                });
        ExitManifest manifest = manifest(Set.of(CHUNK, CLEAN));
        assertTrue(store.stagePlayer(manifest, CLIENT, validPlayer()));
        assertTrue(store.stageChunk(manifest, CHUNK, new byte[]{7}, false));

        store.apply(manifest, explicit(Set.of(CHUNK)));

        assertEquals(List.of(CHUNK), written);
        assertEquals(1, flushes.get());
    }

    @Test
    void explicitPlanRejectsMissingWrittenPayloadAndNewPreserveStage()
            throws Exception {
        ExitPayloadStore store = store(
                new ArrayList<>(), new ArrayList<>(), new AtomicBoolean(), false);
        assertTrue(store.stagePlayer(manifest(), CLIENT, validPlayer()));

        assertFalse(store.stageChunk(manifest(), CHUNK, null, true));
        assertThrows(IllegalStateException.class,
                () -> store.apply(manifest(), explicit(Set.of(CHUNK))));
    }

    @Test
    void legacyDispositionStillRestoresPreserveMarkers() throws Exception {
        List<byte[]> writtenChunks = new ArrayList<>();
        AtomicBoolean flushed = new AtomicBoolean();
        ExitPayloadStore store = store(
                new ArrayList<>(), writtenChunks, flushed, false);
        assertTrue(store.stagePlayer(manifest(), CLIENT, validPlayer()));
        Path marker = temporary.resolve(SHUTDOWN.toString())
                .resolve("preserve")
                .resolve(Sha256.hex(CHUNK.asString().getBytes(StandardCharsets.UTF_8))
                        + ".marker");
        Files.createDirectories(marker.getParent());
        Files.writeString(marker, "preserve\n", StandardCharsets.US_ASCII);

        store.apply(manifest(),
                new ExitTransactionLedger.ChunkDisposition(Set.of(), false));

        assertEquals(1, writtenChunks.size());
        assertNull(writtenChunks.get(0));
        assertTrue(flushed.get());
    }

    private ExitPayloadStore store(List<byte[]> players, List<byte[]> chunks,
                                   AtomicBoolean flushed, boolean failChunkOnce) {
        return store(players, chunks, flushed, new AtomicBoolean(failChunkOnce));
    }

    private ExitPayloadStore store(List<byte[]> players, List<byte[]> chunks,
                                   AtomicBoolean flushed,
                                   AtomicBoolean failChunkOnce) {
        return new ExitPayloadStore(temporary,
                (playerId, data) -> {
                    players.add(data.clone());
                    return true;
                },
                new ExitPayloadStore.ChunkAccess() {
                    @Override
                    public boolean validate(UUID groupId, ChunkKey key, byte[] data,
                                            boolean preserveExisting) {
                        return preserveExisting || data != null;
                    }

                    @Override
                    public boolean write(UUID groupId, ChunkKey key, byte[] data,
                                         boolean preserveExisting) {
                        if (failChunkOnce.getAndSet(false)) {
                            return false;
                        }
                        chunks.add(data != null ? data.clone() : null);
                        return true;
                    }

                    @Override
                    public void flush() {
                        flushed.set(true);
                    }
                });
    }

    private static ExitManifest manifest() {
        return manifest(Set.of(CHUNK));
    }

    private static ExitManifest manifest(Set<ChunkKey> chunks) {
        return new ExitManifest(SHUTDOWN, CLIENT, CLIENT,
                Set.of(CLIENT), chunks);
    }

    private static ExitTransactionLedger.ChunkDisposition explicit(
            Set<ChunkKey> writtenChunks) {
        return new ExitTransactionLedger.ChunkDisposition(writtenChunks, true);
    }

    private static byte[] validPlayer() throws Exception {
        CompoundTag tag = new CompoundTag();
        tag.putString("Dimension", "minecraft:overworld");
        ListTag pos = new ListTag();
        pos.add(DoubleTag.valueOf(1.0));
        pos.add(DoubleTag.valueOf(64.0));
        pos.add(DoubleTag.valueOf(2.0));
        tag.put("Pos", pos);
        return PlayerStateNbt.encode(tag);
    }
}
