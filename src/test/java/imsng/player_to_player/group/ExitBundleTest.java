package imsng.player_to_player.group;

import imsng.player_to_player.registry.ChunkKey;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ExitBundleTest {

    private static final UUID ID =
            UUID.fromString("9207ce8d-2b04-4fd8-a9f7-a437ce1211a0");
    private static final ChunkKey WRITTEN =
            new ChunkKey("minecraft:overworld", 1, 2);
    private static final ChunkKey PRESERVED =
            new ChunkKey("minecraft:the_nether", 3, 4);

    @Test
    void allowsCleanManifestChunksWithoutExplicitPayload() {
        ExitManifest manifest = new ExitManifest(UUID.randomUUID(), ID, ID,
                Set.of(ID), Set.of(WRITTEN, PRESERVED));

        ExitBundle bundle = new ExitBundle(manifest,
                Map.of(ID, new byte[]{1, 2, 3}),
                Map.of(WRITTEN, new byte[]{4, 5}));

        assertEquals(manifest, bundle.manifest());
        assertArrayEquals(new byte[]{1, 2, 3}, bundle.playerData(ID));
        assertArrayEquals(new byte[]{4, 5}, bundle.chunkData(WRITTEN));
        assertEquals(Set.of(WRITTEN), bundle.writtenChunks());

        assertThrows(IllegalArgumentException.class, () -> new ExitBundle(manifest,
                Map.of(ID, new byte[]{1}),
                Map.of(new ChunkKey("minecraft:overworld", 99, 99), new byte[]{2})));
    }

    @Test
    void binaryPayloadsAreDefensivelyCopied() {
        byte[] player = {1, 2};
        byte[] chunk = {3, 4};
        ExitManifest manifest = new ExitManifest(UUID.randomUUID(), ID, ID,
                Set.of(ID), Set.of(WRITTEN));
        ExitBundle bundle = new ExitBundle(manifest,
                Map.of(ID, player), Map.of(WRITTEN, chunk));

        player[0] = 9;
        chunk[0] = 9;
        byte[] returned = bundle.chunkData(WRITTEN);
        returned[0] = 8;

        assertArrayEquals(new byte[]{1, 2}, bundle.playerData(ID));
        assertArrayEquals(new byte[]{3, 4}, bundle.chunkData(WRITTEN));
        assertEquals(4L, bundle.compressedBytes());
    }
}
