package imsng.player_to_player.group;

import imsng.player_to_player.registry.ChunkKey;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ChunkDeltaBatchTest {

    private static final UUID TRANSACTION =
            UUID.fromString("ef8f9c75-429d-4b3f-8dca-3148a41192a8");
    private static final ChunkKey FIRST =
            new ChunkKey("minecraft:overworld", 1, 2);
    private static final ChunkKey SECOND =
            new ChunkKey("minecraft:the_nether", 3, 4);

    @Test
    void coalescesLatestVersionAndSplitsByPayloadLimit() {
        ChunkDelta old = ChunkDelta.of(FIRST, 1, new byte[]{1, 2});
        ChunkDelta latest = ChunkDelta.of(FIRST, 3, new byte[]{3, 4, 5});
        ChunkDelta other = ChunkDelta.of(SECOND, 2, new byte[]{6, 7, 8, 9});

        List<ChunkDeltaBatch> batches = ChunkDeltaBatch.split(
                TRANSACTION, 10, List.of(old, latest, other), 100);

        assertEquals(2, batches.size());
        assertEquals(List.of(latest), batches.get(0).deltas());
        assertEquals(List.of(other), batches.get(1).deltas());
        assertEquals(10, batches.get(0).epoch());
        assertEquals(11, batches.get(1).epoch());
    }

    @Test
    void binaryRoundTripPreservesBatchIdentityAndPayload() {
        ChunkDelta delta = ChunkDelta.of(FIRST, 7, new byte[]{9, 8, 7, 6});
        ChunkDeltaBatch original = new ChunkDeltaBatch(
                TRANSACTION, 42, 5, List.of(delta));

        ChunkDeltaBatch decoded = ChunkDeltaBatch.decode(original.encode());

        assertEquals(original.batchId(), decoded.batchId());
        assertEquals(original.epoch(), decoded.epoch());
        assertEquals(original.sequence(), decoded.sequence());
        assertEquals(original.deltas().get(0).key(), decoded.deltas().get(0).key());
        assertEquals(original.deltas().get(0).version(), decoded.deltas().get(0).version());
        assertEquals(original.deltas().get(0).sha256(), decoded.deltas().get(0).sha256());
        assertArrayEquals(new byte[]{9, 8, 7, 6}, decoded.deltas().get(0).payload());
    }

    @Test
    void rejectsTamperedPayloadAndDuplicateKeys() {
        ChunkDelta delta = ChunkDelta.of(FIRST, 1, new byte[]{1});
        assertThrows(IllegalArgumentException.class,
                () -> new ChunkDelta(delta.key(), delta.version(),
                        new byte[]{2}, delta.sha256()));
        assertThrows(IllegalArgumentException.class,
                () -> new ChunkDeltaBatch(TRANSACTION, 1, 0,
                        List.of(delta, delta)));
    }
}
