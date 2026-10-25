package imsng.player_to_player.metadata;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class MetadataTransferBarrierTest {
    @Test void oldTermAndIncompleteVersionCannotCommitOwnership() {
        assertFalse(MetadataTransferBarrier.accepts(20, 3, 19, 3, 3));
        assertFalse(MetadataTransferBarrier.accepts(20, 3, 25, 2, 3));
        assertFalse(MetadataTransferBarrier.accepts(20, 3, 25, 3, 4));
        assertTrue(MetadataTransferBarrier.accepts(20, 3, 20, 3, 3));
        assertTrue(MetadataTransferBarrier.accepts(20, 3, 25, 3, 3));
    }
}
