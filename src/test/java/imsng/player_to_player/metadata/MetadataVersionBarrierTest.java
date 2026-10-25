package imsng.player_to_player.metadata;

import net.minecraft.nbt.CompoundTag;
import org.junit.jupiter.api.Test;
import java.util.Set;
import static org.junit.jupiter.api.Assertions.*;

class MetadataVersionBarrierTest {
    private static MetadataSnapshot state(long version, long term) {
        return new MetadataSnapshot("world", version, term, new CompoundTag());
    }
    private static MetadataPatch patch(long base, long term) {
        return new MetadataPatch("world", base, base + 1, term, "op-" + base, new CompoundTag(), Set.of());
    }
    @Test void laterGapCannotBeForgottenWhenEarlierSnapshotAndAckArrive() {
        var barrier = new MetadataVersionBarrier(state(1, 1));
        // 在请求v1快照期间收到v4补丁，随后请求响应v2和ACK都成功仍然不够。
        barrier.observe(patch(3, 1));
        assertFalse(barrier.allows(state(2, 1), state(2, 1)));
        assertFalse(barrier.allows(state(3, 1), state(3, 1)));
        assertTrue(barrier.allows(state(4, 1), state(4, 1)));
    }
    @Test void oldTermAckCannotReleaseNewTermAndOldTermPatchCannotRaiseBarrier() {
        var barrier = new MetadataVersionBarrier(state(10, 2));
        barrier.observe(patch(300, 1));
        assertFalse(barrier.allows(state(10, 2), state(10, 1)));
        assertTrue(barrier.allows(state(10, 2), state(10, 2)));
        barrier.observe(patch(11, 3));
        assertFalse(barrier.allows(state(100, 2), state(100, 2)));
        assertTrue(barrier.allows(state(12, 3), state(12, 3)));
    }
    @Test void continuousPatchWhileAckIsInFlightDoesNotRequireReapplyingOldSnapshot() {
        var barrier = new MetadataVersionBarrier(state(2, 1));
        barrier.observe(patch(2, 1));
        assertTrue(barrier.allows(state(3, 1), state(2, 1)));
        assertFalse(barrier.allows(state(2, 1), state(3, 1)));
        assertFalse(barrier.allows(state(3, 1), new MetadataSnapshot("other", 3, 1, new CompoundTag())));
    }
}
