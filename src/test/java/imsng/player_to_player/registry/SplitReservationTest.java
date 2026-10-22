package imsng.player_to_player.registry;

import org.junit.jupiter.api.Test;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import static org.junit.jupiter.api.Assertions.*;

class SplitReservationTest {
    private final UUID source = UUID.randomUUID();
    private final UUID other = UUID.randomUUID();
    private final UUID split = UUID.randomUUID();
    private final ChunkRegistry registry = new ChunkRegistry(null, null);
    private final ChunkKey center = new ChunkKey("minecraft:overworld", 0, 0);
    private final SplitRegion region = new SplitRegion(center.dimension(), -2, 2, -2, 2);

    @Test
    void reservationBlocksNewClaimsButKeepsOldWriterUntilRelease() {
        assertTrue(registry.tryClaim(center, source).granted());
        assertTrue(registry.reserveSplit(split, source, List.of(region)));
        assertEquals(source, registry.ownerOf(center));
        assertTrue(registry.tryClaim(center, source).granted(),
                "授予ACK丢失时源必须还能确认旧claim，然后执行耐久释放");
        assertFalse(registry.tryClaim(new ChunkKey(center.dimension(), 1, 0), source).granted());
        assertFalse(registry.tryClaim(new ChunkKey(center.dimension(), 3, 0), other).granted());
        assertTrue(registry.tryClaim(new ChunkKey(center.dimension(), 100, 0), source).granted());
        assertTrue(registry.release(center, source));
        assertFalse(registry.tryClaim(center, other).granted(), "逐块释放期间仍保护接管区域");
    }

    @Test
    void commitRequiresEmptyRegionAndChangesGroupsBeforeRemovingReservation() {
        registry.tryClaim(center, source);
        registry.reserveSplit(split, source, List.of(region));
        AtomicBoolean committed = new AtomicBoolean();
        assertFalse(registry.finishSplit(split, source, () -> committed.set(true)));
        assertFalse(committed.get());
        registry.release(center, source);
        assertTrue(registry.finishSplit(split, source, () -> committed.set(true)));
        assertTrue(committed.get());
        assertTrue(registry.tryClaim(center, other).granted());
        assertFalse(registry.finishSplit(split, source, () -> fail("不可二次提交")));
    }

    @Test
    void cancellationAndConflictingReservationsAreScopedToTheirOwner() {
        assertTrue(registry.reserveSplit(split, source, List.of(region)));
        assertFalse(registry.reserveSplit(UUID.randomUUID(), other, List.of(region)));
        assertFalse(registry.cancelSplit(split, other));
        assertFalse(registry.tryClaim(center, other).granted());
        assertTrue(registry.cancelSplit(split, source));
        assertTrue(registry.tryClaim(center, other).granted());
    }

    @Test
    void failedGroupCommitKeepsReservation() {
        registry.reserveSplit(split, source, List.of(region));
        assertThrows(IllegalStateException.class, () -> registry.finishSplit(split, source,
                () -> { throw new IllegalStateException("成员变化"); }));
        assertFalse(registry.tryClaim(center, other).granted());
    }
}
