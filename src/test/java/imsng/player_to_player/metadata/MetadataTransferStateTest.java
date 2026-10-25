package imsng.player_to_player.metadata;

import net.minecraft.nbt.CompoundTag;
import org.junit.jupiter.api.Test;
import java.io.*;
import java.util.Set;
import static org.junit.jupiter.api.Assertions.*;

class MetadataTransferStateTest {
    @Test void casRefreshRetainsFrozenCandidateAndOnlyChangesOwnedFields() {
        CompoundTag before = new CompoundTag(); before.putInt("mod:owned", 1); before.putLong("DayTime", 10);
        CompoundTag live = before.copy(); live.putInt("mod:owned", 9);
        var frozen = new MetadataTransferState(new MetadataSnapshot("world", 4, 3, before), Set.of("mod:owned"), live);
        live.putInt("mod:owned", 100); // 捕获后原对象变化不能污染冻结候选。
        CompoundTag newer = before.copy(); newer.putLong("DayTime", 99);
        var patch = frozen.patchAgainst(new MetadataSnapshot("world", 8, 3, newer), "transfer").orElseThrow();
        assertEquals(8, patch.baseVersion());
        assertEquals(9, patch.set().getInt("mod:owned"));
        assertFalse(patch.set().contains("DayTime"));
    }

    @Test void deletionAndTermFenceSurviveRefresh() {
        CompoundTag previous = new CompoundTag(); previous.putString("mod:owned", "old");
        var frozen = new MetadataTransferState(new MetadataSnapshot("world", 1, 2, previous), Set.of("mod:owned"), new CompoundTag());
        var patch = frozen.patchAgainst(new MetadataSnapshot("world", 5, 2, previous), "transfer").orElseThrow();
        assertEquals(Set.of("mod:owned"), patch.removed());
        assertThrows(IllegalStateException.class,
                () -> frozen.patchAgainst(new MetadataSnapshot("world", 5, 3, previous), "transfer"));
    }

    @Test void equalCandidateDoesNotProduceAnArtificialWrite() {
        CompoundTag state = new CompoundTag(); state.putInt("mod:owned", 3);
        var snapshot = new MetadataSnapshot("world", 7, 2, state);
        assertTrue(new MetadataTransferState(snapshot, Set.of("mod:owned"), state).patchAgainst(snapshot, "unused").isEmpty());
    }

    @Test void metadataFrameConsumesOnlyItsBytesAndRejectsWrongAck() throws Exception {
        var fence = new MetadataTransferFence("world", 3, 45);
        var bytes = new ByteArrayOutputStream(); var output = new DataOutputStream(bytes);
        fence.write(output, false); output.writeInt(0x12345678);
        var input = new DataInputStream(new ByteArrayInputStream(bytes.toByteArray()));
        assertEquals(fence, MetadataTransferFence.read(input, false));
        assertEquals(0x12345678, input.readInt());
        assertThrows(IOException.class, () -> MetadataTransferFence.read(
                new DataInputStream(new ByteArrayInputStream(bytes.toByteArray())), true));
        assertTrue(fence.coveredBy(new MetadataSnapshot("world", 46, 3, new CompoundTag())));
        assertFalse(fence.coveredBy(new MetadataSnapshot("world", 44, 3, new CompoundTag())));
        assertFalse(fence.coveredBy(new MetadataSnapshot("world", 46, 4, new CompoundTag())));
    }
}
