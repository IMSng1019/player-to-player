package imsng.player_to_player.metadata;

import com.google.gson.JsonObject;
import imsng.player_to_player.netproto.ControlMessage;
import imsng.player_to_player.netproto.MessageType;
import net.minecraft.nbt.CompoundTag;
import net.minecraft.nbt.ListTag;
import org.junit.jupiter.api.Test;
import java.io.IOException;
import java.util.List;
import java.util.Set;
import static org.junit.jupiter.api.Assertions.*;

class MetadataCatchupTest {
    private static MetadataSnapshot initial() {
        CompoundTag data = new CompoundTag(); data.putLong("DayTime", 100); data.putString("old", "remove-me");
        return new MetadataSnapshot("world", 10, 2, data);
    }
    private static MetadataPatch patch(long base, long term, long time, Set<String> removed) {
        CompoundTag data = new CompoundTag(); data.putLong("DayTime", time);
        return new MetadataPatch("world", base, base + 1, term, "operation-" + base, data, removed);
    }
    private static ControlMessage response(long end, long term, List<MetadataPatch> patches) throws IOException {
        JsonObject header = new JsonObject(); header.addProperty("worldId", "world");
        header.addProperty("version", end); header.addProperty("term", term);
        ListTag list = new ListTag(); patches.forEach(patch -> list.add(MetadataWire.patchToTag(patch)));
        CompoundTag root = new CompoundTag(); root.put("Patches", list);
        return ControlMessage.of(MessageType.METADATA_CATCHUP, header, MetadataWire.encode(root));
    }

    @Test void fillsMissingVersionsInOrderAndPreservesDeletionAndTypedValues() throws Exception {
        var base = initial();
        var patches = MetadataCatchup.decode(base, response(12, 2,
                List.of(patch(10, 2, 200, Set.of("old")), patch(11, 2, 300, Set.of()))));
        assertEquals(List.of(11L, 12L), patches.stream().map(MetadataPatch::version).toList());
        var first = patches.get(0).applyTo(base);
        var last = patches.get(1).applyTo(first);
        assertEquals(200, first.data().getLong("DayTime"));
        assertEquals(300, last.data().getLong("DayTime"));
        assertFalse(last.data().contains("old"));
        assertEquals("remove-me", base.data().getString("old"));
        assertEquals(100, base.data().getLong("DayTime"));
    }

    @Test void emptyWindowAcknowledgesOnlyTheRequestedVersion() throws Exception {
        assertTrue(MetadataCatchup.decode(initial(), response(10, 2, List.of())).isEmpty());
        assertThrows(IOException.class, () -> MetadataCatchup.decode(initial(), response(11, 2, List.of())));
    }

    @Test void rejectsOutOfOrderMissingAndTruncatedResponsesBeforeApplyingAnyState() throws Exception {
        var first = patch(10, 2, 200, Set.of()); var second = patch(11, 2, 300, Set.of());
        assertThrows(IOException.class, () -> MetadataCatchup.decode(initial(), response(12, 2, List.of(second, first))));
        assertThrows(IOException.class, () -> MetadataCatchup.decode(initial(), response(12, 2, List.of(second))));
        assertThrows(IOException.class, () -> MetadataCatchup.decode(initial(), response(12, 2, List.of(first))));
    }

    @Test void oldTermOrMixedTermWindowCannotJoinCurrentHistory() throws Exception {
        assertThrows(IOException.class, () -> MetadataCatchup.decode(initial(), response(11, 1, List.of(patch(10, 1, 200, Set.of())))));
        assertThrows(IOException.class, () -> MetadataCatchup.decode(initial(), response(12, 2,
                List.of(patch(10, 2, 200, Set.of()), patch(11, 3, 300, Set.of())))));
    }
}
