package imsng.player_to_player.metadata;

import net.minecraft.nbt.CompoundTag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class MetadataWireTest {
    @Test
    void roundtripsTypesSnapshotsAndDeletionPatches() throws Exception {
        CompoundTag data = new CompoundTag();
        data.putLong("DayTime", Long.MAX_VALUE);
        data.putFloat("SpawnAngle", 1.5F);
        data.putIntArray("ModArray", new int[]{1, 2, 3});
        MetadataSnapshot snapshot = new MetadataSnapshot("world", 3, 2, data);
        assertEquals(snapshot, MetadataWire.snapshotFromTag(MetadataWire.decode(
                MetadataWire.encode(MetadataWire.snapshotToTag(snapshot)))));
        MetadataPatch patch = new MetadataPatch("world", 2, 3, 2, "op", data, Set.of("CustomBossEvents"));
        assertEquals(patch, MetadataWire.patchFromTag(MetadataWire.decode(
                MetadataWire.encode(MetadataWire.patchToTag(patch)))));
    }

    @Test
    void rejectsMissingHeaderAndOversizedDecodedNbt() throws Exception {
        assertThrows(IOException.class, () -> MetadataWire.snapshotFromTag(new CompoundTag()));
        assertThrows(IOException.class, () -> MetadataWire.patchFromTag(new CompoundTag()));
        assertThrows(IOException.class, () -> MetadataWire.decode(new byte[0]));
        CompoundTag bomb = new CompoundTag();
        bomb.putByteArray("bomb", new byte[(int) MetadataWire.MAX_DECOMPRESSED_BYTES + 1]);
        java.io.ByteArrayOutputStream bytes = new java.io.ByteArrayOutputStream();
        net.minecraft.nbt.NbtIo.writeCompressed(bomb, bytes);
        assertThrows(IOException.class, () -> MetadataWire.decode(bytes.toByteArray()));
    }
}
