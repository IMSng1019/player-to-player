package imsng.player_to_player.server;

import imsng.player_to_player.group.PlayerStateNbt;
import net.minecraft.nbt.CompoundTag;
import net.minecraft.nbt.DoubleTag;
import net.minecraft.nbt.ListTag;
import net.minecraft.nbt.NbtAccounter;
import net.minecraft.nbt.NbtIo;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class PlayerDataFileStoreTest {

    private static final UUID PLAYER_ID =
            UUID.fromString("9207ce8d-2b04-4fd8-a9f7-a437ce1211a0");

    @TempDir
    Path tempDir;

    @Test
    void validStateAtomicallyReplacesPlayerFile() throws Exception {
        Path file = tempDir.resolve(PLAYER_ID + ".dat");
        NbtIo.writeCompressed(playerAt(1.0), file);

        assertEquals(true, PlayerDataFileStore.write(
                tempDir, PLAYER_ID, PlayerStateNbt.encode(playerAt(-107.0))));

        assertEquals(-107.0, read(file).getList("Pos", 6).getDouble(0), 0.0);
        assertFalse(Files.exists(tempDir.resolve(PLAYER_ID + ".dat.p2p-tmp")));
    }

    @Test
    void invalidStateDoesNotReplaceExistingFile() throws Exception {
        Path file = tempDir.resolve(PLAYER_ID + ".dat");
        NbtIo.writeCompressed(playerAt(5.0), file);

        assertFalse(PlayerDataFileStore.write(tempDir, PLAYER_ID, new byte[]{1, 2, 3}));

        assertEquals(5.0, read(file).getList("Pos", 6).getDouble(0), 0.0);
    }

    @Test
    void blockedRealtimeWriteDoesNotReplaceFinalState() throws Exception {
        Path file = tempDir.resolve(PLAYER_ID + ".dat");
        NbtIo.writeCompressed(playerAt(-107.0), file);

        assertFalse(PlayerDataFileStore.writeIfAllowed(
                tempDir, PLAYER_ID, PlayerStateNbt.encode(playerAt(0.0)),
                () -> false));

        assertEquals(-107.0, read(file).getList("Pos", 6).getDouble(0), 0.0);
    }

    private static CompoundTag playerAt(double x) {
        CompoundTag tag = new CompoundTag();
        tag.putString("Dimension", "minecraft:overworld");
        ListTag pos = new ListTag();
        pos.add(DoubleTag.valueOf(x));
        pos.add(DoubleTag.valueOf(80.0));
        pos.add(DoubleTag.valueOf(10.0));
        tag.put("Pos", pos);
        return tag;
    }

    private static CompoundTag read(Path file) throws Exception {
        return NbtIo.readCompressed(file, NbtAccounter.unlimitedHeap());
    }
}
