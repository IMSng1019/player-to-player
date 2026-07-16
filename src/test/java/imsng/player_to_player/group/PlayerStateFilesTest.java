package imsng.player_to_player.group;

import net.minecraft.nbt.CompoundTag;
import net.minecraft.nbt.DoubleTag;
import net.minecraft.nbt.ListTag;
import net.minecraft.nbt.NbtAccounter;
import net.minecraft.nbt.NbtIo;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

class PlayerStateFilesTest {

    private static final UUID PLAYER_ID =
            UUID.fromString("9207ce8d-2b04-4fd8-a9f7-a437ce1211a0");

    @TempDir
    Path tempDir;

    @Test
    void authoritativeStateReplacesPoisonedPlayerFileAndRemovesEmbeddedPlayer()
            throws Exception {
        Path save = tempDir.resolve("world");
        Path playerDir = save.resolve("playerdata");
        Files.createDirectories(playerDir);
        writeLevel(save.resolve("level.dat"), playerAt(0.0, 0.0, 0.0), 1234L);
        writeLevel(save.resolve("level.dat_old"), new CompoundTag(), 1234L);
        NbtIo.writeCompressed(playerAt(0.0, 0.0, 0.0),
                playerDir.resolve(PLAYER_ID + ".dat"));

        PlayerStateFiles.installAuthoritative(save, PLAYER_ID,
                playerAt(-107.0, 116.0, 10.0));

        CompoundTag installed = read(playerDir.resolve(PLAYER_ID + ".dat"));
        assertEquals(-107.0, installed.getList("Pos", 6).getDouble(0), 0.0);
        CompoundTag level = read(save.resolve("level.dat"));
        CompoundTag levelOld = read(save.resolve("level.dat_old"));
        assertFalse(level.getCompound("Data").contains("Player"));
        assertFalse(levelOld.getCompound("Data").contains("Player"));
        assertEquals(1234L, level.getCompound("Data").getLong("DayTime"));
    }

    @Test
    void missingLevelDatDoesNotReplaceExistingPlayerFile() throws Exception {
        Path save = tempDir.resolve("world");
        Path playerDir = save.resolve("playerdata");
        Files.createDirectories(playerDir);
        Path playerFile = playerDir.resolve(PLAYER_ID + ".dat");
        NbtIo.writeCompressed(playerAt(5.0, 70.0, 5.0), playerFile);

        assertThrows(IOException.class, () -> PlayerStateFiles.installAuthoritative(
                save, PLAYER_ID, playerAt(-107.0, 116.0, 10.0)));

        assertEquals(5.0, read(playerFile).getList("Pos", 6).getDouble(0), 0.0);
    }

    private static CompoundTag playerAt(double x, double y, double z) {
        CompoundTag tag = new CompoundTag();
        tag.putString("Dimension", "minecraft:overworld");
        ListTag pos = new ListTag();
        pos.add(DoubleTag.valueOf(x));
        pos.add(DoubleTag.valueOf(y));
        pos.add(DoubleTag.valueOf(z));
        tag.put("Pos", pos);
        return tag;
    }

    private static void writeLevel(Path file, CompoundTag player, long dayTime)
            throws IOException {
        CompoundTag data = new CompoundTag();
        data.putLong("DayTime", dayTime);
        data.put("Player", player);
        CompoundTag root = new CompoundTag();
        root.put("Data", data);
        NbtIo.writeCompressed(root, file);
    }

    private static CompoundTag read(Path file) throws IOException {
        return NbtIo.readCompressed(file, NbtAccounter.unlimitedHeap());
    }
}
