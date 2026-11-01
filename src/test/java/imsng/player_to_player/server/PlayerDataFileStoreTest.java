package imsng.player_to_player.server;

import imsng.player_to_player.group.PlayerStateNbt;
import net.minecraft.nbt.CompoundTag;
import net.minecraft.nbt.DoubleTag;
import net.minecraft.nbt.ListTag;
import net.minecraft.nbt.NbtAccounter;
import net.minecraft.nbt.NbtIo;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.UUID;
import java.util.zip.Deflater;
import java.util.zip.GZIPOutputStream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
    void validatedUploadKeepsOriginalGzipBytesWithoutRecompression() throws Exception {
        CompoundTag state = playerAt(-107.0);
        byte[] upload = encodeWithoutCompression(state);
        // 同一份合法 NBT 使用另一种压缩级别，避免默认压缩恰好相同而漏掉重复压缩。
        assertEquals(state, PlayerStateNbt.decodeValidated(upload));
        assertFalse(Arrays.equals(PlayerStateNbt.encode(state), upload));

        assertTrue(PlayerDataFileStore.write(tempDir, PLAYER_ID, upload));

        Path file = tempDir.resolve(PLAYER_ID + ".dat");
        assertArrayEquals(upload, Files.readAllBytes(file));
        assertEquals(state, read(file));
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
    void validGzipWithInvalidPlayerStateDoesNotReplaceExistingFile() throws Exception {
        Path file = tempDir.resolve(PLAYER_ID + ".dat");
        byte[] previous = PlayerStateNbt.encode(playerAt(5.0));
        Files.write(file, previous);
        CompoundTag invalid = playerAt(6.0);
        invalid.remove("Dimension");

        // 跳过重复压缩后，仍必须校验解压后的玩家字段，不能只检查 gzip 容器。
        assertFalse(PlayerDataFileStore.write(
                tempDir, PLAYER_ID, encodeWithoutCompression(invalid)));

        assertArrayEquals(previous, Files.readAllBytes(file));
        assertFalse(Files.exists(tempDir.resolve(PLAYER_ID + ".dat.p2p-tmp")));
    }

    @Test
    void blockedRealtimeWriteDoesNotReplaceFinalState() throws Exception {
        Path file = tempDir.resolve(PLAYER_ID + ".dat");
        NbtIo.writeCompressed(playerAt(-107.0), file);

        assertFalse(PlayerDataFileStore.writeIfAllowed(
                tempDir, PLAYER_ID, PlayerStateNbt.encode(playerAt(0.0)),
                () -> false));

        assertEquals(-107.0, read(file).getList("Pos", 6).getDouble(0), 0.0);
        assertFalse(Files.exists(tempDir.resolve(PLAYER_ID + ".dat.p2p-tmp")));
    }

    /** 测试专用合法 gzip：保留 gzip 格式，只关闭 deflate 压缩以区分上传字节和重编码结果。 */
    private static byte[] encodeWithoutCompression(CompoundTag state) throws IOException {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        try (GZIPOutputStream gzip = new GZIPOutputStream(output) {
            { def.setLevel(Deflater.NO_COMPRESSION); }
        }; DataOutputStream nbt = new DataOutputStream(gzip)) {
            NbtIo.write(state, nbt);
        }
        return output.toByteArray();
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
