package imsng.player_to_player.group;

import net.minecraft.nbt.CompoundTag;
import net.minecraft.nbt.DoubleTag;
import net.minecraft.nbt.ListTag;
import net.minecraft.nbt.NbtIo;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class PlayerStateNbtTest {

    @Test
    void decodesValidPhysicalServerPlayerState() throws Exception {
        CompoundTag decoded = PlayerStateNbt.decodeValidated(
                gzip(validPlayer(-92.0, 119.0, 8.0)));

        assertEquals("minecraft:overworld", decoded.getString("Dimension"));
        assertEquals(-92.0, decoded.getList("Pos", 6).getDouble(0), 0.0);
    }

    @Test
    void rejectsEmptyPayload() {
        assertThrows(IOException.class,
                () -> PlayerStateNbt.decodeValidated(new byte[0]));
    }

    @Test
    void rejectsMissingDimension() throws Exception {
        CompoundTag tag = validPlayer(1.0, 64.0, 1.0);
        tag.remove("Dimension");

        assertThrows(IOException.class,
                () -> PlayerStateNbt.decodeValidated(gzip(tag)));
    }

    @Test
    void rejectsNonStringDimension() throws Exception {
        CompoundTag tag = validPlayer(1.0, 64.0, 1.0);
        tag.putInt("Dimension", 0);

        assertThrows(IOException.class,
                () -> PlayerStateNbt.decodeValidated(gzip(tag)));
    }

    @Test
    void rejectsShortPositionList() throws Exception {
        CompoundTag tag = validPlayer(1.0, 64.0, 1.0);
        ListTag pos = new ListTag();
        pos.add(DoubleTag.valueOf(1.0));
        pos.add(DoubleTag.valueOf(64.0));
        tag.put("Pos", pos);

        assertThrows(IOException.class,
                () -> PlayerStateNbt.decodeValidated(gzip(tag)));
    }

    @Test
    void rejectsNonFinitePosition() throws Exception {
        CompoundTag tag = validPlayer(Double.NaN, 64.0, 1.0);

        assertThrows(IOException.class,
                () -> PlayerStateNbt.decodeValidated(gzip(tag)));
    }

    @Test
    void rejectsPayloadAboveDecompressedLimit() throws Exception {
        CompoundTag tag = validPlayer(1.0, 64.0, 1.0);
        tag.putByteArray("Padding",
                new byte[(int) PlayerStateNbt.MAX_PLAYER_NBT_BYTES + 1]);

        assertThrows(IOException.class,
                () -> PlayerStateNbt.decodeValidated(gzip(tag)));
    }

    private static CompoundTag validPlayer(double x, double y, double z) {
        CompoundTag tag = new CompoundTag();
        tag.putString("Dimension", "minecraft:overworld");
        ListTag pos = new ListTag();
        pos.add(DoubleTag.valueOf(x));
        pos.add(DoubleTag.valueOf(y));
        pos.add(DoubleTag.valueOf(z));
        tag.put("Pos", pos);
        return tag;
    }

    private static byte[] gzip(CompoundTag tag) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        NbtIo.writeCompressed(tag, out);
        return out.toByteArray();
    }
}
