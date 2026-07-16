package imsng.player_to_player.group;

import net.minecraft.nbt.CompoundTag;
import net.minecraft.nbt.ListTag;
import net.minecraft.nbt.NbtAccounter;
import net.minecraft.nbt.NbtIo;
import net.minecraft.nbt.Tag;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * 玩家状态 NBT 的统一编解码与边界校验。
 * <p>
 * 主客户端切换前收到的玩家数据来自物理服务器，但仍必须在写入本地存档前验证：
 * 无效维度或坐标会让原版集成服务器以默认位置创建玩家，正是本次实测中
 * 玩家落到 {@code (0, 0, 0)} 并黑屏的直接原因。
 */
public final class PlayerStateNbt {

    /** 玩家 NBT 解压上限；正常玩家数据通常只有数 KB。 */
    public static final long MAX_PLAYER_NBT_BYTES = 16L * 1024 * 1024;

    private PlayerStateNbt() {
    }

    /** 解压并验证物理服务器下发的玩家状态。 */
    public static CompoundTag decodeValidated(byte[] gzipNbt) throws IOException {
        if (gzipNbt == null || gzipNbt.length == 0) {
            throw new IOException("玩家 NBT 为空");
        }

        CompoundTag tag;
        try (ByteArrayInputStream input = new ByteArrayInputStream(gzipNbt)) {
            tag = NbtIo.readCompressed(input,
                    NbtAccounter.create(MAX_PLAYER_NBT_BYTES));
        } catch (IOException | RuntimeException e) {
            throw new IOException("玩家 NBT 解压或解析失败", e);
        }
        validate(tag);
        return tag;
    }

    /** 按原版玩家文件格式编码为 gzip NBT。 */
    public static byte[] encode(CompoundTag tag) throws IOException {
        if (tag == null) {
            throw new IOException("玩家 NBT 不能为空");
        }
        ByteArrayOutputStream output = new ByteArrayOutputStream(16 * 1024);
        NbtIo.writeCompressed(tag, output);
        return output.toByteArray();
    }

    private static void validate(CompoundTag tag) throws IOException {
        if (tag == null) {
            throw new IOException("玩家 NBT 根标签缺失");
        }
        if (!tag.contains("Dimension", Tag.TAG_STRING)
                || tag.getString("Dimension").isBlank()) {
            throw new IOException("玩家 NBT 的 Dimension 不是有效字符串");
        }

        Tag rawPos = tag.get("Pos");
        if (!(rawPos instanceof ListTag pos)
                || pos.getElementType() != Tag.TAG_DOUBLE
                || pos.size() != 3) {
            throw new IOException("玩家 NBT 的 Pos 必须包含三个 double 坐标");
        }
        for (int i = 0; i < 3; i++) {
            if (!Double.isFinite(pos.getDouble(i))) {
                throw new IOException("玩家 NBT 的 Pos 包含非有限坐标");
            }
        }
    }
}
