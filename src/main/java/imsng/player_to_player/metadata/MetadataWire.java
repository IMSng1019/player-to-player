package imsng.player_to_player.metadata;

import net.minecraft.nbt.CompoundTag;
import net.minecraft.nbt.ListTag;
import net.minecraft.nbt.NbtAccounter;
import net.minecraft.nbt.NbtIo;
import net.minecraft.nbt.StringTag;
import net.minecraft.nbt.Tag;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * v9 元数据载荷：压缩 NBT 保留 long、浮点数、数组等原始类型。
 * 压缩帧和解压后对象均设置上限，避免将合法的小压缩包展开成无限内存分配。
 */
public final class MetadataWire {
    public static final int MAX_COMPRESSED_BYTES = 8 * 1024 * 1024;
    public static final long MAX_DECOMPRESSED_BYTES = 16L * 1024 * 1024;
    private static final int FORMAT = 1;

    private MetadataWire() { }

    public static byte[] encode(CompoundTag tag) throws IOException {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        NbtIo.writeCompressed(tag, output);
        byte[] bytes = output.toByteArray();
        if (bytes.length > MAX_COMPRESSED_BYTES) {
            throw new IOException("元数据压缩载荷超过上限");
        }
        // 发送端也验证接收端所使用的额度，避免持久化后才发现无法传输。
        decode(bytes);
        return bytes;
    }

    public static CompoundTag decode(byte[] bytes) throws IOException {
        if (bytes == null || bytes.length == 0 || bytes.length > MAX_COMPRESSED_BYTES) {
            throw new IOException("元数据压缩载荷为空或超过上限");
        }
        try {
            return NbtIo.readCompressed(new ByteArrayInputStream(bytes),
                    NbtAccounter.create(MAX_DECOMPRESSED_BYTES));
        } catch (RuntimeException exception) {
            throw new IOException("元数据 NBT 损坏或超过解压上限", exception);
        }
    }

    public static CompoundTag snapshotToTag(MetadataSnapshot snapshot) {
        CompoundTag tag = header(snapshot.worldId(), snapshot.version(), snapshot.term());
        tag.put("data", snapshot.data());
        return tag;
    }

    public static MetadataSnapshot snapshotFromTag(CompoundTag tag) throws IOException {
        validateHeader(tag);
        require(tag, "data", Tag.TAG_COMPOUND);
        try {
            return new MetadataSnapshot(tag.getString("worldId"), tag.getLong("version"),
                    tag.getLong("term"), tag.getCompound("data"));
        } catch (IllegalArgumentException exception) {
            throw new IOException("元数据快照字段非法", exception);
        }
    }

    public static CompoundTag patchToTag(MetadataPatch patch) {
        CompoundTag tag = header(patch.worldId(), patch.version(), patch.term());
        tag.putLong("baseVersion", patch.baseVersion());
        tag.putString("operationId", patch.operationId());
        tag.put("set", patch.set());
        ListTag removed = new ListTag();
        patch.removed().stream().sorted().forEach(field -> removed.add(StringTag.valueOf(field)));
        tag.put("removed", removed);
        return tag;
    }

    public static MetadataPatch patchFromTag(CompoundTag tag) throws IOException {
        validateHeader(tag);
        require(tag, "baseVersion", Tag.TAG_LONG);
        require(tag, "operationId", Tag.TAG_STRING);
        require(tag, "set", Tag.TAG_COMPOUND);
        require(tag, "removed", Tag.TAG_LIST);
        ListTag list = (ListTag) tag.get("removed");
        if (!list.isEmpty() && list.getElementType() != Tag.TAG_STRING) {
            throw new IOException("元数据删除列表必须为字符串");
        }
        Set<String> removed = new HashSet<>();
        for (int index = 0; index < list.size(); index++) {
            if (!removed.add(list.getString(index))) {
                throw new IOException("元数据删除列表含重复字段");
            }
        }
        try {
            return new MetadataPatch(tag.getString("worldId"), tag.getLong("baseVersion"),
                    tag.getLong("version"), tag.getLong("term"), tag.getString("operationId"),
                    tag.getCompound("set"), removed);
        } catch (IllegalArgumentException exception) {
            throw new IOException("元数据补丁字段非法", exception);
        }
    }

    private static CompoundTag header(String worldId, long version, long term) {
        CompoundTag tag = new CompoundTag();
        tag.putInt("format", FORMAT);
        tag.putString("worldId", worldId);
        tag.putLong("version", version);
        tag.putLong("term", term);
        return tag;
    }

    private static void validateHeader(CompoundTag tag) throws IOException {
        require(tag, "format", Tag.TAG_INT);
        require(tag, "worldId", Tag.TAG_STRING);
        require(tag, "version", Tag.TAG_LONG);
        require(tag, "term", Tag.TAG_LONG);
        if (tag.getInt("format") != FORMAT) {
            throw new IOException("不支持的元数据格式");
        }
    }

    private static void require(CompoundTag tag, String key, int type) throws IOException {
        if (tag == null || !tag.contains(key, type)) {
            throw new IOException("元数据字段缺失或类型非法: " + key);
        }
    }
}
