package imsng.player_to_player.metadata;

import imsng.player_to_player.netproto.ControlMessage;
import imsng.player_to_player.netproto.MessageType;
import imsng.player_to_player.util.JsonUtil;
import net.minecraft.nbt.CompoundTag;
import net.minecraft.nbt.ListTag;
import net.minecraft.nbt.Tag;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 有界增量补齐的纯数据验证。整批先验证世界、任期、连续版本和声明终点，才允许交给
 * 服务器线程逐条应用；损坏或被截短的批次不能只应用前半段后误发成功ACK。
 */
final class MetadataCatchup {
    private MetadataCatchup() { }

    static List<MetadataPatch> decode(MetadataSnapshot base, ControlMessage response) throws IOException {
        if (response.type() != MessageType.METADATA_CATCHUP
                || !base.worldId().equals(JsonUtil.getString(response.json(), "worldId", ""))
                || base.term() != JsonUtil.getLong(response.json(), "term", -1))
            throw new IOException("元数据补齐响应世界或任期不匹配");
        long targetVersion = JsonUtil.getLong(response.json(), "version", -1);
        if (targetVersion < base.version()) throw new IOException("元数据补齐响应版本倒退");
        CompoundTag root = MetadataWire.decode(response.binary());
        if (!root.contains("Patches", Tag.TAG_LIST)) throw new IOException("元数据补齐响应缺少补丁列表");
        ListTag patches = (ListTag) root.get("Patches");
        if (patches.size() > 256 || (!patches.isEmpty() && patches.getElementType() != Tag.TAG_COMPOUND))
            throw new IOException("元数据补齐列表类型错误或超过窗口上限");
        List<MetadataPatch> result = new ArrayList<>();
        long nextVersion = base.version();
        for (Tag tag : patches) {
            MetadataPatch patch = MetadataWire.patchFromTag((CompoundTag) tag);
            if (!base.worldId().equals(patch.worldId()) || base.term() != patch.term() || patch.baseVersion() != nextVersion)
                throw new IOException("元数据补齐批次存在乱序、缺口或跨任期补丁");
            nextVersion = patch.version();
            result.add(patch);
        }
        if (nextVersion != targetVersion) throw new IOException("元数据补齐批次未达到声明终点");
        // 只保存有界补丁，不预先展开256份完整世界镜像。大NBT基线与小时间补丁组合
        // 不应把8MiB网络包放大成数GiB常驻堆内存；运行线程逐条消费并释放旧镜像。
        return List.copyOf(result);
    }
}
