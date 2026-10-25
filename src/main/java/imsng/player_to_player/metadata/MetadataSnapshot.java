package imsng.player_to_player.metadata;

import net.minecraft.nbt.CompoundTag;

import java.util.Objects;

/**
 * 某世界、某写入任期内已提交的完整 Data 字段镜像。
 * 所有 NBT 都按值复制；调用者不能通过修改可变 CompoundTag 绕开版本提交。
 * Player 属于独立玩家状态通道，绝不进入全局镜像。
 */
public record MetadataSnapshot(String worldId, long version, long term, CompoundTag data) {
    public MetadataSnapshot {
        if (worldId == null || worldId.isBlank() || worldId.length() > 512) {
            throw new IllegalArgumentException("元数据世界身份不能为空或过长");
        }
        if (version < 0 || term <= 0) {
            throw new IllegalArgumentException("元数据版本或任期非法");
        }
        data = Objects.requireNonNull(data, "data").copy();
        data.remove("Player");
    }

    @Override
    public CompoundTag data() {
        return data.copy();
    }
}
