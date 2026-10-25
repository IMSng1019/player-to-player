package imsng.player_to_player.metadata;

import net.minecraft.nbt.CompoundTag;

import java.util.Objects;
import java.util.Set;

/**
 * 按顶层字段替换的有类型补丁。删除单独表示，空 NBT 值与删除不混淆。
 * 嵌套复合字段由其字段适配器整体采集，避免不同适配器同时修改同一棵子树。
 */
public record MetadataPatch(String worldId, long baseVersion, long version, long term,
                            String operationId, CompoundTag set, Set<String> removed) {
    public MetadataPatch {
        if (worldId == null || worldId.isBlank() || worldId.length() > 512
                || operationId == null || operationId.isBlank() || operationId.length() > 128) {
            throw new IllegalArgumentException("元数据世界身份或操作 ID 非法");
        }
        if (baseVersion < 0 || baseVersion == Long.MAX_VALUE || version != baseVersion + 1 || term <= 0) {
            throw new IllegalArgumentException("元数据补丁版本或任期非法");
        }
        set = Objects.requireNonNull(set, "set").copy();
        removed = Set.copyOf(Objects.requireNonNull(removed, "removed"));
        for (String field : set.getAllKeys()) {
            validateField(field);
            if (removed.contains(field)) {
                throw new IllegalArgumentException("元数据字段不能同时设置和删除: " + field);
            }
        }
        removed.forEach(MetadataPatch::validateField);
    }

    private static void validateField(String field) {
        if (field == null || field.isBlank() || field.length() > 1024 || field.equals("Player")) {
            throw new IllegalArgumentException("禁止修改元数据字段: " + field);
        }
    }

    @Override
    public CompoundTag set() {
        return set.copy();
    }

    /** 只有顺序连续且任期一致的补丁才允许应用；任期改变必须重新获取快照。 */
    public MetadataSnapshot applyTo(MetadataSnapshot previous) {
        if (!worldId.equals(previous.worldId()) || term != previous.term()
                || baseVersion != previous.version()) {
            throw new IllegalStateException("元数据补丁缺口、世界身份或任期不匹配，必须补齐快照");
        }
        CompoundTag data = previous.data();
        removed.forEach(data::remove);
        for (String field : set.getAllKeys()) {
            data.put(field, Objects.requireNonNull(set.get(field)).copy());
        }
        return new MetadataSnapshot(worldId, version, term, data);
    }
}
