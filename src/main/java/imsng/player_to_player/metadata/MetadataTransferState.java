package imsng.player_to_player.metadata;

import net.minecraft.nbt.CompoundTag;
import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/** 冻结时捕获一次的模拟字段尾部；基础版本冲突可以重取，尾部候选绝不能被旧镜像替换。 */
final class MetadataTransferState {
    private final String worldId;
    private final long term;
    private final Set<String> fields;
    private final CompoundTag values;

    MetadataTransferState(MetadataSnapshot snapshot, Set<String> fields, CompoundTag live) {
        this.worldId = snapshot.worldId(); this.term = snapshot.term();
        this.fields = Set.copyOf(fields); this.values = new CompoundTag();
        for (String field : fields) if (live.contains(field)) values.put(field, Objects.requireNonNull(live.get(field)).copy());
    }

    Optional<MetadataPatch> patchAgainst(MetadataSnapshot latest, String operation) {
        if (!worldId.equals(latest.worldId()) || term != latest.term())
            throw new IllegalStateException("模拟尾部的世界身份或写入任期已变化，必须中止接管");
        CompoundTag changed = new CompoundTag(), previous = latest.data();
        Set<String> removed = new HashSet<>();
        for (String field : fields) {
            if (Objects.equals(values.get(field), previous.get(field))) continue;
            if (values.contains(field)) changed.put(field, Objects.requireNonNull(values.get(field)).copy());
            else removed.add(field);
        }
        if (changed.isEmpty() && removed.isEmpty()) return Optional.empty();
        return Optional.of(new MetadataPatch(worldId, latest.version(), latest.version() + 1,
                term, operation, changed, removed));
    }
}
