package imsng.player_to_player.metadata;

import java.util.Set;

/** 启动资源变化不能伪装为字段已热应用；调用者必须暂停实例并经过完整存档重启流程。 */
public final class MetadataRestartRequiredException extends IllegalStateException {
    private final Set<String> fields;

    public MetadataRestartRequiredException(Set<String> fields) {
        super("世界元数据启动字段发生变化，必须重新加载计算实例: " + String.join(", ", new java.util.TreeSet<>(fields)));
        this.fields = Set.copyOf(fields);
    }

    public Set<String> fields() { return fields; }
}
