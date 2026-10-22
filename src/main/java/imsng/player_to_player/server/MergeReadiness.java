package imsng.player_to_player.server;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

/**
 * 合并的控制面屏障：迁移成员连接准备齐备后才开始预同步；只有 A 冻结和 B
 * 确认最终尾部均到达才允许提交。此类不替代数据层的耐久性/状态完整性校验。
 */
final class MergeReadiness {
    private final UUID source;
    private final UUID target;
    private final Set<UUID> required;
    private final Set<UUID> ready = new HashSet<>();
    private boolean started, frozen, received, terminal;

    MergeReadiness(UUID source, UUID target, Set<UUID> required) {
        this.source = source;
        this.target = target;
        this.required = Set.copyOf(required);
    }

    synchronized boolean prepared(UUID member) {
        if (terminal || started || !required.contains(member)) return false;
        ready.add(member);
        if (!ready.containsAll(required)) return false;
        started = true;
        return true;
    }

    synchronized boolean sourceFrozen(UUID reporter) {
        if (!started || terminal || !source.equals(reporter)) return false;
        frozen = true;
        return received;
    }

    synchronized boolean tailReceived(UUID reporter) {
        if (!started || terminal || !target.equals(reporter)) return false;
        received = true;
        return frozen;
    }

    synchronized boolean tryCommit() {
        if (terminal || !started || !frozen || !received) return false;
        terminal = true;
        return true;
    }

    synchronized boolean abort() {
        if (terminal) return false;
        terminal = true;
        return true;
    }
}
