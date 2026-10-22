package imsng.player_to_player.server;

import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 新组就绪前暂存跟随成员。每次登记拥有独立对象，即使主客户端和成员名单完全相同，
 * 也属于另一轮交接；定时器必须携带该轮登记对象，不能仅凭可复用的组UUID结束等待。
 */
final class PendingGroupRedirects {
    static final class Entry {
        private final Set<UUID> members;
        private Entry(Set<UUID> members) { this.members = Set.copyOf(members); }
        Set<UUID> members() { return members; }
    }

    private final Map<UUID, Entry> pending = new ConcurrentHashMap<>();

    Entry register(UUID group, Set<UUID> members) {
        Entry entry = new Entry(members);
        pending.put(group, entry);
        return entry;
    }

    Entry ready(UUID group) { return pending.remove(group); }

    Entry expire(UUID group, Entry expected) {
        // Entry刻意保留对象身份语义，不按成员集合比较；同组同成员的新分离也不能被旧timer删除。
        // ConcurrentHashMap条件删除与就绪消费线性化，只有一个分支能够取走本轮成员。
        return pending.remove(group, expected) ? expected : null;
    }

    void clear() { pending.clear(); }
}
