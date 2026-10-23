package imsng.player_to_player.server;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.UUID;
import java.util.function.Supplier;

/**
 * 全服运行版本准入屏障。在线组继续运行它们已经启动的版本，新发布的静态环境只有在
 * 全部旧版组退出后才能产生新版组，防止新增模组/世界生成配置跨版本合并。
 * <p>
 * 这里保存的是完成角色准入的版本，不能直接使用最新 HELLO 声明：同一个玩家的新
 * 进程会先发送 HELLO，旧组可能仍等待退出恢复；新声明不能抹掉旧写者的运行版本。
 * 主副成员都记录，因此分离时原副客户端晋升仍继承原组的版本证据。
 */
final class EnvironmentAdmissionGate {
    static final EnvironmentAdmissionGate SERVER = new EnvironmentAdmissionGate();
    private final Map<UUID, String> admitted = new HashMap<>();

    synchronized String admit(GroupTable groups, UUID client, String actual, Supplier<String> required,
                              Runnable assignment) {
        // 除并发ROLE_REQUEST外，还要与组表的merge/split写事务互斥。否则它们摘掉旧组
        // 再放入新组的窗口会被误判为“全服已经没有旧组”。组表内的重入写操作是安全的。
        return groups.withMutationLock(() -> {
            String published = required.get();
            if (!valid(actual)) return "缺少有效的已启动环境版本，请完成环境预检后重新连接。";
            if (!valid(published)) return "服务端环境尚未准备完成，请稍后重新连接。";
            if (!actual.equals(published)) return "服务器环境已经更新，请重新连接并启动最新环境。";

            var primaries = groups.primariesSnapshot();
            for (UUID primary : primaries.values()) {
                if (!actual.equals(admitted.get(primary))) {
                    return "旧版本组仍在运行，请等待全部旧组安全退出后重新连接，以免混用不同世界环境。";
                }
            }
            // 仅在稳定组表下回收离线记录。副客户端记录必须保留，不能只保留主客户端。
            HashSet<UUID> activeMembers = new HashSet<>();
            for (UUID group : primaries.keySet()) activeMembers.addAll(groups.membersOf(group));
            admitted.keySet().retainAll(activeMembers);
            admitted.put(client, actual);
            try {
                assignment.run();
            } catch (RuntimeException | Error failed) {
                // 若写组已发生但后续通知失败，保留版本证据；只有完全未入组才能清除。
                if (groups.groupOf(client) == null) admitted.remove(client);
                throw failed;
            }
            return "";
        });
    }

    synchronized boolean sameRuntime(UUID first, UUID second) {
        if (first == null || second == null) return false;
        String version = admitted.get(first);
        return valid(version) && version.equals(admitted.get(second));
    }

    synchronized void clear() { admitted.clear(); }

    private static boolean valid(String hash) { return hash != null && hash.matches("[0-9a-f]{64}"); }
}
