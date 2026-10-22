package imsng.player_to_player.server;

import java.util.UUID;
import java.util.HashMap;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.function.BooleanSupplier;

/**
 * 玩家上行的连接内顺序与执行时授权门控。
 * <p>客户端超时不代表服务端IO任务被取消。只有记录已经耐久写入的最高序号，
 * 才能阻止较早排队的上传在最终快照ACK后覆盖新状态。连接采用弱引用键：旧连接
 * 的所有排队任务释放引用后，这份去重状态可回收；新控制连接拥有独立序号空间。</p>
 */
final class PlayerUploadOrder {
    private static final class Confirmed {
        long sequence;
        String digest;
    }

    private final Map<Object, Map<UUID, Confirmed>> connections = new WeakHashMap<>();

    /** 生产调用必须已经持有PlayerDataFileStore的玩家写锁，避免不同主客户端并发替换文件。 */
    boolean write(Object connection, UUID playerId, long sequence, String digest,
                  BooleanSupplier authorized, BooleanSupplier writer) {
        if (connection == null || playerId == null || sequence <= 0 || digest == null
                || authorized == null || writer == null) return false;
        Confirmed latest;
        synchronized (connections) {
            latest = connections.computeIfAbsent(connection, ignored -> new HashMap<>())
                    .computeIfAbsent(playerId, ignored -> new Confirmed());
        }
        synchronized (latest) {
            // 连幂等重试也必须重新核验：旧主降为副客户端后不得继续上传“自己”的旧NBT。
            if (!authorized.getAsBoolean()) return false;
            if (sequence < latest.sequence) return true; // 已被更新且耐久的同连接快照覆盖，无需再写。
            if (sequence == latest.sequence) return digest.equals(latest.digest);
            if (!writer.getAsBoolean()) return false; // 写盘失败不消耗序号，允许同帧可靠重试。
            latest.sequence = sequence;
            latest.digest = digest;
            return true;
        }
    }

    static boolean isAuthorized(GroupTable groups, UUID peer, UUID playerId) {
        if (groups == null || peer == null || playerId == null) return false;
        // 上行只由正在运行权威集成服的主客户端产生；本人身份不等于当前世界写权限。
        UUID group = groups.groupOf(playerId);
        return group != null && peer.equals(groups.primaryOf(group));
    }
}
