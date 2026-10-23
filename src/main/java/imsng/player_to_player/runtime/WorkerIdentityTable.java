package imsng.player_to_player.runtime;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * 父进程已经认证的 P2P 身份与其连接子服的 TCP 源端口绑定。
 * 授权只经带随机令牌的本机 IPC 写入，在发送首个游戏字节前完成；登录只能消费一次。
 * 这保留全部原版/模组登录字节，同时避免离线 UUID 导致权威玩家背包和权限错位。
 */
public final class WorkerIdentityTable {
    public record Identity(UUID playerId, String playerName) { }
    private record Grant(Identity identity, long expires) { }
    private final Map<Integer, Grant> grants = new ConcurrentHashMap<>();

    public void grant(int port, UUID playerId, String playerName) {
        if (port <= 0 || port > 65535 || playerId == null || playerName == null
                || !playerName.matches("[A-Za-z0-9_]{1,16}")) {
            throw new IllegalArgumentException("非法的游戏连接身份");
        }
        long now = System.nanoTime();
        grants.entrySet().removeIf(entry -> entry.getValue().expires < now);
        if (grants.size() >= 256) throw new IllegalStateException("待登录授权数量过多");
        grants.put(port, new Grant(new Identity(playerId, playerName),
                now + TimeUnit.SECONDS.toNanos(30)));
    }

    public Identity consume(SocketAddress remote, String claimedName) {
        if (!(remote instanceof InetSocketAddress inet) || inet.getAddress() == null
                || !inet.getAddress().isLoopbackAddress()) return null;
        Grant grant = grants.remove(inet.getPort());
        return grant != null && grant.expires >= System.nanoTime()
                && grant.identity.playerName().equals(claimedName) ? grant.identity : null;
    }
}
