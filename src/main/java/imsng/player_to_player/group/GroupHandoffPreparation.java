package imsng.player_to_player.group;

import java.util.Objects;
import java.util.UUID;

/**
 * 分离及主客户端退出时，副客户端必须先完成旧游戏连接的编排性关闭，再确认可以停服。
 * 同一事务的通知可能因 ACK 丢失重复到达；只重复确认，不能再次关闭新建的连接。
 * 已完成的普通分离等待可由同源主客户端退出接替；反方向的迟到通知不能覆盖退出等待。
 * 状态与渲染操作分离，锁内不调用 Minecraft/网络代码，避免等待 Netty 回调死锁。
 */
public final class GroupHandoffPreparation {
    private static final class Attempt {
        final UUID transaction;
        final UUID source;
        final boolean primaryExit;
        boolean prepared;
        boolean connecting;
        ConnectionIntent connectionIntent;

        Attempt(UUID transaction, UUID source, boolean primaryExit) {
            this.transaction = transaction;
            this.source = source;
            this.primaryExit = primaryExit;
        }
    }

    /** 后台隧道任务持有的具体连接意图；事务被退出接替或会话结束后自然失效。 */
    public static final class ConnectionIntent {
        private final Attempt owner;
        private ConnectionIntent(Attempt owner) { this.owner = owner; }
    }

    private Attempt active;

    /** 普通分离也必须在关闭旧游戏连接后进入等待，供退出交接接替。 */
    public boolean prepareSplit(UUID transaction, UUID source, Runnable disconnect) {
        return prepare(transaction, source, false, disconnect);
    }

    /** 返回 true 才能发送成功确认；失败或被新会话清除的旧任务不得确认。 */
    public boolean prepare(UUID transaction, UUID source, Runnable disconnect) {
        return prepare(transaction, source, true, disconnect);
    }

    private boolean prepare(UUID transaction, UUID source, boolean primaryExit, Runnable disconnect) {
        Objects.requireNonNull(transaction);
        Objects.requireNonNull(source);
        Objects.requireNonNull(disconnect);
        Attempt attempt;
        synchronized (this) {
            if (active != null) {
                if (active.transaction.equals(transaction) && active.source.equals(source)
                        && active.primaryExit == primaryExit) return active.prepared && !active.connecting;
                // 原分离已经关掉游戏连接，但控制连接和组归属仍在。主客户端此时退出
                // 必须接替等待；若要求重新出现游戏世界，原主会永久等不到停服确认。
                if (!primaryExit || active.primaryExit || !active.prepared || active.connecting
                        || !active.source.equals(source)) return false;
            }
            active = attempt = new Attempt(transaction, source, primaryExit);
        }
        try {
            disconnect.run();
            synchronized (this) {
                if (active != attempt) return false;
                attempt.prepared = true;
                return true;
            }
        } catch (RuntimeException | Error failure) {
            synchronized (this) {
                if (active == attempt) active = null;
            }
            throw failure;
        }
    }

    public synchronized boolean isPrepared() {
        return active != null && active.prepared && !active.connecting;
    }

    /**
     * 角色指派先占用一份后台准备意图，重复指派不能再开一条隧道。此时尚未开始原版
     * 登录，仍允许主客户端退出接替普通分离；被替换的意图不能再激活游戏连接。
     */
    public synchronized ConnectionIntent reserveConnection(UUID transaction, UUID targetGroup) {
        if (!isPrepared() || !active.transaction.equals(transaction) || active.connectionIntent != null
                || (active.primaryExit && active.source.equals(targetGroup))) return null;
        return active.connectionIntent = new ConnectionIntent(active);
    }

    /**
     * 必须紧接着在渲染线程启动原版登录。ConnectScreen 的半登录连接不属于 ClientLevel，
     * 此后 level=null 不能再作为“旧游戏连接已关闭”的证据，须等 JOIN 清理本状态。
     */
    public synchronized boolean beginConnecting(ConnectionIntent intent) {
        if (!isCurrentConnection(intent) || active.connecting) return false;
        active.connecting = true;
        return true;
    }

    /** 后台准备、失败回调和最终激活都按对象身份核对，不能只判断中心控制连接没变。 */
    public synchronized boolean isCurrentConnection(ConnectionIntent intent) {
        return intent != null && active == intent.owner && active.connectionIntent == intent;
    }

    /** 真正退出、换服或进入新组后撤销旧交接；迟到任务按对象身份失效。 */
    public synchronized void clear() {
        active = null;
    }
}
