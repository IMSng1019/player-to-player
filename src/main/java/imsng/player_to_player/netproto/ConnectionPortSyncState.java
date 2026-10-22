package imsng.player_to_player.netproto;

import java.util.concurrent.TimeUnit;

/**
 * 按原版 play 连接隔离端口同步状态，不依赖客户端类型，便于独立验证登录时序。
 * 网络线程写入快照，加入工作线程等待快照；所有转换都由同一把对象锁保护。
 */
public final class ConnectionPortSyncState {
    private Object connection;
    private long generation;
    private Ports ports;
    private Boolean advertised;

    /** play 监听器构造完成时建立连接归属，此时 JOIN 尚未发生。 */
    public synchronized void beginConnection(Object nextConnection) {
        if (connection != nextConnection) {
            connection = nextConnection;
            generation++;
            ports = null;
            advertised = null;
            notifyAll();
        }
    }

    /**
     * 开始一次物理服加入，返回 INIT 已建立连接的不可变代次票据。
     * JOIN 只是消费该连接已有的结果，不能再次清空 Netty 在 INIT 后提前交付的快照。
     * 连接身份必须来自 INIT 事件，避免监听器构造期间客户端尚未发布 getConnection()。
     */
    public synchronized Join beginJoin() {
        return new Join(generation);
    }

    /** 旧连接的迟到包不能借写入动作重新激活旧代次。 */
    public synchronized boolean receive(Object sourceConnection, int controlPort, int relayPort) {
        if (connection == null || sourceConnection != connection
                || controlPort < 1 || controlPort > 65535 || relayPort < 0 || relayPort > 65535) {
            return false;
        }
        ports = new Ports(controlPort, relayPort);
        notifyAll();
        return true;
    }

    /** 频道事件可能是增量列表；确认支持后，不允许后续不相关增量将其覆盖。 */
    public synchronized void advertise(Object sourceConnection, boolean supportsPortSync) {
        if (connection == null || sourceConnection != connection) {
            return;
        }
        if (supportsPortSync || advertised == null) {
            advertised = supportsPortSync;
        }
        notifyAll();
    }

    /** 只取消被关闭的连接；旧连接的 DISCONNECT 不得清理已经建立的新连接。 */
    public synchronized void disconnect(Object closedConnection) {
        if (connection != null && connection == closedConnection) {
            connection = null;
            generation++;
            ports = null;
            advertised = null;
            notifyAll();
        }
    }

    /**
     * 等待票据所属连接；换连接或断开立即取消，绝不返回新连接的端口给旧工作线程。
     * 能力未知走基础超时，确认支持后给予慢链路上限；超时仍交由调用方回退本地配置。
     */
    public synchronized Ports await(Join join, long baseWaitMillis, long supportedWaitMillis) {
        if (join == null) {
            return null;
        }
        long start = System.nanoTime();
        while (connection != null && generation == join.generation()) {
            if (ports != null) {
                return ports;
            }
            if (Boolean.FALSE.equals(advertised)) {
                return null;
            }
            long waitMillis = Boolean.TRUE.equals(advertised) ? supportedWaitMillis : baseWaitMillis;
            long remaining = TimeUnit.MILLISECONDS.toNanos(waitMillis) - (System.nanoTime() - start);
            if (remaining <= 0) {
                return null;
            }
            try {
                TimeUnit.NANOSECONDS.timedWait(this, remaining);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                return null;
            }
        }
        return null;
    }

    public record Join(long generation) {
    }

    public record Ports(int controlPort, int relayPort) {
    }
}
