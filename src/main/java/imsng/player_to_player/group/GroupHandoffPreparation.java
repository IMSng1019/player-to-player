package imsng.player_to_player.group;

import java.util.Objects;
import java.util.UUID;

/**
 * 主客户端退出时，副客户端必须先完成旧游戏连接的编排性关闭，再确认可以停服。
 * 同一事务的通知可能因 ACK 丢失重复到达；只重复确认，不能再次关闭新建的连接。
 * 状态与渲染操作分离，锁内不调用 Minecraft/网络代码，避免等待 Netty 回调死锁。
 */
public final class GroupHandoffPreparation {
    private static final class Attempt {
        final UUID transaction;
        final UUID source;
        boolean prepared;

        Attempt(UUID transaction, UUID source) {
            this.transaction = transaction;
            this.source = source;
        }
    }

    private Attempt active;

    /** 返回 true 才能发送成功确认；失败或被新会话清除的旧任务不得确认。 */
    public boolean prepare(UUID transaction, UUID source, Runnable disconnect) {
        Objects.requireNonNull(transaction);
        Objects.requireNonNull(source);
        Objects.requireNonNull(disconnect);
        Attempt attempt;
        synchronized (this) {
            if (active != null) {
                return active.transaction.equals(transaction) && active.source.equals(source)
                        && active.prepared;
            }
            active = attempt = new Attempt(transaction, source);
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
        return active != null && active.prepared;
    }

    /** 真正退出、换服或进入新组后撤销旧交接；迟到任务按对象身份失效。 */
    public synchronized void clear() {
        active = null;
    }
}
