package imsng.player_to_player.group;

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;

/**
 * 分离两种生命周期：调用线程的同步切换作用域，以及旧游戏连接的一次断连事件。
 * Fabric 可以在 Netty 线程先发出 DISCONNECT，而调用线程之后还要执行原版的多次
 * disconnect。消费事件只能免除旧会话拆除，不能提前撤销同步调用的退出屏障旁路。
 * 本类不引用客户端类型，便于在无图形环境中回归验证真实的跨线程事件顺序。
 */
public final class ConnectionSwitchGuard {
    private final ThreadLocal<Integer> depth = ThreadLocal.withInitial(() -> 0);
    private final Set<Object> pendingDisconnects =
            Collections.newSetFromMap(new IdentityHashMap<>());

    /**
     * 覆盖整段同步切换，包括 ConnectScreen.startConnecting 内部再次清理旧世界。
     * oldConnection 为 null 表示当前没有旧游戏连接，此时仍要保护原版的空世界清理。
     */
    public void run(Object oldConnection, Runnable transition) {
        int previousDepth = depth.get();
        depth.set(previousDepth + 1);
        synchronized (pendingDisconnects) {
            if (oldConnection != null) pendingDisconnects.add(oldConnection);
        }
        boolean completed = false;
        try {
            // 绝不能持 pendingDisconnects 的锁执行游戏调用：关闭 channel 会等待
            // Netty 回调，而回调需要消费事件；持锁执行将制造新的线程互等。
            transition.run();
            completed = true;
        } finally {
            if (previousDepth == 0) depth.remove();
            else depth.set(previousDepth);
            if (!completed) {
                // 若还未真正断开就失败，不可把许可留给玩家下一次主动退出。
                synchronized (pendingDisconnects) { pendingDisconnects.remove(oldConnection); }
            }
        }
    }

    /** 仅调用切换的线程可旁路 disconnect；事件线程消费许可不会改变这个状态。 */
    public boolean isSwitching() {
        return depth.get() > 0;
    }

    /** 只认具体旧连接，迟到事件不能误吞新世界的退出或连接失败。 */
    public boolean consumeDisconnect(Object connection) {
        if (connection == null) return false;
        synchronized (pendingDisconnects) { return pendingDisconnects.remove(connection); }
    }

    /** 会话拆除后释放可能尚未收到回调的旧连接引用；正在执行的作用域由 finally 清理。 */
    public void clear() {
        synchronized (pendingDisconnects) { pendingDisconnects.clear(); }
    }
}
