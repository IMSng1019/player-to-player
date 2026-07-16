package imsng.player_to_player.group;

import java.util.Objects;
import java.util.function.Predicate;

/**
 * 集成服务端 LAN 发布的一次性就绪门控。
 *
 * <p>集成服务端的 {@code SERVER_STARTED} 回调发生在服务器线程，但此时客户端侧
 * 玩家和游戏连接可能尚未创建，不能立刻调用原版 LAN 发布入口。服务器线程通过
 * {@link #arm(Object)} 登记待发布实例，客户端主线程每 tick 调用
 * {@link #takeIfReady(Object, boolean, boolean, Predicate)} 检查真实就绪条件。</p>
 *
 * <p>这里刻意使用引用身份而不是 {@link Object#equals(Object)}：集成服务端实例本身
 * 就是一次世界生命周期的唯一标识。旧世界的实例即使携带相同存档信息，也绝不能
 * 消费新世界的发布任务。</p>
 *
 * @param <T> 作为生命周期身份的服务器实例类型
 */
public final class GroupPublicationGate<T> {

    /**
     * 等待客户端就绪后发布的服务器实例。
     *
     * <p>写入来自服务器线程，读取来自客户端主线程，因此使用 volatile 保证可见性；
     * 最后的核对和清除仍在同步区中完成，以保证一次性消费。</p>
     */
    private volatile T pending;

    /**
     * 登记一个新的待发布服务器实例；新生命周期会替换尚未消费的旧实例。
     */
    public synchronized void arm(T value) {
        pending = Objects.requireNonNull(value, "value");
    }

    /**
     * 当服务器身份、客户端玩家、客户端连接和组运行时同时就绪时消费待发布实例。
     *
     * @param current         客户端当前持有的集成服务端实例
     * @param playerReady     客户端玩家对象是否已经创建
     * @param connectionReady 客户端游戏连接是否已经创建
     * @param active          判断该实例是否仍由组运行时接管
     * @return 可以发布且由本次调用成功消费的服务器；条件不足或已被消费时返回 null
     */
    public T takeIfReady(T current, boolean playerReady, boolean connectionReady,
                         Predicate<T> active) {
        Objects.requireNonNull(active, "active");
        T candidate = pending;
        if (candidate == null || current != candidate || !playerReady || !connectionReady
                || !active.test(candidate)) {
            return null;
        }

        synchronized (this) {
            // 初步检查之后可能发生 reset、重新 arm 或其他线程先行消费，故必须
            // 在清除前再次核对引用身份与运行时状态。
            if (pending != candidate || current != candidate || !active.test(candidate)) {
                return null;
            }
            pending = null;
            return candidate;
        }
    }

    /**
     * 丢弃尚未消费的发布任务；世界会话拆除和新宿主启动前均可安全重复调用。
     */
    public synchronized void reset() {
        pending = null;
    }
}
