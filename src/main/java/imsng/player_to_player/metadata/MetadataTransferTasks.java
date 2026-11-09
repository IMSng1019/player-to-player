package imsng.player_to_player.metadata;

import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * 迁移请求与中心元数据发布的串行入口。
 *
 * <p>网络线程只投递任务；所属主线程先完成已经在途的检查点，再进入会话锁和迁移
 * 协调器。不能持有这些业务锁等待磁盘，否则退出/换连接可能反过来等待同一锁。
 * 这里等待的是一份已经提交的检查点，不创建新采样，也不降低任何版本/所有权检查。</p>
 *
 * <p>独立于 Minecraft 类型，使入队、发布、实例退休和会话替换的顺序可直接回归验证。
 * Executor 必须是所属服务器的串行执行器；各阶段共用此入口以保持原连接请求顺序。</p>
 */
final class MetadataTransferTasks {
    @FunctionalInterface interface Barrier { void await() throws Exception; }

    private final Executor owner;
    private final BooleanSupplier active;
    private final Barrier barrier;

    MetadataTransferTasks(Executor owner, BooleanSupplier active, Barrier barrier) {
        this.owner = Objects.requireNonNull(owner);
        this.active = Objects.requireNonNull(active);
        this.barrier = Objects.requireNonNull(barrier);
    }

    void execute(Predicate<Runnable> sessionGuard, Runnable action, Consumer<Exception> rejected) {
        Objects.requireNonNull(sessionGuard);
        Objects.requireNonNull(action);
        Objects.requireNonNull(rejected);
        try {
            owner.execute(() -> {
                Exception failure = null;
                try {
                    requireActive();
                    barrier.await();
                    // 发布可能发现持久化故障并关闭服务；队列中的旧实例也不能复活。
                    requireActive();
                    if (!sessionGuard.test(action)) {
                        throw new IllegalStateException("迁移请求来自已失效的控制会话");
                    }
                } catch (Exception error) {
                    failure = error;
                }
                if (failure != null) rejected.accept(failure);
            });
        } catch (RuntimeException schedulingFailure) {
            rejected.accept(schedulingFailure);
        }
    }

    private void requireActive() {
        if (!active.getAsBoolean()) throw new IllegalStateException("迁移所属元数据服务已关闭或替换");
    }
}
