package imsng.player_to_player.group;

import java.util.ArrayDeque;
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.function.BooleanSupplier;

/**
 * 仅承载明确提交的迁移控制任务，不接入 MinecraftServer 的普通任务队列。
 * 后台只负责提交，实际读取世界仍在绑定的服务器主线程；每次轮询最多执行一项。
 * 独立于 Minecraft 类型，使区块等待、取消和实例退休这些并发规则可以直接回归测试。
 */
final class GroupControlTaskQueue implements Executor {
    private final Thread mainThread;
    private final BooleanSupplier currentInstance;
    private final ArrayDeque<Runnable> tasks = new ArrayDeque<>();
    private boolean closed;
    private boolean running;

    GroupControlTaskQueue(Thread mainThread, BooleanSupplier currentInstance) {
        this.mainThread = Objects.requireNonNull(mainThread, "服务器主线程");
        this.currentInstance = Objects.requireNonNull(currentInstance, "实例有效性判定");
    }

    @Override
    public synchronized void execute(Runnable task) {
        Objects.requireNonNull(task, "控制任务");
        if (!isCurrent()) {
            throw new RejectedExecutionException("组世界已停止或切换，拒绝迁移控制任务");
        }
        // 即使提交方恰在主线程也只入队，防止在模组/区块回调内部立即重入采集。
        tasks.addLast(task);
    }

    synchronized boolean poll() {
        if (Thread.currentThread() != mainThread) {
            throw new IllegalStateException("迁移控制任务只能由绑定的服务器主线程执行");
        }
        if (!isCurrent() || running) {
            return false;
        }
        Runnable task = tasks.pollFirst();
        if (task == null) {
            return false;
        }
        running = true;
        try {
            // 锁覆盖执行窗口，使 close 返回之后不存在仍会开始读取旧世界的任务。
            // 控制任务自身必须有界且不可等待普通 tick；快照调用方按批次限制采集成本。
            task.run();
        } finally {
            running = false;
        }
        return true;
    }

    synchronized void close() {
        closed = true;
        tasks.clear();
    }

    /** 调用方已经持有本队列锁；运行时失效也永久退休本实例的旧执行器。 */
    private boolean isCurrent() {
        if (closed || !currentInstance.getAsBoolean()) {
            close();
            return false;
        }
        return true;
    }
}
