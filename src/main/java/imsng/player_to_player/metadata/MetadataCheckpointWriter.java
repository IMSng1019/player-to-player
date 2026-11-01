package imsng.player_to_player.metadata;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.FutureTask;

/**
 * 自然时间检查点的单飞后台写入器。
 *
 * <p>调用方在服务器线程采集不可变的 NBT 副本，后台只压缩和耐久写入。最多存在一个
 * 在途检查点；慢盘期间跳过中间自然采样，完成后下一周期重新采集最新运行态，绝不把
 * 每秒采样积压成无界任务队列。离散命令、任期屏障及停服通过 {@link #drain()} 等待
 * 并发布前一检查点，然后才执行自己的操作，因此旧采样不会覆盖新的命令结果。</p>
 *
 * <p>除后台 FutureTask 的完成状态外，生命周期方法由同一服务器线程调用。
 * 发布回调只在 poll/drain/close 的调用线程执行，后台线程绝不接触 Minecraft 对象。
 * 任一写入/发布错误会封停后续采样，调用方据此关闭元数据控制会话。</p>
 */
final class MetadataCheckpointWriter implements AutoCloseable {
    @FunctionalInterface interface Write { MetadataPatch run() throws IOException; }
    @FunctionalInterface interface Publish { void accept(MetadataPatch patch) throws IOException; }

    private final Executor executor;
    private final Publish publish;
    /** 转移屏障可从其他线程只读此标记；成功但尚未发布的结果仍然算在途。 */
    private volatile FutureTask<MetadataPatch> inFlight;
    private IOException failure;
    private boolean closed;

    MetadataCheckpointWriter(Executor executor, Publish publish) {
        this.executor = Objects.requireNonNull(executor);
        this.publish = Objects.requireNonNull(publish);
    }

    boolean submit(Write write) throws IOException {
        if (failure != null) throw failure;
        if (closed) throw new IOException("元数据检查点写入器已关闭");
        if (inFlight != null) return false;
        FutureTask<MetadataPatch> task = new FutureTask<>(write::run);
        inFlight = task;
        try {
            executor.execute(task);
        } catch (RuntimeException rejected) {
            inFlight = null;
            failure = new IOException("无法调度元数据检查点", rejected);
            throw failure;
        }
        return true;
    }

    boolean isPending() { return inFlight != null; }

    /** 普通 tick 只收割已经完成的写入，绝不等待磁盘。 */
    void poll() throws IOException {
        if (failure != null) throw failure;
        FutureTask<MetadataPatch> task = inFlight;
        if (task != null && task.isDone()) drain();
    }

    /**
     * 强顺序边界等待写入并恰好发布一次。中断不能让停服先关闭存储、后台随后继续写，
     * 因此等待不可取消，完成后恢复中断标记；这与原来的同步耐久提交语义一致。
     */
    void drain() throws IOException {
        if (failure != null) throw failure;
        FutureTask<MetadataPatch> task = inFlight;
        if (task == null) return;
        boolean interrupted = false;
        try {
            MetadataPatch patch;
            while (true) {
                try {
                    patch = task.get();
                    break;
                } catch (InterruptedException ignored) {
                    interrupted = true;
                }
            }
            // 发布结束前仍保持在途，跨线程的转移检查不能在镜像/版本更新到一半时放行。
            // 成功后清除，异常分支也会清除并封停，下一次 tick 不会重复广播。
            publish.accept(patch);
            inFlight = null;
        } catch (ExecutionException error) {
            inFlight = null;
            Throwable cause = error.getCause();
            failure = cause instanceof IOException io ? io : new IOException("元数据检查点写入失败", cause);
            throw failure;
        } catch (IOException error) {
            inFlight = null;
            failure = error;
            throw error;
        } catch (RuntimeException error) {
            inFlight = null;
            failure = new IOException("元数据检查点发布失败", error);
            throw failure;
        } finally {
            if (interrupted) Thread.currentThread().interrupt();
        }
    }

    @Override public void close() throws IOException {
        closed = true;
        drain();
    }
}
