package imsng.player_to_player.util;

import org.slf4j.LoggerFactory;
import java.util.function.Consumer;
import java.util.function.LongSupplier;

/**
 * 工作流阶段计时。只用本机单调时钟计算时长，跨机器通过事务 ID 关联日志，
 * 绝不相减两台机器的墙钟。结束后忽略迟到事件，避免把取消流程记成成功。
 */
public final class WorkflowTrace {
    public record Event(String kind, String id, String phase, long phaseMillis, long totalMillis) { }
    private final String kind;
    private final String id;
    private final LongSupplier clock;
    private final Consumer<Event> sink;
    private final long started;
    private long previous;
    private boolean finished;

    public WorkflowTrace(String kind, String id) {
        this(kind, id, System::nanoTime, event ->
                LoggerFactory.getLogger("player_to_player/timing").info(
                        "workflow={} id={} phase={} phase_ms={} total_ms={}",
                        event.kind(), event.id(), event.phase(), event.phaseMillis(), event.totalMillis()));
    }

    WorkflowTrace(String kind, String id, LongSupplier clock, Consumer<Event> sink) {
        this.kind = kind;
        this.id = id;
        this.clock = clock;
        this.sink = sink;
        started = previous = clock.getAsLong();
        mark("begin");
    }

    public synchronized void mark(String phase) {
        if (finished) return;
        long now = clock.getAsLong();
        sink.accept(new Event(kind, id, phase, (now - previous) / 1_000_000,
                (now - started) / 1_000_000));
        previous = now;
    }

    public synchronized void finish(String result) {
        if (finished) return;
        mark(result);
        finished = true;
    }

    public synchronized boolean isFinished() { return finished; }

    public long elapsedMillis() { return (clock.getAsLong() - started) / 1_000_000; }
}
