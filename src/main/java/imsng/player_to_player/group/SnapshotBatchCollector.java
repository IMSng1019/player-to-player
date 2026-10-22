package imsng.player_to_player.group;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Function;
import java.util.function.LongSupplier;

/**
 * 将快照采集限制在一次有界主线程任务内；后台消费完本批才申请下一批，不积压全量 NBT。
 * 调用方必须逐批等待，迭代器与采集函数只会在传入的主线程执行器中访问。
 * 时间预算是软上限：单条原版序列化不可中断，但下一条开始前必须重新检查预算。
 */
final class SnapshotBatchCollector<K, V> {
    private final Iterator<K> keys;
    private final Executor mainExecutor;
    private final Function<K, V> capture;
    private final int maximumItems;
    private final long budgetNanos;
    private final LongSupplier nanoClock;

    SnapshotBatchCollector(Iterator<K> keys, Executor mainExecutor, Function<K, V> capture,
                           int maximumItems, long budgetNanos, LongSupplier nanoClock) {
        if (maximumItems < 1 || budgetNanos < 1) {
            throw new IllegalArgumentException("快照批量数与主线程时间预算必须为正数");
        }
        this.keys = keys;
        this.mainExecutor = mainExecutor;
        this.capture = capture;
        this.maximumItems = maximumItems;
        this.budgetNanos = budgetNanos;
        this.nanoClock = nanoClock;
    }

    CompletableFuture<Batch<K, V>> next() {
        long submitted = nanoClock.getAsLong();
        CompletableFuture<Batch<K, V>> result = new CompletableFuture<>();
        mainExecutor.execute(() -> {
            // 超时后尚在排队的旧任务不应再读取世界。
            if (result.isCancelled()) {
                return;
            }
            long started = nanoClock.getAsLong();
            List<Map.Entry<K, V>> values = new ArrayList<>();
            int visited = 0;
            try {
                while (keys.hasNext() && visited < maximumItems && !result.isCancelled()
                        && (visited == 0 || nanoClock.getAsLong() - started < budgetNanos)) {
                    K key = keys.next();
                    V value = capture.apply(key);
                    visited++;
                    if (value != null) {
                        values.add(Map.entry(key, value));
                    }
                }
                result.complete(new Batch<>(List.copyOf(values), visited, !keys.hasNext(),
                        started - submitted, nanoClock.getAsLong() - started));
            } catch (Exception error) {
                result.completeExceptionally(error);
            }
        });
        return result;
    }

    record Batch<K, V>(List<Map.Entry<K, V>> values, int visited, boolean finished,
                       long queueNanos, long captureNanos) {
    }
}
