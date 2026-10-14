package imsng.player_to_player.server;

import imsng.player_to_player.registry.ChunkKey;
import imsng.player_to_player.registry.ChunkWriteback;

import java.util.Objects;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

/**
 * 服务端区块写入的分片互斥器（<b>逻辑</b>串行化，不是 MCA 文件级锁）。
 * <p>
 * 实时上行、退出事务写入和 CHUNK_RELEASE 最终写回必须使用同一组锁。特别是最终写回
 * 与 ownership 释放不能分成两个临界区，否则迟到实时上行可能在中间插入并覆盖最终快照。
 *
 * <h2>为什么不需要"每 region 文件一把锁"（重要，勿重复"修"）</h2>
 * 分片锁保护的是<b>同一区块的写顺序</b>（上行/物化/最终写回/退出冻结之间的竞态），
 * 而 <b>MCA 文件字节级并发写入由原版存储栈自己串行化</b>：所有落盘都走
 * {@code level.getChunkSource().chunkMap.write(pos, tag)}（见
 * {@code ChunkDataHandlers#materializeOne / #validateAndWriteLocked}），
 * 而 {@code ChunkStorage.write} 只是把任务投进该 {@code ChunkMap} 自己的
 * <b>IOWorker 串行队列</b>（1.20.4 中由 {@code ChunkStorage.getChunkIoWorker()} 返回的
 * 那一个 worker 负责；一个维度一个 ChunkMap、一个 worker）。
 * 因此 region 文件头（8KB 偏移表）与扇区分配由<b>同一个 worker 顺序处理</b> ——
 * 两个区块即使落在同一 region 文件、被不同分片锁保护，实际写盘也是先后排队执行的，
 * 不存在"两个写者同时扩展扇区、竞争文件头"的场景。
 * <p>
 * 因此：<b>不要</b>为了"MCA 并发写"再引入文件级锁或自研 region writer——那会绕开
 * IOWorker 反而制造真正的双写句柄冲突（这也正是 DESIGN 选择复用原版存储栈的原因，
 * 见 {@code ChunkDataHandlers} 类注释）。若将来有写入路径绕过 {@code ChunkStorage}
 * 直接动 .mca 文件，必须回到这里补文件级串行化。
 */
final class ChunkWriteCoordinator {

    private static final int LOCK_STRIPES = 256;
    private final Object[] locks = new Object[LOCK_STRIPES];

    ChunkWriteCoordinator() {
        java.util.Arrays.setAll(locks, ignored -> new Object());
    }

    <T> T withLock(ChunkKey key, Supplier<T> action) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(action, "action");
        synchronized (lockFor(key)) {
            return action.get();
        }
    }

    /**
     * 在同一区块锁内按“最终写回并 flush -> 释放 ownership”的固定顺序执行。
     * 最终写回失败时绝不调用 release；无数据释放则直接在锁内执行 release。
     */
    ChunkWriteback.ReleaseResult finalizeAndRelease(
            ChunkKey key, boolean hasFinalData,
            BooleanSupplier finalWriter, BooleanSupplier release) {
        Objects.requireNonNull(release, "release");
        return finalizeAndRelease(key, hasFinalData, finalWriter,
                new ChunkWriteback.ReleaseTransaction() {
                    @Override
                    public ChunkWriteback.ReleaseResult replay() {
                        return null;
                    }

                    @Override
                    public ChunkWriteback.ReleaseResult commit(
                            boolean finalDataWritten) {
                        return new ChunkWriteback.ReleaseResult(
                                finalDataWritten, release.getAsBoolean());
                    }
                });
    }

    /**
     * 幂等释放版本：先在区块锁内查询耐久回执，命中时直接重放，完全跳过最终写回；
     * 首次执行才按“写回并 flush → 原子记录回执与删除 ownership”提交。
     */
    ChunkWriteback.ReleaseResult finalizeAndRelease(
            ChunkKey key, boolean hasFinalData,
            BooleanSupplier finalWriter,
            ChunkWriteback.ReleaseTransaction transaction) {
        Objects.requireNonNull(transaction, "transaction");
        return withLock(key, () -> finalizeAndReleaseWithinChunkLock(
                hasFinalData, finalWriter, transaction));
    }

    /**
     * 调用方已经持有该区块的分片锁时执行释放状态机。服务端 WAL 路径需要在区块锁
     * 内再取得批次线性化锁，并让“最终 WAL 写入 + ownership 提交”共同处于该锁内；
     * 单独复用本方法可避免重复取得区块锁，同时保持全局锁序清晰。
     */
    ChunkWriteback.ReleaseResult finalizeAndReleaseWithinChunkLock(
            boolean hasFinalData, BooleanSupplier finalWriter,
            ChunkWriteback.ReleaseTransaction transaction) {
        Objects.requireNonNull(transaction, "transaction");
        ChunkWriteback.ReleaseResult replayed = transaction.replay();
        if (replayed != null) {
            return replayed;
        }
        boolean written = false;
        if (hasFinalData) {
            if (finalWriter == null || !finalWriter.getAsBoolean()) {
                return new ChunkWriteback.ReleaseResult(false, false);
            }
            written = true;
        }
        return transaction.commit(written);
    }

    private Object lockFor(ChunkKey key) {
        return locks[(key.hashCode() & Integer.MAX_VALUE) % locks.length];
    }
}
