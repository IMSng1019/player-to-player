package imsng.player_to_player.server;

import imsng.player_to_player.registry.ChunkKey;
import imsng.player_to_player.registry.ChunkWriteback;

import java.util.Objects;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

/**
 * 服务端区块写入的分片互斥器。
 * <p>
 * 实时上行、退出事务写入和 CHUNK_RELEASE 最终写回必须使用同一组锁。特别是最终写回
 * 与 ownership 释放不能分成两个临界区，否则迟到实时上行可能在中间插入并覆盖最终快照。
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
        return withLock(key, () -> {
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
        });
    }

    private Object lockFor(ChunkKey key) {
        return locks[(key.hashCode() & Integer.MAX_VALUE) % locks.length];
    }
}
