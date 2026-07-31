package imsng.player_to_player.group;

import imsng.player_to_player.registry.ChunkKey;
import imsng.player_to_player.util.ThreadPools;
import net.minecraft.nbt.CompoundTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * 主客户端区块预卸载状态机。
 * <p>
 * 原版决定卸载区块时，本服务先建立一个按 {@link ChunkKey} 去重的异步屏障：服务端完成
 * 最终数据写回并确认释放所有权前，屏障绝不成功完成。后续 Mixin 会把该 future 接入
 * {@code ChunkHolder.addSaveDependency}，因此无需取消或重写原版卸载算法。
 * <p>
 * 是否携带最终 NBT 由三个条件共同决定：区块仍带原版 unsaved 标记、存在实时未确认版本，
 * 或本次 claim 时物理服没有数据基线。三者都为 false 时只发送无数据释放，避免重复传输。
 */
public final class ChunkPreUnloadService {

    private static final Logger LOGGER =
            LoggerFactory.getLogger("player_to_player/chunk-pre-unload");

    private final Predicate<ChunkKey> claimedPredicate;
    private final Predicate<ChunkKey> unacknowledgedPredicate;
    private final Predicate<ChunkKey> initialWritePredicate;
    private final BiFunction<ChunkKey, byte[], CompletableFuture<Void>> releaser;
    private final Consumer<ChunkKey> uploadForgetter;
    private final Function<CompoundTag, byte[]> compressor;
    private final Executor compressionExecutor;
    private final Consumer<Runnable> retryScheduler;

    private final Object lock = new Object();
    private final Map<ChunkKey, CompletableFuture<Void>> barriers = new HashMap<>();
    /** 仅统计本地快照/压缩阶段失败；网络重试由 ChunkClaimClient 单独采样记录。 */
    private final Map<ChunkKey, Integer> localRetryCounts = new HashMap<>();

    /** 已开始退出准备；仍允许正在形成的原版卸载加入屏障集合。 */
    private boolean exitPreparing;

    /** awaitQuiescent 已请求在屏障清空的同一临界区内完成冻结。 */
    private boolean freezeRequested;

    /** 冻结后不再创建普通释放屏障，剩余区块由退出事务统一处理。 */
    private boolean exitFrozen;

    private boolean shutdown;
    private CompletableFuture<Void> quiescentFuture;

    /** 生产构造器：直接连接可靠 claim/release 状态与实时上行状态。 */
    public ChunkPreUnloadService(ChunkClaimClient claims, ChunkUploadService uploads) {
        this(claims::isClaimed,
                uploads::hasUnacknowledged,
                claims::requiresInitialWrite,
                claims::releaseWhenConfirmed,
                uploads::forget,
                ChunkUploadService::compress,
                ThreadPools.io(),
                task -> ThreadPools.scheduler().schedule(task, 1, TimeUnit.SECONDS));
    }

    /**
     * 包可见测试构造器。依赖均以行为接口注入，使并发状态机无需启动 Minecraft 或 Netty。
     */
    ChunkPreUnloadService(
            Predicate<ChunkKey> claimedPredicate,
            Predicate<ChunkKey> unacknowledgedPredicate,
            Predicate<ChunkKey> initialWritePredicate,
            BiFunction<ChunkKey, byte[], CompletableFuture<Void>> releaser,
            Consumer<ChunkKey> uploadForgetter,
            Function<CompoundTag, byte[]> compressor,
            Executor compressionExecutor) {
        this(claimedPredicate, unacknowledgedPredicate, initialWritePredicate,
                releaser, uploadForgetter, compressor, compressionExecutor,
                task -> ThreadPools.scheduler().schedule(task, 1, TimeUnit.SECONDS));
    }

    ChunkPreUnloadService(
            Predicate<ChunkKey> claimedPredicate,
            Predicate<ChunkKey> unacknowledgedPredicate,
            Predicate<ChunkKey> initialWritePredicate,
            BiFunction<ChunkKey, byte[], CompletableFuture<Void>> releaser,
            Consumer<ChunkKey> uploadForgetter,
            Function<CompoundTag, byte[]> compressor,
            Executor compressionExecutor,
            Consumer<Runnable> retryScheduler) {
        this.claimedPredicate = Objects.requireNonNull(claimedPredicate, "claimedPredicate");
        this.unacknowledgedPredicate = Objects.requireNonNull(
                unacknowledgedPredicate, "unacknowledgedPredicate");
        this.initialWritePredicate = Objects.requireNonNull(
                initialWritePredicate, "initialWritePredicate");
        this.releaser = Objects.requireNonNull(releaser, "releaser");
        this.uploadForgetter = Objects.requireNonNull(uploadForgetter, "uploadForgetter");
        this.compressor = Objects.requireNonNull(compressor, "compressor");
        this.compressionExecutor = Objects.requireNonNull(
                compressionExecutor, "compressionExecutor");
        this.retryScheduler = Objects.requireNonNull(retryScheduler, "retryScheduler");
    }

    /**
     * 建立或复用一个区块的预卸载屏障。
     *
     * @param key              维度与区块坐标
     * @param unsaved          原版最终 ChunkAccess 的 unsaved 状态
     * @param snapshotSupplier 异步取得最终不可变 NBT；生产路径每次都调度到服务器主线程
     * @return 正 ACK 后完成的共享 future；退出已冻结或本地已无 claim 时立即成功
     */
    public CompletableFuture<Void> begin(
            ChunkKey key, boolean unsaved,
            Supplier<CompletableFuture<CompoundTag>> snapshotSupplier) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(snapshotSupplier, "snapshotSupplier");

        CompletableFuture<Void> barrier;
        synchronized (lock) {
            if (shutdown) {
                return CompletableFuture.failedFuture(
                        new IllegalStateException("预卸载服务已关停"));
            }
            if (exitFrozen || !claimedPredicate.test(key)) {
                // 退出事务已经固定剩余所有权，或该区块早已释放/迁移；两种情况都不应
                // 再发送普通 CHUNK_RELEASE，也不能给原版卸载挂一个永远无意义的依赖。
                return CompletableFuture.completedFuture(null);
            }
            barrier = barriers.get(key);
            if (barrier != null) {
                return barrier;
            }
            barrier = new CompletableFuture<>();
            barriers.put(key, barrier);
        }

        attempt(key, unsaved, snapshotSupplier, barrier);
        return barrier;
    }

    /** 执行一次本地快照/压缩，再交给 ChunkClaimClient 的可靠释放循环。 */
    private void attempt(ChunkKey key, boolean unsaved,
                         Supplier<CompletableFuture<CompoundTag>> snapshotSupplier,
                         CompletableFuture<Void> barrier) {
        synchronized (lock) {
            if (barriers.get(key) != barrier || shutdown) {
                return;
            }
        }

        try {
            boolean needsFinalData = unsaved
                    || unacknowledgedPredicate.test(key)
                    || initialWritePredicate.test(key);
            CompletableFuture<byte[]> payloadFuture;
            if (needsFinalData) {
                // ChunkSerializer.write 必须在集成服务端主线程执行。supplier 每次尝试都返回
                // 一个由 GroupRuntime 调度到服务器线程的 future；gzip 随后转后台线程。
                CompletableFuture<CompoundTag> snapshotFuture = Objects.requireNonNull(
                        snapshotSupplier.get(), "最终区块快照 future 为空");
                payloadFuture = snapshotFuture.thenApplyAsync(snapshot -> {
                    Objects.requireNonNull(snapshot, "最终区块快照为空");
                    byte[] compressed = compressor.apply(snapshot);
                    if (compressed == null || compressed.length == 0) {
                        throw new IllegalStateException("最终区块 NBT 压缩失败");
                    }
                    return compressed;
                }, compressionExecutor);
            } else {
                payloadFuture = CompletableFuture.completedFuture(null);
            }

            payloadFuture.thenCompose(payload -> {
                CompletableFuture<Void> release = releaser.apply(key, payload);
                return Objects.requireNonNull(release, "区块释放 future 为空");
            }).whenComplete((ignored, error) -> {
                if (error == null) {
                    completeSuccessfully(key, barrier);
                    return;
                }
                retryAfterLocalFailure(key, unsaved, snapshotSupplier, barrier, error);
            });
        } catch (Throwable error) {
            retryAfterLocalFailure(key, unsaved, snapshotSupplier, barrier, error);
        }
    }

    /**
     * 正常网络失败由 ChunkClaimClient 内部无限重试；这里兜底处理快照、压缩或生命周期竞态，
     * 同样不把屏障降级为成功。只要仍持有 claim，就重新取得最新快照再尝试。
     */
    private void retryAfterLocalFailure(
            ChunkKey key, boolean unsaved,
            Supplier<CompletableFuture<CompoundTag>> snapshotSupplier,
            CompletableFuture<Void> barrier, Throwable error) {
        int retryCount;
        synchronized (lock) {
            if (barriers.get(key) != barrier || shutdown) {
                return;
            }
            retryCount = localRetryCounts.merge(key, 1, Integer::sum);
        }
        if (!claimedPredicate.test(key)) {
            // 释放 future 可能因外部合并迁移而异常结束；服务端所有权已经不属于本组时，
            // 本地无需再次释放，按成功清理实时状态即可。
            completeSuccessfully(key, barrier);
            return;
        }
        if (retryCount == 1 || retryCount % 30 == 0) {
            LOGGER.warn("区块预卸载本地阶段第 {} 次失败，1 秒后重试: {}",
                    retryCount, key.asString(), error);
        }
        try {
            retryScheduler.accept(() -> attempt(key, unsaved, snapshotSupplier, barrier));
        } catch (RuntimeException schedulingError) {
            // scheduler 短暂拒绝时保持屏障未完成；使用公共 scheduler 再做一次兜底，避免
            // 因单次 RejectedExecutionException 永久失去重试任务。
            ThreadPools.scheduler().schedule(
                    () -> attempt(key, unsaved, snapshotSupplier, barrier),
                    1, TimeUnit.SECONDS);
        }
    }

    /** 服务端正 ACK 后清理实时未确认状态，再完成原版 save dependency。 */
    private void completeSuccessfully(ChunkKey key, CompletableFuture<Void> barrier) {
        CompletableFuture<Void> quiescent = null;
        int localRetries;
        synchronized (lock) {
            if (!barriers.remove(key, barrier)) {
                return;
            }
            localRetries = localRetryCounts.getOrDefault(key, 0);
            localRetryCounts.remove(key);
            if (freezeRequested && barriers.isEmpty()) {
                // 与 begin 使用同一把锁：从“最后一个屏障移除”到“禁止新屏障”之间没有窗口。
                exitFrozen = true;
                quiescent = quiescentFuture;
                quiescentFuture = null;
            }
        }

        try {
            uploadForgetter.accept(key);
        } catch (RuntimeException error) {
            // 所有权已经由服务端确认释放，不能重新发送 release。遗忘操作只是本地内存清理，
            // 记录异常后仍完成屏障；后续上行还有 claimedPredicate 作为第二道过滤。
            LOGGER.warn("区块释放已确认，但清理实时上传状态失败: {}", key.asString(), error);
        }
        barrier.complete(null);
        if (quiescent != null) {
            quiescent.complete(null);
        }
        if (localRetries > 0) {
            LOGGER.info("区块预卸载在 {} 次本地重试后确认: {}",
                    localRetries, key.asString());
        } else {
            LOGGER.debug("区块预卸载已确认: {}", key.asString());
        }
    }

    /** 进入退出准备；停止新 claim 由 ChunkClaimClient 负责，本服务仍接纳并追踪卸载竞态。 */
    public void beginExitPreparation() {
        synchronized (lock) {
            exitPreparing = true;
        }
    }

    /**
     * 等待全部普通预卸载完成，并在最后一个屏障移除的同一临界区内冻结新屏障。
     * 返回 future 完成后，调用方可以安全固定 ChunkClaimClient 的剩余 ownership manifest。
     */
    public CompletableFuture<Void> awaitQuiescent() {
        synchronized (lock) {
            exitPreparing = true;
            freezeRequested = true;
            if (exitFrozen) {
                return CompletableFuture.completedFuture(null);
            }
            if (barriers.isEmpty()) {
                exitFrozen = true;
                return CompletableFuture.completedFuture(null);
            }
            if (quiescentFuture == null) {
                quiescentFuture = new CompletableFuture<>();
            }
            return quiescentFuture;
        }
    }

    /** 显式冻结入口；只有没有在途屏障时才允许，主要供生命周期防御性调用。 */
    public void freezeForExit() {
        CompletableFuture<Void> quiescent;
        synchronized (lock) {
            if (!barriers.isEmpty()) {
                throw new IllegalStateException("仍有区块预卸载未确认，不能冻结退出清单");
            }
            exitPreparing = true;
            freezeRequested = true;
            exitFrozen = true;
            quiescent = quiescentFuture;
            quiescentFuture = null;
        }
        if (quiescent != null) {
            quiescent.complete(null);
        }
    }

    /** 解除运行时接管时终止本地状态；正常路径应先由退出事务完成全部屏障。 */
    public void shutdown() {
        Map<ChunkKey, CompletableFuture<Void>> active;
        CompletableFuture<Void> quiescent;
        synchronized (lock) {
            shutdown = true;
            exitFrozen = true;
            active = new HashMap<>(barriers);
            barriers.clear();
            localRetryCounts.clear();
            quiescent = quiescentFuture;
            quiescentFuture = null;
        }
        IllegalStateException error = new IllegalStateException("预卸载服务已关停");
        active.values().forEach(future -> future.completeExceptionally(error));
        if (quiescent != null) {
            quiescent.completeExceptionally(error);
        }
    }
}
