package imsng.player_to_player.compute;

import imsng.player_to_player.config.GlobalConfig;
import imsng.player_to_player.util.ThreadPools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;

/**
 * 本机算力评分的统一入口（客户端侧）。
 * <p>
 * 规范出处：player_to_player-prompt.txt "玩家加载模组：模组检查该玩家的cpu得出其单核算力
 * 生成相应的算力数值"；DESIGN.md 第 6 节 "评分来源：① Geekbench 非官方公开接口按 CPU 型号
 * 查询单核分（可配置关闭、失败降级）；② 本地单核微基准"。
 *
 * <h2>检测流水线（顺序固定，先跑分再联网）</h2>
 * <ol>
 *   <li>{@code ThreadPools.compute()}：CPU 型号（{@link CpuInfo}，首次会执行外部进程）
 *       与整机内存快照（{@link MemoryInfo}）；</li>
 *   <li>{@code ThreadPools.compute()}：<b>无条件</b>执行 {@link LocalBenchmark}
 *       （约 1.7 秒单核满载），得到算力表的<b>排序键</b>
 *       （见 {@link ComputeScore#rankScore()} 与 ComputeScore 类 Javadoc）；</li>
 *   <li>{@code ThreadPools.io()}：总配置 {@code geekbenchLookupEnabled} 为 true 时执行
 *       {@link GeekbenchLookup}（同步 HTTP，最长约 5 秒）取<b>参考分</b>；
 *       关闭 / 失败 / 未收录都只是"没有参考分"，<b>不影响第 2 步的本地分</b>；</li>
 *   <li>组装 {@link ComputeScore}（本地分 + 可选参考分），一次检测结束。</li>
 * </ol>
 *
 * <h2>为什么先跑分、再联网，而不是并行</h2>
 * 本地微基准测的是单核吞吐：如果与同步 HTTP（TLS 握手、页面下载与解析）同时进行，
 * 网络线程、GC 会争抢同一个物理核，把跑分压低 —— 而"逐字节相同的确定性负载"
 * 最怕的就是这种噪声（它直接决定选主结论）。串行的代价只是启动时多等几秒，
 * 且全程在后台线程，不阻塞 MC 主线程；实测流水线总耗时约 2~7 秒。
 *
 * <h2>缓存语义</h2>
 * CPU 型号、单核能力与内存快照在整机运行期视为不变，<b>整机只测一次</b>：
 * 首次调用发起检测并缓存 CompletableFuture（volatile + 双检锁），并发调用共享
 * 同一次进行中的检测；检测异常完成时清空缓存允许下次重试（正常情况下流水线各环节
 * 均自带降级、不会异常）。
 * 注意：缓存以首次调用者传入的 config 为准，后续调用的 config 参数不再生效 ——
 * 调用方（P2PBootstrap）在模组加载初期即触发检测，配置此后不变，语义安全。
 * <p>
 * <b>内存快照的含义</b>：{@link ComputeScore#freeMemoryBytes()} 是"模组加载那一刻"的
 * 整机可用物理内存，之后<b>不再刷新</b>（本类缓存了整份评分）；主客户端门槛由服务端
 * {@link GlobalConfig#minFreeMemoryBytes}（默认 0.5 GB）判定，客户端只负责上报该快照。
 * <p>
 * <b>线程模型</b>：本方法自身不阻塞（立即返回 future），可在任意线程调用；
 * 全部重活都在 compute()/io() 池内完成，符合 DESIGN.md 第 9 节铁律。
 */
public final class ComputeScoreProvider {

    private static final Logger LOGGER = LoggerFactory.getLogger("player_to_player/compute");

    /** 检测结果缓存（缓存 future 而非结果，使并发首调也只触发一次检测）。 */
    private static volatile CompletableFuture<ComputeScore> cached;

    private ComputeScoreProvider() {
    }

    /**
     * 检测（或取缓存的）本机算力评分。
     *
     * @param config 总配置（读 geekbenchLookupEnabled 开关；仅首次调用生效）
     * @return 异步评分结果；流水线自带降级，正常情况下必然正常完成
     */
    public static CompletableFuture<ComputeScore> detect(GlobalConfig config) {
        CompletableFuture<ComputeScore> local = cached;
        if (local != null) {
            return local;
        }
        synchronized (ComputeScoreProvider.class) {
            if (cached == null) {
                cached = runPipeline(config);
                // 异常完成时清缓存：下次调用可重试（防御性，正常不会走到）
                cached.whenComplete((score, error) -> {
                    if (error != null) {
                        LOGGER.warn("算力检测异常完成（已清缓存，下次调用重试）", error);
                        synchronized (ComputeScoreProvider.class) {
                            cached = null;
                        }
                    } else {
                        LOGGER.info("本机算力评分: cpu=\"{}\" 排序键={} (本地分={} / geekbench参考分={} {}) "
                                        + "source={} freeMem={}MB totalMem={}MB",
                                score.cpuModel(), score.rankScore(),
                                score.localSingleCoreScore(), score.geekbenchSingleCoreScore(),
                                score.geekbenchVersion(), score.source(),
                                score.freeMemoryBytes() / (1024 * 1024),
                                score.totalMemoryBytes() / (1024 * 1024));
                    }
                });
            }
            return cached;
        }
    }

    /** 组装检测流水线（见类 Javadoc）。 */
    private static CompletableFuture<ComputeScore> runPipeline(GlobalConfig config) {
        // 开关快照：在调用线程上先读定，避免流水线执行期间配置对象被替换产生歧义
        boolean geekbenchEnabled = config != null && config.geekbenchLookupEnabled;

        return CompletableFuture.supplyAsync(() -> {
            // 第 1 步（compute 池）：CPU 型号（首次阻塞执行外部进程）+ 整机内存快照
            String cpuModel = CpuInfo.cpuModel();
            long freeMemory = MemoryInfo.freeMemoryBytes();
            long totalMemory = MemoryInfo.totalMemoryBytes();
            // 第 2 步（仍在同一个 compute 任务里，紧接第 1 步）：本地微基准 —— 排序键。
            // 放在网络查询之前执行，避免 HTTP/GC 抢占物理核污染跑分（见类 Javadoc）。
            long localScore = LocalBenchmark.runSingleCoreScore();
            return new HardwareSnapshot(cpuModel, localScore, freeMemory, totalMemory);
        }, ThreadPools.compute()).thenCompose(hw -> {
            // 第 3 步（io 池）：Geekbench 参考分；关闭时直接给 NONE，不创建 HttpClient
            CompletableFuture<GeekbenchLookup.Reference> reference = geekbenchEnabled
                    ? CompletableFuture.supplyAsync(
                            () -> lookupReferenceSafely(hw.cpuModel), ThreadPools.io())
                    : CompletableFuture.completedFuture(GeekbenchLookup.Reference.NONE);
            // 第 4 步：组装（纯字段写入、无阻塞，直接在完成线程上执行即可）
            return reference.thenApply(ref -> new ComputeScore(
                    hw.cpuModel,
                    hw.localScore,
                    ref.score(),
                    ref.version(),
                    hw.freeMemory,
                    hw.totalMemory));
        });
    }

    /**
     * 参考分查询的兜底包装。
     * <p>
     * {@link GeekbenchLookup#lookupReference} 内部已把一切失败收敛为
     * {@link GeekbenchLookup.Reference#NONE}，这里再兜一层：即使查询抛出未预料的
     * 运行时异常（例如解析逻辑 bug），也只损失参考分，绝不连累本地分与整条流水线 ——
     * "Geekbench 失败不影响选主"是本任务的硬要求。
     */
    private static GeekbenchLookup.Reference lookupReferenceSafely(String cpuModel) {
        try {
            return GeekbenchLookup.lookupReference(cpuModel);
        } catch (Exception e) {
            LOGGER.debug("Geekbench 参考分查询异常（忽略，仅损失参考分）: cpu={}", cpuModel, e);
            return GeekbenchLookup.Reference.NONE;
        }
    }

    /**
     * 硬件与跑分快照（流水线前两步产物，在后续阶段间传递）。
     *
     * @param cpuModel     CPU 型号
     * @param localScore   本地微基准单核分（排序键，总是有效）
     * @param freeMemory   检测时刻整机可用物理内存（字节）
     * @param totalMemory  检测时刻整机总物理内存（字节）
     */
    private record HardwareSnapshot(String cpuModel, long localScore,
                                    long freeMemory, long totalMemory) {
    }
}
