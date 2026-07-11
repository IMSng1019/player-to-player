package imsng.player_to_player.compute;

import imsng.player_to_player.config.GlobalConfig;
import imsng.player_to_player.util.ThreadPools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;

/**
 * 本机算力评分的统一入口（客户端侧）。
 * <p>
 * 规范出处：player_to_player-prompt.txt "玩家加载模组：模组检查该玩家的cpu得出其单核算力
 * 生成相应的算力数值"；DESIGN.md 第 6 节 "评分来源：① Geekbench 非官方公开接口按 CPU 型号
 * 查询单核分（可配置关闭、失败降级）；② 本地单核微基准"。
 *
 * <h2>检测流水线</h2>
 * <ol>
 *   <li>{@code ThreadPools.compute()} 上取 CPU 型号（{@link CpuInfo}，首次会执行外部进程）
 *       与物理内存快照（{@link MemoryInfo}）；</li>
 *   <li>总配置 {@code geekbenchLookupEnabled} 为 true 时，切到 {@code ThreadPools.io()}
 *       执行 {@link GeekbenchLookup}（同步 HTTP，最长约 5 秒）；关闭则直接跳过；</li>
 *   <li>查询结果为空（断网 / 未收录 / 页面改版 / 已关闭）时回到
 *       {@code ThreadPools.compute()} 跑 {@link LocalBenchmark} 兜底（约 1.7 秒满载单核）；</li>
 *   <li>组装 {@link ComputeScore}，source 相应取
 *       {@link ComputeScore#SOURCE_GEEKBENCH} / {@link ComputeScore#SOURCE_LOCAL}。</li>
 * </ol>
 *
 * <h2>缓存语义</h2>
 * CPU 型号与单核能力整机运行期不变，<b>整机只测一次</b>：首次调用发起检测并缓存
 * CompletableFuture（volatile + 双检锁），并发调用共享同一次进行中的检测；
 * 检测异常完成时清空缓存允许下次重试（正常情况下流水线各环节均自带降级、不会异常）。
 * 注意：缓存以首次调用者传入的 config 为准，后续调用的 config 参数不再生效 ——
 * 调用方（P2PBootstrap）在模组加载初期即触发检测，配置此后不变，语义安全。
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
                        LOGGER.info("本机算力评分: cpu={} singleCore={} source={} freeMem={}MB totalMem={}MB",
                                score.cpuModel(), score.singleCoreScore(), score.source(),
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
            // 第 1 步（compute 池）：CPU 型号（首次阻塞执行外部进程）+ 内存快照
            String cpuModel = CpuInfo.cpuModel();
            long freeMemory = MemoryInfo.freeMemoryBytes();
            long totalMemory = MemoryInfo.totalMemoryBytes();
            return new HardwareSnapshot(cpuModel, freeMemory, totalMemory);
        }, ThreadPools.compute()).thenCompose(hw -> {
            // 第 2 步（io 池）：Geekbench 网络查询；关闭时直接给 empty 走降级
            CompletableFuture<OptionalLong> lookup = geekbenchEnabled
                    ? CompletableFuture.supplyAsync(
                            () -> GeekbenchLookup.lookupSingleCore(hw.cpuModel), ThreadPools.io())
                    : CompletableFuture.completedFuture(OptionalLong.empty());
            // 第 3 步（compute 池）：查询为空则本地跑分兜底，随后组装结果
            return lookup.thenApplyAsync(geekbenchScore -> {
                if (geekbenchScore.isPresent()) {
                    return new ComputeScore(hw.cpuModel, geekbenchScore.getAsLong(),
                            ComputeScore.SOURCE_GEEKBENCH, hw.freeMemory, hw.totalMemory);
                }
                // 降级路径：Geekbench 查不到是常态（无 SLA），本地微基准保证任何机器都有分
                long localScore = LocalBenchmark.runSingleCoreScore();
                return new ComputeScore(hw.cpuModel, localScore,
                        ComputeScore.SOURCE_LOCAL, hw.freeMemory, hw.totalMemory);
            }, ThreadPools.compute());
        });
    }

    /** 硬件信息快照（流水线第 1 步产物，在后续阶段间传递）。 */
    private record HardwareSnapshot(String cpuModel, long freeMemory, long totalMemory) {
    }
}
