package imsng.player_to_player.compute;

import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 服务端算力表：汇总所有在线客户端上报的 {@link ComputeScore}，选主时据此排序。
 * <p>
 * 规范出处：player_to_player-prompt.txt "算力表：显示符合剩余内存大小的玩家电脑的算力
 * 取决于CPU的单核能力"、"算力分配：……算力最高的为主客户端……同时成为主客户段还有一个
 * 条件为目前剩余的内存大小 服务端可以设定剩余的内存大小 默认为0.5GB"；
 * DESIGN.md 第 6 节 "服务端 ComputeTable 汇总所有在线客户端评分，合并/加入世界时据此选主"。
 * <p>
 * <b>线程模型</b>：条目上报来自 Netty 事件循环（{@link ComputeHandlers}），
 * 选主查询可能来自任意后台线程 —— 全部操作基于 ConcurrentHashMap 单键原子读写，无需外部锁。
 * selectPrimary 遍历期间的并发修改是可接受的弱一致（选主本身就是基于"当时快照"的决策）。
 */
public final class ComputeTable {

    /** 在线客户端算力表：clientId → 最近一次上报的评分（重复上报直接覆盖）。 */
    private final Map<UUID, ComputeScore> scores = new ConcurrentHashMap<>();

    /** 记录（或覆盖）某客户端的算力评分；参数为 null 时忽略（入站数据不可信，防御性）。 */
    public void report(UUID clientId, ComputeScore score) {
        if (clientId == null || score == null) {
            return;
        }
        scores.put(clientId, score);
    }

    /** 取某客户端的评分；未上报过返回 null。 */
    public ComputeScore get(UUID clientId) {
        return clientId == null ? null : scores.get(clientId);
    }

    /**
     * 取某客户端当前的选主排序键（{@link #scoreKey}）；未上报算力返回 {@link Long#MIN_VALUE}。
     * 供日志归因/调试展示使用：与内部排序**同源**（都经 {@link #scoreKey} 走
     * {@link ComputeScore#rankScore()}），保证"日志里写的分"就是"决策用的分"。
     */
    public long rankOf(UUID clientId) {
        ComputeScore score = get(clientId);
        return score != null ? scoreKey(score) : Long.MIN_VALUE;
    }

    /** 移除某客户端（断开连接时由服务端调用，防表无限增长）。 */
    public void remove(UUID clientId) {
        if (clientId != null) {
            scores.remove(clientId);
        }
    }

    /**
     * 选主结果（把"选出了谁"与"是不是降级选出"分开表达，调用方据此决定日志与后续策略）。
     *
     * @param clientId      选中的客户端（必然非 null）
     * @param memoryRelaxed true = 候选里<b>没有任何人</b>达到内存门槛，属于放宽门槛的
     *                      "降级选出"：规范"同时成为主客户段还有一个条件为目前剩余的内存大小"
     *                      在此让位于"算力最高的为主客户端" —— 宁可让内存吃紧的机器当主，
     *                      也不能让区块无人 tick；调用方应记 warn 并在策略上区别对待
     *                      （例如加入世界的"更强才接管"比较不应把降级结果当作"更强"）。
     */
    public record Selection(UUID clientId, boolean memoryRelaxed) {
        public Selection {
            if (clientId == null) {
                throw new IllegalArgumentException("clientId 不能为 null");
            }
        }
    }

    /**
     * 在候选集合中选出主客户端，并显式区分"是否降级选出"（规范"算力分配：重新分配此区域中的
     * 玩家的客户端等级（主客户端和副客户端） 通过算力表 算力最高的为主客户端 同时成为主客户段
     * 还有一个条件为目前剩余的内存大小"）。
     * <p>
     * <b>规则（完全确定性：同一输入必然同一输出 —— 合并双方各自独立计算也不会分叉，
     * 这是分布式选主的硬要求）</b>：
     * <ol>
     *   <li>只保留"已上报算力"的候选（{@link ComputeScore} 为 null 视为无资格：连分数都没有，
     *       无从比较，不参与降级）；</li>
     *   <li>存在满足内存门槛者（{@link ComputeScore#meetsMemoryRequirement}）→ 在其中取
     *       {@link #scoreKey} 最大者，{@code memoryRelaxed = false}；</li>
     *   <li>无一人达标 → 在<b>全部已上报候选</b>中取 {@link #scoreKey} 最大者，
     *       {@code memoryRelaxed = true}（把旧 Javadoc 里"调用方自行放宽门槛"的建议
     *       落地为库内确定性行为，消除三个调用方各自兜底、口径不一的问题）；</li>
     *   <li>分数并列一律取 UUID 字典序（{@link UUID#compareTo}）最小者。</li>
     * </ol>
     *
     * @param candidates         候选 clientId 集合（合并/分离场景 = 涉及组的两组<b>全部成员</b>，
     *                           副客户端同样有当选资格）
     * @param minFreeMemoryBytes 主客户端内存门槛（{@code GlobalConfig.minFreeMemoryBytes}）
     * @return 选主结果；候选为空 / 无一人上报过算力（连降级都无从选起）返回 null ——
     *         此时调用方必须自行兜底（例如按 UUID 字典序），并记 warn
     */
    public Selection selectPrimaryOrRelaxed(Collection<UUID> candidates, long minFreeMemoryBytes) {
        if (candidates == null || candidates.isEmpty()) {
            return null;
        }
        UUID bestAny = null;
        ComputeScore bestAnyScore = null;
        UUID bestEligible = null;
        ComputeScore bestEligibleScore = null;
        for (UUID candidate : candidates) {
            if (candidate == null) {
                continue;
            }
            ComputeScore score = scores.get(candidate);
            if (score == null) {
                continue; // 未上报算力：无资格（没有可比的分，不参与降级）
            }
            if (better(score, candidate, bestAnyScore, bestAny)) {
                bestAny = candidate;
                bestAnyScore = score;
            }
            if (score.meetsMemoryRequirement(minFreeMemoryBytes)
                    && better(score, candidate, bestEligibleScore, bestEligible)) {
                bestEligible = candidate;
                bestEligibleScore = score;
            }
        }
        if (bestEligible != null) {
            return new Selection(bestEligible, false);
        }
        // 无人达标：降级选出算力最高者（规范"算力最高的为主客户端"优先于内存门槛）
        return bestAny != null ? new Selection(bestAny, true) : null;
    }

    /**
     * 兼容入口：只关心"选出谁"、不关心是否降级的调用方使用
     * （等价于 {@code selectPrimaryOrRelaxed(...).clientId()}，语义与降级兜底前不同：
     * 内存全员不达标时不再返回 null，而是返回算力最高者）。
     */
    public UUID selectPrimary(Collection<UUID> candidates, long minFreeMemoryBytes) {
        Selection selection = selectPrimaryOrRelaxed(candidates, minFreeMemoryBytes);
        return selection != null ? selection.clientId() : null;
    }

    /**
     * 候选优先级比较：{@link #scoreKey} 大者优先；并列取 UUID 字典序最小者（确定性决胜）。
     * 擂主为 null 时任何候选都优先。
     */
    private static boolean better(ComputeScore score, UUID clientId,
                                  ComputeScore incumbentScore, UUID incumbent) {
        if (incumbent == null || incumbentScore == null) {
            return true;
        }
        long challenger = scoreKey(score);
        long current = scoreKey(incumbentScore);
        if (challenger != current) {
            return challenger > current;
        }
        return clientId.compareTo(incumbent) < 0;
    }

    /**
     * 选主排序键（规范"算力表：……取决于CPU的单核能力"）。
     * <p>
     * 统一走 {@link ComputeScore#rankScore()} —— 它是 compute 包收敛后的<b>唯一</b>可比量纲
     * （本地微基准分优先，Geekbench 参考分仅在本地分缺失时兜底，见 ComputeScore 类 Javadoc），
     * 旧的 {@code singleCoreScore()} 现在只是它的别名。选主排序只经过本方法一处，
     * 量纲策略再变化也不需要碰选主逻辑。
     */
    private static long scoreKey(ComputeScore score) {
        return score.rankScore();
    }

    /** 当前表快照（防御性拷贝，供状态展示/调试指令使用）。 */
    public Map<UUID, ComputeScore> snapshot() {
        return Map.copyOf(scores);
    }
}
