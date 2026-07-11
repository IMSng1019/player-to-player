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

    /** 移除某客户端（断开连接时由服务端调用，防表无限增长）。 */
    public void remove(UUID clientId) {
        if (clientId != null) {
            scores.remove(clientId);
        }
    }

    /**
     * 在候选集合中选出主客户端。
     * <p>
     * 规则（规范"算力分配"）：先过滤 {@link ComputeScore#meetsMemoryRequirement}
     * （剩余内存 ≥ 服务端阈值，默认 0.5GB），再取 {@code singleCoreScore} 最大者；
     * 分数并列时按 UUID 字典序（{@link UUID#compareTo}）取最小者 ——
     * 保证同一输入永远选出同一主客户端（确定性对合并流程至关重要：
     * 两侧独立计算选主结果也不会分叉）。
     *
     * @param candidates         候选 clientId 集合（通常为一个组或待合并两组的全部成员）
     * @param minFreeMemoryBytes 主客户端内存门槛（{@code GlobalConfig.minFreeMemoryBytes}）
     * @return 选中的 clientId；无合格者（候选为空 / 均未上报 / 均不满足内存门槛）返回 null。
     *         <b>调用方兜底策略</b>：返回 null 时不应让组无主 —— 建议放宽内存门槛在候选中
     *         直接取分数最高者（宁可让内存吃紧的机器当主，也不能让区块无人 tick），
     *         并记 warn 日志提示玩家内存不足。
     */
    public UUID selectPrimary(Collection<UUID> candidates, long minFreeMemoryBytes) {
        if (candidates == null || candidates.isEmpty()) {
            return null;
        }
        UUID best = null;
        ComputeScore bestScore = null;
        for (UUID candidate : candidates) {
            if (candidate == null) {
                continue;
            }
            ComputeScore score = scores.get(candidate);
            // 未上报算力 / 内存不达标者直接出局
            if (score == null || !score.meetsMemoryRequirement(minFreeMemoryBytes)) {
                continue;
            }
            if (bestScore == null
                    || score.singleCoreScore() > bestScore.singleCoreScore()
                    // 并列时 UUID 字典序最小者胜出（确定性，见方法 Javadoc）
                    || (score.singleCoreScore() == bestScore.singleCoreScore()
                            && candidate.compareTo(best) < 0)) {
                best = candidate;
                bestScore = score;
            }
        }
        return best;
    }

    /** 当前表快照（防御性拷贝，供状态展示/调试指令使用）。 */
    public Map<UUID, ComputeScore> snapshot() {
        return Map.copyOf(scores);
    }
}
