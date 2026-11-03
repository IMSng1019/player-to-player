package imsng.player_to_player.group;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * 分离候选的连续距离资格与准备重试节流，全部由服务器主线程使用。
 * 达到十秒只获得准备资格，不代表元数据/区块边界已经可迁移。暂时延期或 prepare
 * 被拒时保留距离资格，避免每次短暂繁忙都重新等待十秒；拓扑变化或事务结束才重置。
 */
final class SplitCandidateTracker {
    private final long holdMillis;
    private final long retryMillis;
    private final Map<String, Candidate> candidates = new HashMap<>();

    private static final class Candidate {
        final long since;
        long observedAt;
        long attemptedAt;
        boolean attempted;

        Candidate(long now) {
            since = now;
            observedAt = now;
        }
    }

    SplitCandidateTracker(long holdMillis, long retryMillis) {
        if (holdMillis <= 0 || retryMillis <= 0) throw new IllegalArgumentException("分离等待与重试间隔必须为正");
        this.holdMillis = holdMillis;
        this.retryMillis = retryMillis;
    }

    /** 首次达标和后续短重试共用入口；调用成功即占用一次重试时隙，防止同步失败忙循环。 */
    boolean tryBegin(String signature, long now) {
        Candidate candidate = candidates.computeIfAbsent(signature, ignored -> new Candidate(now));
        if (now < candidate.observedAt) {
            // 生产传入单调时钟；若调用方重置时钟或极端回绕，则重新采样，不能永久等旧截止。
            candidate = new Candidate(now);
            candidates.put(signature, candidate);
        }
        candidate.observedAt = now;
        if (now - candidate.since < holdMillis) return false;
        if (candidate.attempted && now - candidate.attemptedAt < retryMillis) return false;
        candidate.attempted = true;
        candidate.attemptedAt = now;
        return true;
    }

    /** 每轮传入全部离组分量签名；接近、成员变化或回到主客户端分量都会移除旧资格。 */
    void retainAll(Set<String> signatures) {
        candidates.keySet().retainAll(signatures);
    }

    void clear() {
        candidates.clear();
    }
}
