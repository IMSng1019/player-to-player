package imsng.player_to_player.util;

/**
 * 简易令牌桶限速（单调钟，{@link System#nanoTime()}）。
 *
 * <p>用途：指令上链的客户端入口限速与服务端入口限速 —— 打错指令连打不该刷爆链路，
 * 被改造的客户端也不该把服务端与各组拖进指令风暴。</p>
 *
 * <p>为什么不用 {@code System.currentTimeMillis()}：墙上时钟回拨（NTP 校时、手动改时）
 * 会让"距上次刷新"变成负数，实现若按 {@code now - last < interval} 判断就会在回拨
 * 期间<b>静默拒绝一切</b>。单调钟不受影响。</p>
 *
 * <p>线程模型：单个桶由 {@code synchronized} 串行（临界区只有几次算术），
 * 一个玩家一个桶，无跨玩家竞争。</p>
 */
public final class TokenBucket {

    private final double capacity;
    private final long refillNanos;

    private double tokens;
    private long lastRefillNanos;

    /**
     * @param capacity    突发容量（连续可取用的次数）
     * @param refillNanos 补充一枚令牌的间隔（纳秒）
     */
    public TokenBucket(int capacity, long refillNanos) {
        this.capacity = Math.max(1, capacity);
        this.refillNanos = Math.max(1L, refillNanos);
        this.tokens = this.capacity;
        this.lastRefillNanos = System.nanoTime();
    }

    /** 取一枚令牌；不足返回 false（调用方决定是拒绝还是降级）。 */
    public synchronized boolean tryAcquire() {
        long now = System.nanoTime();
        long elapsed = now - lastRefillNanos;
        if (elapsed > 0) {
            tokens = Math.min(capacity, tokens + (double) elapsed / refillNanos);
            lastRefillNanos = now;
        }
        if (tokens < 1.0) {
            return false;
        }
        tokens -= 1.0;
        return true;
    }
}
