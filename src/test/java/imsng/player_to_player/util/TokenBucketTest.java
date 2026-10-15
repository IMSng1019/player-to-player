package imsng.player_to_player.util;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 令牌桶（指令上链限速）行为锁定。
 *
 * <p>限速的语义要求：突发额度内不拒绝（连敲几条指令是正常玩法）、额度耗尽后拒绝、
 * 随时间恢复；且必须用<b>单调钟</b> —— 墙上时钟回拨时按
 * {@code now - last < interval} 判断的实现会静默拒绝一切上送。</p>
 */
class TokenBucketTest {

    /** 容量 3、补充间隔 1 毫秒（测试里用极短间隔观察恢复）。 */
    private static TokenBucket bucket() {
        return new TokenBucket(3, 1_000_000L);
    }

    @Test
    @DisplayName("突发额度内全部放行，耗尽后拒绝")
    void burstThenReject() {
        TokenBucket bucket = bucket();
        assertTrue(bucket.tryAcquire());
        assertTrue(bucket.tryAcquire());
        assertTrue(bucket.tryAcquire());
        assertFalse(bucket.tryAcquire(), "额度耗尽必须拒绝（第 4 次）");
    }

    @Test
    @DisplayName("等待后按间隔补充（恢复取用）")
    void refillsOverTime() throws InterruptedException {
        TokenBucket bucket = bucket();
        assertTrue(bucket.tryAcquire());
        assertTrue(bucket.tryAcquire());
        assertTrue(bucket.tryAcquire());
        assertFalse(bucket.tryAcquire());

        Thread.sleep(20);
        assertTrue(bucket.tryAcquire(), "20ms ≈ 20 枚令牌的补充，应当恢复");
        assertTrue(bucket.tryAcquire());
        assertTrue(bucket.tryAcquire());
        assertFalse(bucket.tryAcquire());
    }

    @Test
    @DisplayName("容量不会因长时间空闲而溢出（补充上限 = 容量）")
    void refillIsCappedAtCapacity() throws InterruptedException {
        TokenBucket bucket = bucket();
        Thread.sleep(30);
        for (int i = 0; i < 3; i++) {
            assertTrue(bucket.tryAcquire(), "第 " + (i + 1) + " 次应在上限内");
        }
        assertFalse(bucket.tryAcquire(), "空闲再久也只能攒回容量那么多");
    }

    @Test
    @DisplayName("参数退化（容量 0 / 间隔 0）不抛异常且仍可放行一次")
    void degenerateParametersAreSafe() {
        assertTrue(new TokenBucket(0, 0).tryAcquire());
        assertTrue(new TokenBucket(-5, -1L).tryAcquire());
    }
}
