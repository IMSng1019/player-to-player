package imsng.player_to_player.group;

import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/** 分离资格与准备重试分开：元数据或卸载暂忙不能抹掉已经持续十秒的距离条件。 */
class SplitCandidateTrackerTest {
    private final SplitCandidateTracker tracker = new SplitCandidateTracker(10_000, 2_000);

    @Test
    void temporaryPreparationFailureRetriesWithoutRepeatingTheTenSecondHold() {
        assertFalse(tracker.tryBegin("a|b", 0));
        assertFalse(tracker.tryBegin("a|b", 9_999));
        assertTrue(tracker.tryBegin("a|b", 10_000));
        // 模拟本次元数据尚在持久化/完整加载岛暂时没有排空；没有成功开启事务。
        assertFalse(tracker.tryBegin("a|b", 10_001), "准备不能每次轮询都重新发送");
        assertFalse(tracker.tryBegin("a|b", 11_999));
        assertTrue(tracker.tryBegin("a|b", 12_000), "连续远离资格仍有效，只等待短重试间隔");
        assertTrue(tracker.tryBegin("a|b", 14_000), "连续暂忙也不能重新要求十秒远离");
    }

    @Test
    void approachingOrChangingMembersRequiresANewContinuousHold() {
        assertFalse(tracker.tryBegin("a", 0));
        assertTrue(tracker.tryBegin("a", 10_000));
        tracker.retainAll(Set.of()); // 本轮渲染区域重新相交，没有离组候选。
        assertFalse(tracker.tryBegin("a", 11_000));
        assertFalse(tracker.tryBegin("a", 20_999));
        assertTrue(tracker.tryBegin("a", 21_000));

        tracker.retainAll(Set.of("a|b")); // 新成员加入该分量，签名变化。
        assertFalse(tracker.tryBegin("a|b", 22_000));
        assertFalse(tracker.tryBegin("a|b", 31_999));
        assertTrue(tracker.tryBegin("a|b", 32_000));
    }

    @Test
    void otherComponentsProgressIndependentlyAndCompletedTransactionsResetTheHold() {
        assertFalse(tracker.tryBegin("a", 0));
        assertFalse(tracker.tryBegin("b", 5_000));
        assertTrue(tracker.tryBegin("a", 10_000));
        tracker.retainAll(Set.of("b"));
        assertFalse(tracker.tryBegin("b", 14_999));
        assertTrue(tracker.tryBegin("b", 15_000));

        tracker.clear(); // 已提交或已回滚的真实分离事务，后续必须重新采样拓扑。
        assertFalse(tracker.tryBegin("b", 15_001));
        assertFalse(tracker.tryBegin("b", 25_000));
        assertTrue(tracker.tryBegin("b", 25_001));
    }

    @Test
    void backwardsClockRestartsTheHoldInsteadOfLeavingAnUnreachableDeadline() {
        assertFalse(tracker.tryBegin("a", 20_000));
        assertFalse(tracker.tryBegin("a", 25_000));
        assertFalse(tracker.tryBegin("a", 1_000));
        assertFalse(tracker.tryBegin("a", 10_999));
        assertTrue(tracker.tryBegin("a", 11_000));
    }

    @Test
    void approachingWhilePrepareReplyIsPendingCannotReuseEarlierEligibility() {
        assertFalse(tracker.tryBegin("a", 0));
        assertTrue(tracker.tryBegin("a", 10_000));
        // prepare请求在途期间只采样拓扑，不启动另一笔事务。
        tracker.retainAll(Set.of("a"));
        tracker.retainAll(Set.of()); // 玩家临时靠近，必须立刻清除原有资格。
        tracker.retainAll(Set.of("a")); // 再次远离后prepare才返回暂忙。
        assertFalse(tracker.tryBegin("a", 13_000));
        assertFalse(tracker.tryBegin("a", 22_999));
        assertTrue(tracker.tryBegin("a", 23_000));
    }

    @Test
    void shortRetriesLeaveTheRecurringCheckpointBusyWindow() {
        int attempts = 0;
        boolean prepared = false;
        // 模拟中心每秒前100ms正在提交；首次分离准备恰落在这个窗口。
        for (int tick = GroupServerHooks.SPLIT_CHECK_INTERVAL_TICKS; tick < 400;
             tick += GroupServerHooks.SPLIT_CHECK_INTERVAL_TICKS) {
            long now = 500L + tick * 50L;
            if (!tracker.tryBegin("a", now)) continue;
            attempts++;
            if (now % 1_000 >= 100) {
                prepared = true;
                break;
            }
        }
        assertTrue(prepared, "不能与每20tick检查点永久同相位，短重试必须有机会成功");
        assertEquals(2, attempts, "首次忙后短重试通过，不重新等待十秒或每tick忙试");
    }
}
