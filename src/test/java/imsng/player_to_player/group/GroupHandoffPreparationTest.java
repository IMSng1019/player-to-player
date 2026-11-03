package imsng.player_to_player.group;

import org.junit.jupiter.api.Test;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import static org.junit.jupiter.api.Assertions.*;

/** 验证退出交接确认与旧连接关闭的顺序，而不依赖图形客户端。 */
class GroupHandoffPreparationTest {
    private final UUID transaction = UUID.randomUUID();
    private final UUID source = UUID.randomUUID();

    @Test
    void acknowledgementRequiresCompletedDisconnectAndRetryDoesNotDisconnectAgain() throws Exception {
        GroupHandoffPreparation state = new GroupHandoffPreparation();
        AtomicInteger disconnects = new AtomicInteger();
        Runnable disconnect = () -> {
            assertFalse(state.isPrepared());
            disconnects.incrementAndGet();
        };
        assertTrue(state.prepare(transaction, source, disconnect));
        assertTrue(state.isPrepared());
        assertTrue(state.prepare(transaction, source, disconnect));
        assertEquals(1, disconnects.get(), "ACK丢失后的重试不能再次关闭游戏连接");
    }

    @Test
    void failedDisconnectCannotBeAcknowledgedAndCanBeRetried() throws Exception {
        GroupHandoffPreparation state = new GroupHandoffPreparation();
        assertThrows(IllegalStateException.class, () -> state.prepare(transaction, source,
                () -> { throw new IllegalStateException("disconnect failed"); }));
        assertFalse(state.isPrepared());
        assertTrue(state.prepare(transaction, source, () -> { }));
    }

    @Test
    void anotherTransactionCannotReplacePendingPreparation() throws Exception {
        GroupHandoffPreparation state = new GroupHandoffPreparation();
        assertTrue(state.prepare(transaction, source, () -> { }));
        assertFalse(state.prepare(UUID.randomUUID(), source,
                () -> fail("不能关闭另一轮交接的世界")));
        assertFalse(state.prepare(transaction, UUID.randomUUID(),
                () -> fail("相同事务号不能用于另一组")));
    }

    @Test
    void clearingSessionDuringDisconnectPreventsLateAcknowledgement() throws Exception {
        GroupHandoffPreparation state = new GroupHandoffPreparation();
        assertFalse(state.prepare(transaction, source, state::clear));
        assertFalse(state.isPrepared());
        assertTrue(state.prepare(UUID.randomUUID(), source, () -> { }));
    }

    @Test
    void preparedSplitCanBeTakenOverByTheSameGroupsPrimaryExit() {
        GroupHandoffPreparation state = new GroupHandoffPreparation();
        UUID split = UUID.randomUUID();
        AtomicInteger transitions = new AtomicInteger();
        assertTrue(state.prepareSplit(split, source, transitions::incrementAndGet));
        assertTrue(state.isPrepared(), "分离已关闭旧连接，后续退出通知到达时没有游戏世界");
        assertTrue(state.prepare(transaction, source, transitions::incrementAndGet),
                "主客户端退出必须接替同源分离等待，不能要求副客户端重新进入旧世界");
        assertTrue(state.prepare(transaction, source, transitions::incrementAndGet));
        assertEquals(2, transitions.get(), "每种交接只执行一次，重试只重发确认");
    }

    @Test
    void lateSplitCannotReplacePrimaryExitOrAnotherGroupsWait() {
        GroupHandoffPreparation state = new GroupHandoffPreparation();
        assertTrue(state.prepareSplit(transaction, source, () -> { }));
        assertFalse(state.prepare(UUID.randomUUID(), UUID.randomUUID(),
                () -> fail("另一个组不能接替当前等待")));
        assertTrue(state.prepare(UUID.randomUUID(), source, () -> { }));
        assertFalse(state.prepareSplit(transaction, source,
                () -> fail("迟到的原分离通知不能覆盖退出交接")));
    }

    @Test
    void primaryExitMustNotAcknowledgeAnIncompleteSplitDisconnect() {
        GroupHandoffPreparation state = new GroupHandoffPreparation();
        assertTrue(state.prepareSplit(UUID.randomUUID(), source, () -> {
            assertFalse(state.prepare(transaction, source,
                    () -> fail("分离关闭旧连接尚未完成，不能提前确认主客户端可停服")));
        }));
    }

    @Test
    void reconnectingAfterSplitConsumesTheEmptyWorldExitAcknowledgement() {
        GroupHandoffPreparation state = new GroupHandoffPreparation();
        UUID split = UUID.randomUUID();
        assertTrue(state.prepareSplit(split, source, () -> { }));
        var intent = state.reserveConnection(split, source);
        assertNotNull(intent);
        assertNull(state.reserveConnection(split, source), "重复角色指派不能再准备另一条隧道");
        assertTrue(state.beginConnecting(intent));
        assertFalse(state.isPrepared(), "半登录并非已断开的等待状态，不能借level=null提前确认停服");
        assertFalse(state.prepare(transaction, source,
                () -> fail("必须等待原版JOIN完成后再进行退出交接")));
        assertTrue(state.isCurrentConnection(intent));
        assertFalse(state.beginConnecting(intent), "重复激活不能启动第二条游戏连接");
        state.clear(); // 原版JOIN完成。
        assertFalse(state.isCurrentConnection(intent));
        assertTrue(state.prepare(transaction, source, () -> { }));
    }

    @Test
    void queuedRollbackCannotReconnectTheOldHostAfterPrimaryExitHasTakenOver() {
        GroupHandoffPreparation state = new GroupHandoffPreparation();
        UUID split = UUID.randomUUID();
        assertTrue(state.prepareSplit(split, source, () -> { }));
        assertTrue(state.prepare(transaction, source, () -> { }));
        assertNull(state.reserveConnection(split, source), "迟到回滚事务不能覆盖新的退出等待");
        assertNull(state.reserveConnection(transaction, source), "无事务号的迟到回滚也不能重连退出中的旧主");
        var intent = state.reserveConnection(transaction, UUID.randomUUID());
        assertNotNull(intent, "仍允许角色指派连接真正的接管方");
        assertTrue(state.beginConnecting(intent));
        state.clear();
        assertFalse(state.isCurrentConnection(intent), "清理后的后台隧道激活不能恢复旧游戏连接");
    }

    @Test
    void exitCanSupersedeTransportPreparationButTheOldQueuedActivationMustBeRejected() {
        GroupHandoffPreparation state = new GroupHandoffPreparation();
        UUID split = UUID.randomUUID();
        assertTrue(state.prepareSplit(split, source, () -> { }));
        var oldIntent = state.reserveConnection(split, source);
        assertTrue(state.isPrepared(), "后台准备隧道尚未建立游戏连接，不应阻挡退出接替");
        assertTrue(state.prepare(transaction, source, () -> { }));
        assertFalse(state.beginConnecting(oldIntent), "旧隧道即使稍后准备完成也不能覆盖新的退出等待");
        assertFalse(state.isCurrentConnection(oldIntent), "迟到失败也不能关闭新的等待");
    }
}
