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
}
