package imsng.player_to_player.group;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

/**
 * {@link GroupPublicationGate} 的生命周期测试。
 *
 * <p>发布门控跨越集成服务端线程和客户端主线程：服务端线程只登记待发布实例，
 * 客户端 tick 在玩家、游戏连接及组运行时全部就绪后一次性消费。这里使用普通对象
 * 模拟集成服务端实例，重点验证引用身份和一次性消费语义。</p>
 */
class GroupPublicationGateTest {

    @Test
    void doesNotConsumeWhileClientReadinessIsIncomplete() {
        GroupPublicationGate<Object> gate = new GroupPublicationGate<>();
        Object server = new Object();
        gate.arm(server);

        assertNull(gate.takeIfReady(server, false, true, ignored -> true));
        assertNull(gate.takeIfReady(server, true, false, ignored -> true));

        // 前两次检查只是在等待条件，不得提前丢失待发布的服务器实例。
        assertSame(server, gate.takeIfReady(server, true, true, ignored -> true));
    }

    @Test
    void consumesReadyServerExactlyOnce() {
        GroupPublicationGate<Object> gate = new GroupPublicationGate<>();
        Object server = new Object();
        gate.arm(server);

        assertSame(server, gate.takeIfReady(server, true, true, ignored -> true));
        assertNull(gate.takeIfReady(server, true, true, ignored -> true));
    }

    @Test
    void ignoresAStaleCurrentServerInstance() {
        GroupPublicationGate<Object> gate = new GroupPublicationGate<>();
        Object pendingServer = new Object();
        gate.arm(pendingServer);

        assertNull(gate.takeIfReady(new Object(), true, true, ignored -> true));

        // 客户端重新指向原待发布实例时仍可消费，说明过期实例检查没有误清门控。
        assertSame(pendingServer,
                gate.takeIfReady(pendingServer, true, true, ignored -> true));
    }

    @Test
    void waitsUntilTheRuntimeManagesThePendingServer() {
        GroupPublicationGate<Object> gate = new GroupPublicationGate<>();
        Object server = new Object();
        gate.arm(server);

        assertNull(gate.takeIfReady(server, true, true, ignored -> false));
        assertSame(server, gate.takeIfReady(server, true, true, ignored -> true));
    }

    @Test
    void resetDiscardsPendingPublication() {
        GroupPublicationGate<Object> gate = new GroupPublicationGate<>();
        Object server = new Object();
        gate.arm(server);

        gate.reset();

        assertNull(gate.takeIfReady(server, true, true, ignored -> true));
    }
}
