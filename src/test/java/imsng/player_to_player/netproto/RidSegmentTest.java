package imsng.player_to_player.netproto;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@code _rid} 号段隔离的常量契约（P.0 必修前置）。
 *
 * <p>同一条控制连接上两端都会发起 request（客户端上送指令、服务端下发转发执行），
 * 应答匹配规则是"命中在途表即完成 future 并不再分发" —— 两个方向若共用一段自增号，
 * 先到的一侧会误吞对端请求（本端 future 被错配完成、对端请求永远不进处理器，
 * 直到 15 秒超时）。因此接受端（服务端/中转端接受的连接）用低位段、发起端
 * （客户端主动拨出的连接）用高位段。</p>
 *
 * <p>本测试锁定两个常量的取值关系（互异、两段之间有足够余量）。
 * 接线（哪个构造点传哪个常量）由 {@code ControlServer}/{@code ControlClient}
 * 的两处调用保证，见各自的注释。</p>
 */
class RidSegmentTest {

    @Test
    @DisplayName("两端号段互异且相距足够远（同一连接的请求编号不可能重叠）")
    void segmentsAreDisjoint() {
        assertNotEquals(Protocol.RID_BASE_ACCEPTOR, Protocol.RID_BASE_DIALER);
        assertTrue(Protocol.RID_BASE_DIALER > Protocol.RID_BASE_ACCEPTOR);

        // 一段连接的请求数远小于两段间距（实际是每分钟个位数；这里按 2^31 留余量）
        long gap = Protocol.RID_BASE_DIALER - Protocol.RID_BASE_ACCEPTOR;
        assertTrue(gap >= (1L << 31),
                "两段之间必须留出远超单连接请求量的余量，实际 gap=" + gap);
    }

    @Test
    @DisplayName("号段起点为正且可安全加计数（不会摸到负数/溢出区）")
    void basesAreUsable() {
        assertTrue(Protocol.RID_BASE_ACCEPTOR > 0);
        assertTrue(Protocol.RID_BASE_DIALER > 0);
        assertEquals(1L, Protocol.RID_BASE_ACCEPTOR, "接受端从 1 起（与原实现一致）");
        assertTrue(Protocol.RID_BASE_DIALER + Integer.MAX_VALUE > 0);
    }
}
