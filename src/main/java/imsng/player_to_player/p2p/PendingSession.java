package imsng.player_to_player.p2p;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/** 撮合应答与传输就绪可能跨线程到达；必须按服务端 sessionId 认领，不能仅按 peer。 */
final class PendingSession {
    private final UUID peer;
    private final CompletableFuture<P2PTransport> result = new CompletableFuture<>();
    private final Map<String, P2PTransport> early = new HashMap<>();
    private String expected;
    private P2PTransport selected;
    private boolean cancelled, taken;

    PendingSession(UUID peer) { this.peer = peer; }
    CompletableFuture<P2PTransport> future() { return result; }

    synchronized void bind(String sessionId) {
        expected = sessionId;
        P2PTransport candidate = early.get(sessionId);
        early.clear();
        if (candidate != null) ready(sessionId, peer, candidate, true);
    }

    synchronized void ready(String id, UUID actualPeer, P2PTransport transport, boolean initiator) {
        if (!initiator || !peer.equals(actualPeer)) return;
        if (expected == null) {
            // 只暂存引用，不取得无关会话的所有权；每个 peer 正常仅有少量并发用途。
            if (early.size() < 16) early.put(id, transport);
            return;
        }
        if (!expected.equals(id)) return;
        if (transport == selected) return;
        if (cancelled || selected != null) {
            transport.close();
            return;
        }
        selected = transport;
        result.complete(transport);
    }

    synchronized P2PTransport take() {
        if (cancelled || taken || selected == null) throw new IllegalStateException("撮合尚未就绪或已被消费");
        taken = true;
        return selected;
    }

    synchronized void cancel() {
        cancelled = true;
        early.clear();
        if (!taken && selected != null) selected.close();
        result.cancel(false);
    }
}
