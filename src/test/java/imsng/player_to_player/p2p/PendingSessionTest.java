package imsng.player_to_player.p2p;

import org.junit.jupiter.api.Test;
import java.util.UUID;
import java.util.function.Consumer;
import static org.junit.jupiter.api.Assertions.*;

class PendingSessionTest {
    @Test
    void matchesSessionBeforeOrAfterBrokerReplyAndClosesLateResult() {
        UUID peer = UUID.randomUUID();
        var pending = new PendingSession(peer);
        var wrong = new Transport();
        var correct = new Transport();
        pending.ready("unrelated", peer, wrong, true);
        pending.ready("expected", peer, correct, true);
        pending.bind("expected");
        assertSame(correct, pending.future().join());
        assertTrue(wrong.open, "不得关闭别人的预同步或隧道");
        pending.cancel();
        assertFalse(correct.open);
        var late = new Transport();
        pending.ready("expected", peer, late, true);
        assertFalse(late.open);
    }

    @Test
    void ignoresInboundAndWrongPeerAndTransfersOwnershipOnce() {
        UUID peer = UUID.randomUUID();
        var pending = new PendingSession(peer);
        pending.bind("expected");
        pending.ready("expected", UUID.randomUUID(), new Transport(), true);
        pending.ready("expected", peer, new Transport(), false);
        assertFalse(pending.future().isDone());
        var correct = new Transport();
        pending.ready("expected", peer, correct, true);
        assertSame(correct, pending.take());
        pending.cancel();
        assertTrue(correct.open, "领取后资源归调用方");
        assertThrows(IllegalStateException.class, pending::take);
    }

    private static class Transport implements P2PTransport {
        boolean open = true;
        public void send(byte[] bytes) { }
        public void setReceiver(Consumer<byte[]> receiver) { }
        public boolean isOpen() { return open; }
        public void close() { open = false; }
    }
}
