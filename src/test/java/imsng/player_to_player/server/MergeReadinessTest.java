package imsng.player_to_player.server;

import org.junit.jupiter.api.Test;
import java.util.Set;
import java.util.UUID;
import static org.junit.jupiter.api.Assertions.*;

class MergeReadinessTest {
    @Test
    void requiresEveryRedirectAndBothPrimaryBarriers() {
        UUID a = UUID.randomUUID(), b = UUID.randomUUID(), follower = UUID.randomUUID();
        var gate = new MergeReadiness(a, b, Set.of(a, follower));
        assertFalse(gate.prepared(a));
        assertFalse(gate.prepared(UUID.randomUUID()));
        assertTrue(gate.prepared(follower));
        assertFalse(gate.prepared(follower), "准备齐备只通知一次");
        assertFalse(gate.tailReceived(a), "让出方不能代替接管方确认");
        assertFalse(gate.sourceFrozen(b));
        assertFalse(gate.sourceFrozen(a));
        assertTrue(gate.tailReceived(b));
        assertTrue(gate.tryCommit());
        assertFalse(gate.tryCommit());
        assertFalse(gate.abort(), "提交后不能因迟到失败而回滚");
    }

    @Test
    void abortedPreparationCannotRestartOrCommit() {
        UUID a = UUID.randomUUID(), b = UUID.randomUUID();
        var gate = new MergeReadiness(a, b, Set.of(a));
        assertTrue(gate.abort());
        assertFalse(gate.prepared(a));
        assertFalse(gate.tailReceived(b));
        assertFalse(gate.sourceFrozen(a));
        assertFalse(gate.tryCommit());
    }
}
