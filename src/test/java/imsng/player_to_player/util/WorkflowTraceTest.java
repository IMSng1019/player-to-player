package imsng.player_to_player.util;

import org.junit.jupiter.api.Test;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicLong;
import static org.junit.jupiter.api.Assertions.*;

class WorkflowTraceTest {
    @Test
    void recordsMonotonicDurationsAndIgnoresLateCompletion() {
        AtomicLong clock = new AtomicLong(10_000_000);
        var events = new ArrayList<WorkflowTrace.Event>();
        var trace = new WorkflowTrace("merge", "transaction", clock::get, events::add);
        clock.addAndGet(5_000_000);
        trace.mark("prepared");
        clock.addAndGet(8_000_000);
        trace.finish("cancelled");
        trace.mark("late_join");
        trace.finish("success");
        assertEquals(3, events.size());
        assertEquals(5, events.get(1).phaseMillis());
        assertEquals(13, events.get(2).totalMillis());
        assertEquals("cancelled", events.get(2).phase());
        assertTrue(trace.isFinished());
    }
}
