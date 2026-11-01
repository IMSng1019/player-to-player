package imsng.player_to_player.metadata;

import net.minecraft.nbt.CompoundTag;
import net.minecraft.nbt.ListTag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class MetadataSchedulesTest {
    @Test
    void futureScheduleNeverEntersCheckpointBarrier() throws Exception {
        assertNull(MetadataSchedules.collectIfDue(events(1000, 2000), 999, () -> {
            fail("未来计划事件不应采集全量元数据或等待后台检查点");
            return null;
        }));
    }

    @Test
    void dueScheduleUsesFreshFullStateAfterCommitBarrier() throws Exception {
        CompoundTag fresh = new CompoundTag();
        fresh.putString("origin", "fresh-after-barrier");
        assertSame(fresh, MetadataSchedules.collectIfDue(events(2000, 1000), 1000, () -> fresh));
        assertSame(fresh, MetadataSchedules.collectIfDue(events(1000), 1001, () -> fresh));
        assertNull(MetadataSchedules.collectIfDue(new ListTag(), 1001, () -> {
            fail("空计划队列不进入检查点屏障");
            return null;
        }));
    }

    private static ListTag events(long... times) {
        ListTag events = new ListTag();
        for (long time : times) {
            CompoundTag event = new CompoundTag();
            event.putLong("TriggerTime", time);
            events.add(event);
        }
        return events;
    }
}
