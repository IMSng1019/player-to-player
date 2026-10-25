package imsng.player_to_player.metadata;

import net.minecraft.nbt.CompoundTag;
import net.minecraft.network.chat.Component;
import net.minecraft.resources.ResourceLocation;
import net.minecraft.server.bossevents.CustomBossEvents;
import net.minecraft.world.level.border.WorldBorder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeAll;

import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/** 直接测试真实原版运行对象，验证仅改存档 NBT 无法覆盖的删除和渐变行为。 */
class MetadataRuntimeObjectsTest {
    @BeforeAll
    static void bootstrapVanillaComponentCodecs() {
        net.minecraft.SharedConstants.tryDetectVersion();
        net.minecraft.server.Bootstrap.bootStrap();
    }

    @Test
    void replacingBossSnapshotDeletesOldEventAndKeepsOfflineMembership() {
        CustomBossEvents events = new CustomBossEvents();
        var staleId = new ResourceLocation("test", "stale");
        events.create(staleId, Component.literal("old"));
        CustomBossEvents authority = new CustomBossEvents();
        var id = new ResourceLocation("test", "current");
        UUID offline = UUID.randomUUID();
        authority.create(id, Component.literal("current")).addOfflinePlayer(offline);
        MetadataRuntime.replaceBossEvents(events, authority.save(), List.of());
        assertNull(events.get(staleId));
        assertEquals(authority.save(), events.save());
        MetadataRuntime.replaceBossEvents(events, new CompoundTag(), List.of());
        assertTrue(events.getEvents().isEmpty());
    }

    @Test
    void expiredBorderLerpAppliesTargetInsteadOfRestartingMovement() {
        WorldBorder border = new WorldBorder();
        CompoundTag state = borderState(100, 20, 1000);
        MetadataRuntime.applyBorderSettings(border, state, 1000, 2000);
        assertEquals(20, border.getSize());
        assertEquals(0, border.getLerpRemainingTime());
    }

    @Test
    void inFlightBorderLerpStartsAtInterpolatedSizeWithRemainingDuration() {
        WorldBorder border = new WorldBorder();
        MetadataRuntime.applyBorderSettings(border, borderState(100, 20, 10000), 1000, 6000);
        assertEquals(20, border.getLerpTarget());
        assertTrue(border.getSize() <= 60 && border.getSize() > 59);
        assertTrue(border.getLerpRemainingTime() <= 5000 && border.getLerpRemainingTime() > 4900);
    }

    private static CompoundTag borderState(double size, double target, long remaining) {
        CompoundTag data = new CompoundTag();
        data.putDouble("BorderSize", size);
        data.putDouble("BorderSizeLerpTarget", target);
        data.putLong("BorderSizeLerpTime", remaining);
        return data;
    }
}
