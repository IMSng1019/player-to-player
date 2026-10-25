package imsng.player_to_player.metadata;

import imsng.player_to_player.registry.PlayerTable;
import net.minecraft.nbt.NbtUtils;
import net.minecraft.nbt.Tag;
import net.minecraft.network.chat.Component;
import net.minecraft.resources.ResourceLocation;
import net.minecraft.server.bossevents.CustomBossEvent;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

class MetadataBossPlayersTest {
    @BeforeAll static void bootstrap() {
        net.minecraft.SharedConstants.tryDetectVersion(); net.minecraft.server.Bootstrap.bootStrap();
    }
    private static PlayerTable players(UUID a, UUID b) {
        var table = new PlayerTable();
        table.put(new PlayerTable.PlayerEntry(a, "Alice", "minecraft:overworld", 0, 64, 0, a, "primary"));
        table.put(new PlayerTable.PlayerEntry(b, "Bob", "minecraft:the_nether", 0, 64, 0, b, "primary"));
        return table;
    }
    @Test void globalAllAndOriginSelfSelectAcrossGroupsWithoutCenterEntities() {
        UUID a = UUID.randomUUID(), b = UUID.randomUUID(); var table = players(a, b);
        assertEquals(Set.of(a, b), Set.copyOf(MetadataBossPlayers.resolveTargets("@a", table, a)));
        assertEquals(List.of(b), MetadataBossPlayers.resolveTargets("@s", table, b));
        assertEquals(List.of(b), MetadataBossPlayers.resolveTargets("Bob", table, a));
        assertEquals(List.of(a), MetadataBossPlayers.resolveTargets(a.toString(), table, b));
    }
    @Test void unresolvedComplexSelectorFailsInsteadOfClearingRemoteMembership() {
        var table = players(UUID.randomUUID(), UUID.randomUUID());
        assertThrows(IllegalArgumentException.class, () -> MetadataBossPlayers.resolveTargets("@a[tag=missing]", table, null));
        assertThrows(IllegalArgumentException.class, () -> MetadataBossPlayers.resolveTargets("@s", table, null));
    }
    @Test void replacingUuidMembershipPersistsRemotePlayersAndExplicitDeletion() {
        UUID a = UUID.randomUUID(), b = UUID.randomUUID();
        CustomBossEvent event = new CustomBossEvent(new ResourceLocation("test", "boss"), Component.literal("Boss"));
        assertTrue(MetadataBossPlayers.replacePlayers(event, List.of(a, b), List.of()));
        Set<UUID> persisted = new HashSet<>();
        for (Tag tag : event.save().getList("Players", Tag.TAG_INT_ARRAY)) persisted.add(NbtUtils.loadUUID(tag));
        assertEquals(Set.of(a, b), persisted);
        assertFalse(MetadataBossPlayers.replacePlayers(event, List.of(b, a), List.of()));
        assertTrue(MetadataBossPlayers.replacePlayers(event, List.of(), List.of()));
        assertTrue(event.save().getList("Players", Tag.TAG_INT_ARRAY).isEmpty());
    }
}
