package imsng.player_to_player.group;

import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/** 权限名单跨宿主启动保留，但权限包应用记录必须随玩家连接和服务器实例隔离。 */
class LocalPermissionStateTest {

    private final LocalPermissionState state = new LocalPermissionState();
    private final UUID group = UUID.randomUUID();
    private final UUID playerId = UUID.randomUUID();

    @Test
    void earlyAuthoritySurvivesHostRestartButApplicationDoesNot() {
        Object player = new Object();
        state.replace(group, Map.of(playerId, new LocalPermissions.Member("Alice", 4)));
        state.markApplied(playerId, player, 4);

        state.resetRuntime();

        assertEquals(4, state.levelFor(group, playerId, "Alice"));
        assertTrue(state.needsApply(playerId, player, 4));
    }

    @Test
    void nextWorldSessionMustNotInheritAuthority() {
        state.replace(group, Map.of(playerId, new LocalPermissions.Member("Alice", 4)));
        state.resetSession();
        assertNull(state.levelFor(group, playerId, "Alice"));
    }

    @Test
    void reconnectWithSameUuidNeedsItsOwnPermissionPacket() {
        Object oldPlayer = new Object();
        Object reconnectedPlayer = new Object();
        state.markApplied(playerId, oldPlayer, 4);

        assertFalse(state.needsApply(playerId, oldPlayer, 4));
        assertTrue(state.needsApply(playerId, reconnectedPlayer, 4));
        state.markApplied(playerId, reconnectedPlayer, 4);
        state.forget(playerId, oldPlayer);
        assertFalse(state.needsApply(playerId, reconnectedPlayer, 4),
                "旧连接晚到的 DISCONNECT 不得抹掉新连接记录");
    }

    @Test
    void uuidWinsOverConflictingNameAndNameFallbackMustBeUnique() {
        Map<UUID, LocalPermissions.Member> table = new LinkedHashMap<>();
        table.put(UUID.randomUUID(), new LocalPermissions.Member("Alice", 4));
        table.put(playerId, new LocalPermissions.Member("Alice", 0));
        state.replace(group, table);

        assertEquals(0, state.levelFor(group, playerId, "Alice"));
        assertEquals(0, state.levelFor(group, UUID.randomUUID(), "ALICE"),
                "仅凭重名无法确定身份，不得继承任意一人的 OP");
        state.replace(group, Map.of(playerId, new LocalPermissions.Member("Alice", 2)));
        assertEquals(2, state.levelFor(group, UUID.randomUUID(), "aLiCe"));
    }

    @Test
    void explicitDeopAndMemberRemovalBothRevokeExistingAuthority() {
        Object player = new Object();
        state.replace(group, Map.of(playerId, new LocalPermissions.Member("Alice", 4)));
        state.markApplied(playerId, player, 4);
        state.replace(group, Map.of(playerId, new LocalPermissions.Member("Alice", 0)));
        assertEquals(0, state.levelFor(group, playerId, "Alice"));
        assertTrue(state.needsApply(playerId, player, 0));

        state.replace(group, Map.of());
        assertEquals(0, state.levelFor(group, playerId, "Alice"));
        assertNull(state.levelFor(UUID.randomUUID(), playerId, "Alice"),
                "其他组的名单不能用于撤销或授予当前组权限");
    }
}
