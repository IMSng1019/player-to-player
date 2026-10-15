package imsng.player_to_player.registry;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 玩家表建行与刷新语义（复查 G-09 的收口）。
 *
 * <p>修复前：{@code put} 无调用者，{@code updatePosition}/{@code updateGroupRole}
 * 都是 {@code computeIfPresent} ⇒ 表恒空 ⇒ {@code /list}、{@code /tp &lt;玩家名&gt;}、
 * {@code /msg} 三个特例全是死代码。这里把"HELLO 建行 → 位置刷新 → 组/角色同步"
 * 的链路钉死。</p>
 */
class PlayerTableTest {

    private static final UUID A = UUID.randomUUID();
    private static final UUID B = UUID.randomUUID();
    private static final UUID GROUP = UUID.randomUUID();

    @Test
    @DisplayName("未声明的玩家：位置/组更新一律 no-op（防越权上报凭空建行）")
    void updatesBeforeDeclareAreNoOps() {
        PlayerTable table = new PlayerTable();
        table.updatePosition(A, "minecraft:the_nether", 1, 2, 3);
        table.updateGroupRole(A, GROUP, "primary");

        assertNull(table.get(A));
        assertEquals(0, table.size());
    }

    @Test
    @DisplayName("HELLO 声明建行 → 位置刷新 → 按名检索（/tp、/msg 的寻址面）")
    void declareThenRefreshPosition() {
        PlayerTable table = new PlayerTable();
        table.declare(A, "Alice");

        PlayerTable.PlayerEntry entry = table.get(A);
        assertNotNull(entry);
        assertEquals("Alice", entry.playerName());
        assertEquals("minecraft:overworld", entry.dimension(), "建行时的占位维度必须合法");
        assertNotNull(table.byName("alice"), "大小写不敏感按名检索应命中");

        table.updatePosition(A, "minecraft:the_nether", 10.5, 64.0, -3.25);
        PlayerTable.PlayerEntry moved = table.byName("ALICE");
        assertNotNull(moved);
        assertEquals("minecraft:the_nether", moved.dimension());
        assertEquals(10.5, moved.x());
        assertEquals(-3.25, moved.z());
    }

    @Test
    @DisplayName("组/角色同步只改这两列，不丢位置（合并/分离/角色指派的既有调用口径）")
    void updateGroupRoleKeepsPosition() {
        PlayerTable table = new PlayerTable();
        table.declare(A, "Alice");
        table.updatePosition(A, "minecraft:overworld", 1, 2, 3);
        table.updateGroupRole(A, GROUP, "secondary");

        PlayerTable.PlayerEntry entry = table.get(A);
        assertEquals(GROUP, entry.groupId());
        assertEquals("secondary", entry.role());
        assertEquals(1.0, entry.x());
        assertEquals("minecraft:overworld", entry.dimension());
    }

    @Test
    @DisplayName("重复 HELLO 声明保留既有位置与组（重连不得把玩家打回原点）")
    void redeclareKeepsExistingState() {
        PlayerTable table = new PlayerTable();
        table.declare(A, "Alice");
        table.updatePosition(A, "minecraft:overworld", 100, 70, 200);
        table.updateGroupRole(A, GROUP, "primary");

        table.declare(A, "Alice");
        PlayerTable.PlayerEntry entry = table.get(A);
        assertEquals(100.0, entry.x());
        assertEquals(GROUP, entry.groupId());
        assertEquals("primary", entry.role());
    }

    @Test
    @DisplayName("断连移除按 UUID 生效（clientId == 玩家 UUID）")
    void removeOnDisconnect() {
        PlayerTable table = new PlayerTable();
        table.declare(A, "Alice");
        table.declare(B, "Bob");
        assertEquals(2, table.size());

        table.remove(A);
        assertNull(table.get(A));
        assertTrue(table.snapshot().containsKey(B));
    }
}
