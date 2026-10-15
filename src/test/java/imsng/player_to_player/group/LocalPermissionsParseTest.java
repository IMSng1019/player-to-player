package imsng.player_to_player.group;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 权限名单解析（服务端 PERMISSION_RELAY → 本地落权的入口）的边界锁定。
 *
 * <p>"本地权限位 = 服务端 ops 名单的真实等级"这条口径全靠这里：解析错了就会
 * 给错等级（未拦截路径的权限面）或让 F3+F4/指令补全静默失效。</p>
 */
class LocalPermissionsParseTest {

    private static JsonObject row(String uuid, String name, int level) {
        JsonObject row = new JsonObject();
        row.addProperty("uuid", uuid);
        row.addProperty("name", name);
        row.addProperty("level", level);
        return row;
    }

    @Test
    @DisplayName("正常名单：UUID/名字/等级逐条解析")
    void parsesMembers() {
        UUID a = UUID.randomUUID();
        UUID b = UUID.randomUUID();
        JsonArray array = new JsonArray();
        array.add(row(a.toString(), "Alice", 4));
        array.add(row(b.toString(), "Bob", 2));

        Map<UUID, LocalPermissions.Member> parsed = LocalPermissions.parseMembers(array);
        assertEquals(2, parsed.size());
        assertEquals("Alice", parsed.get(a).name());
        assertEquals(4, parsed.get(a).level(), "真实等级不钳制（F3+F4 与补全树按它生效）");
        assertEquals(2, parsed.get(b).level());
    }

    @Test
    @DisplayName("非法条目跳过；等级越界钳到 0..4；缺名字回落 '?'")
    void skipsMalformedRows() {
        UUID good = UUID.randomUUID();
        JsonArray array = new JsonArray();
        array.add(row("不是-uuid", "X", 3));          // 缺省/非法 uuid：跳过
        array.add(row(good.toString(), "Good", 99));   // 越界等级：钳到 4
        array.add(row(UUID.randomUUID().toString(), "Low", -5)); // 越界等级：钳到 0
        JsonObject noName = new JsonObject();
        noName.addProperty("uuid", UUID.randomUUID().toString());
        noName.addProperty("level", 1);                // 缺名字
        array.add(noName);
        array.add("不是对象");                          // 非对象条目：跳过

        Map<UUID, LocalPermissions.Member> parsed = LocalPermissions.parseMembers(array);
        assertEquals(3, parsed.size());
        assertEquals(4, parsed.get(good).level());
        assertTrue(parsed.values().stream().anyMatch(m -> m.level() == 0));
        assertTrue(parsed.values().stream().anyMatch(m -> "?".equals(m.name())));
    }

    @Test
    @DisplayName("字段缺失/不是数组：返回 null（调用方保持旧名单，不把表打空）")
    void missingMembersKeepsOldTable() {
        assertNull(LocalPermissions.parseMembers(null));
        assertNull(LocalPermissions.parseMembers(com.google.gson.JsonNull.INSTANCE));
        assertNull(LocalPermissions.parseMembers(new com.google.gson.JsonPrimitive("x")));
    }
}
