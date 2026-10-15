package imsng.player_to_player.group;

import com.google.gson.JsonArray;
import com.google.gson.JsonNull;
import com.google.gson.JsonPrimitive;
import imsng.player_to_player.registry.ChunkKey;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 接收端归属复核（防冻结的最后一道闸）的纯函数测试。
 *
 * <p>契约：服务端下发的 {@code ownedChunks} 是它核过的"本组持有"清单，
 * 接收端执行前必须确认这些区块<b>仍</b>由本组持有（合并/分离/退出事务会在
 * 预检与执行之间改变归属）。不通过就必须<b>不执行</b> —— 集成服主线程碰触
 * 非本组区块会永久忙等。</p>
 */
class CommandOwnershipVerifyTest {

    private static final String DIM = "minecraft:overworld";

    @Test
    @DisplayName("清单字段的解析：缺失/非数组 → 空清单；非法条目跳过")
    void parseChunkKeysIsDefensive() {
        assertTrue(CommandRouting.parseChunkKeys(null).isEmpty());
        assertTrue(CommandRouting.parseChunkKeys(JsonNull.INSTANCE).isEmpty());
        assertTrue(CommandRouting.parseChunkKeys(new JsonPrimitive("x")).isEmpty());

        JsonArray array = new JsonArray();
        array.add(new ChunkKey(DIM, 1, 2).asString());
        array.add("垃圾键");                 // 非法：跳过
        array.add(new JsonPrimitive(42));    // 非字符串：跳过
        array.add(JsonNull.INSTANCE);        // null：跳过
        List<ChunkKey> keys = CommandRouting.parseChunkKeys(array);
        assertEquals(List.of(new ChunkKey(DIM, 1, 2)), keys);
    }

    @Test
    @DisplayName("空清单放行（无坐标指令 / 无人持有 → 发起组按需申请）")
    void emptyKeysPass() {
        assertNull(CommandRouting.verifyOwnedChunks(List.of(), key -> false));
        assertNull(CommandRouting.verifyOwnedChunks(null, key -> false));
    }

    @Test
    @DisplayName("全部仍持有 → 放行；任一失去 → 拒绝且不执行")
    void missingOwnershipRejects() {
        List<ChunkKey> keys = List.of(
                new ChunkKey(DIM, 0, 0), new ChunkKey(DIM, 1, 0), new ChunkKey(DIM, 1, 1));
        Set<ChunkKey> owned = new HashSet<>(keys);

        assertNull(CommandRouting.verifyOwnedChunks(keys, owned::contains));

        // 预检之后所有权被迁移走了（合并/分离窗口）
        owned.remove(new ChunkKey(DIM, 1, 1));
        String verdict = CommandRouting.verifyOwnedChunks(keys, owned::contains);
        assertTrue(verdict != null && verdict.contains("不由本组持有"), verdict);
    }
}
