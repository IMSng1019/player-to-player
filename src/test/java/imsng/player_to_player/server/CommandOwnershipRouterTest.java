package imsng.player_to_player.server;

import imsng.player_to_player.group.CommandRouting;
import imsng.player_to_player.registry.ChunkKey;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 归属裁决（挂死守则的服务端一侧）四类分支锁定。
 *
 * <p>判据的后果是"集成服主线程会不会永久忙等"，所以每个分支都必须有测试钉住：
 * 单一持有组 → 只发它；无人持有 → 交回发起组按需申请；跨多组 → 拒绝；
 * 超上限 → 拒绝。清单（ownedChunks）也必须只含该组真正持有的区块 ——
 * 它会被接收端用来复核归属未变。</p>
 */
class CommandOwnershipRouterTest {

    private static final String DIM = "minecraft:overworld";
    private static final UUID A = UUID.randomUUID();
    private static final UUID B = UUID.randomUUID();

    private static CommandRouting.ChunkRange range(int minX, int minZ, int maxX, int maxZ) {
        return new CommandRouting.ChunkRange(minX, minZ, maxX, maxZ);
    }

    private static Map<ChunkKey, UUID> ownerMap(Object... pairs) {
        Map<ChunkKey, UUID> map = new HashMap<>();
        for (int i = 0; i < pairs.length; i += 3) {
            map.put(new ChunkKey(DIM, (Integer) pairs[i], (Integer) pairs[i + 1]),
                    (UUID) pairs[i + 2]);
        }
        return map;
    }

    @Test
    @DisplayName("单一持有组：目标是它，清单只含它持有的区块")
    void singleOwnerRoutesToIt() {
        Map<ChunkKey, UUID> owners = ownerMap(0, 0, A, 1, 0, A);
        CommandOwnershipRouter.Decision d =
                CommandOwnershipRouter.decide(range(0, 0, 2, 2), DIM, owners::get, A);

        assertFalse(d.rejected());
        assertEquals(A, d.owner());
        assertEquals(2, d.ownedChunks().size(), "只列该组真正持有的区块");
        assertTrue(d.ownedChunks().contains(new ChunkKey(DIM, 0, 0)));
        assertTrue(d.ownedChunks().contains(new ChunkKey(DIM, 1, 0)));
    }

    @Test
    @DisplayName("无人持有：不定目标（调用方交回发起组按需申请），也不下发复核清单")
    void unownedHasNoTarget() {
        CommandOwnershipRouter.Decision d =
                CommandOwnershipRouter.decide(range(100, 100, 101, 101), DIM, owners -> null, A);

        assertFalse(d.rejected());
        assertTrue(d.unowned());
        assertTrue(d.ownedChunks().isEmpty());
    }

    @Test
    @DisplayName("跨多个组：拒绝执行（没有任何组能整体执行，硬执行会遍历到对方区块 → 永久忙等）")
    void crossGroupIsRejected() {
        // 2×2 的包围盒里既有 A 的区块也有 B 的 —— /fill 的内部区块同样不能漏
        Map<ChunkKey, UUID> owners = ownerMap(0, 0, A, 1, 1, B);
        CommandOwnershipRouter.Decision d =
                CommandOwnershipRouter.decide(range(0, 0, 1, 1), DIM, owners::get, A);

        assertTrue(d.rejected());
        assertNotNull(d.rejection());
        assertTrue(d.rejection().contains("跨越了多个组"));
    }

    @Test
    @DisplayName("包围盒覆盖 /fill 的内部区块：角点同组、内部他组也必须判跨组")
    void interiorChunksAreCovered() {
        // 角点 (0,0) 与 (2,2) 都是 A 的，但内部 (1,1) 是 B 的
        Map<ChunkKey, UUID> owners = ownerMap(0, 0, A, 2, 2, A, 1, 1, B);
        CommandOwnershipRouter.Decision d =
                CommandOwnershipRouter.decide(range(0, 0, 2, 2), DIM, owners::get, A);

        assertTrue(d.rejected(), "只查角点会让 A 执行时遍历进 B 的区块 → 主线程永久忙等");
    }

    @Test
    @DisplayName("范围过大：拒绝（防大包围盒把预检变成 DoS）")
    void oversizedRangeIsRejected() {
        long side = (long) Math.sqrt(CommandRouting.MAX_ROUTING_CHUNKS) + 2; // 93×93 > 8192
        CommandOwnershipRouter.Decision d = CommandOwnershipRouter.decide(
                range(0, 0, (int) side, (int) side), DIM, owners -> null, A);

        assertTrue(d.rejected());
        assertTrue(d.rejection().contains("过大"));
    }

    @Test
    @DisplayName("死区块（无主但侧邻属他组）：对全网都不可认领，必须拒绝而不是交给某组去忙等")
    void unclaimableDeadChunkIsRejected() {
        // (5,0) 无主，但它的侧邻 (5,1) 属于 B ⇒ 谁申请都会被四邻规则拒（认领无限重试）
        Map<ChunkKey, UUID> owners = ownerMap(5, 1, B);
        CommandOwnershipRouter.Decision d = CommandOwnershipRouter.decide(
                range(5, 0, 5, 0), DIM, owners::get, A);

        assertTrue(d.rejected(), "交给发起组按需申请会让它的主线程永久忙等");
        assertTrue(d.rejection().contains("无法认领"), d.rejection());
    }

    @Test
    @DisplayName("本组能认领的无主区块：放行（交给发起组按需申请）")
    void claimableUnownedChunkIsAllowed() {
        // (10,10) 无主且四邻也无人持有 ⇒ 发起组可以申请到
        CommandOwnershipRouter.Decision d = CommandOwnershipRouter.decide(
                range(10, 10, 10, 10), DIM, owners -> null, A);
        assertFalse(d.rejected());
        assertTrue(d.unowned());
    }

    @Test
    @DisplayName("checkExecutableBy：他组区块/不可认领区块一律否决，自己的区块放行")
    void checkExecutableByVetoes() {
        Map<ChunkKey, UUID> owners = ownerMap(0, 0, A, 1, 0, B);
        String veto = CommandOwnershipRouter.checkExecutableBy(
                range(0, 0, 1, 0), DIM, A, owners::get);
        assertTrue(veto != null && veto.contains("其他组"), veto);

        assertNull(CommandOwnershipRouter.checkExecutableBy(
                range(0, 0, 0, 0), DIM, A, owners::get));
    }

    @Test
    @DisplayName("空范围（无坐标指令不应走到这里）：拒绝并说明")
    void nullRangeIsRejected() {
        CommandOwnershipRouter.Decision d = CommandOwnershipRouter.decide(null, DIM, owners -> null, A);
        assertTrue(d.rejected());
    }
}
