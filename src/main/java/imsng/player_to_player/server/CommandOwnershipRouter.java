package imsng.player_to_player.server;

import imsng.player_to_player.group.CommandRouting;
import imsng.player_to_player.registry.ChunkKey;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;

/**
 * 指令落点的区块归属裁决（挂死守则的服务端一侧；纯函数，可单测）。
 *
 * <p>输入是 {@link CommandRouting.Analysis#chunkRange() 坐标包围盒}（{@code /fill} 只声明
 * 角点，内部区块必须一并覆盖）与"查归属"的函数（生产环境是
 * {@link imsng.player_to_player.registry.ChunkRegistry#ownerOf}），输出是唯一目标组、
 * 服务端核过的持有区块清单（随转发下发给接收端复核）或拒绝文案。</p>
 *
 * <p>三条分支的取舍：<b>单一持有组</b> → 发给它（它持有全部相关区块，执行不会碰触
 * 别人的领地）；<b>无人持有</b> → 不在这里定，交回发起组按需申请（与原版本地执行
 * 同一条加载门控）；<b>跨多个组</b> → 拒绝：没有任何一个组能整体执行，而让每个组各
 * 执行一部分会让它们遍历到对方的区块 —— 集成服主线程对"本组不持有"的区块会在
 * {@code managedBlock + join} 里永久忙等（无 watchdog）。</p>
 *
 * <p>为什么单独成类：这四条分支是防冻结的核心判据，必须能在没有 MinecraftServer
 * 的环境里被测试钉住（{@link CommandOwnershipRouterTest}）。</p>
 */
public final class CommandOwnershipRouter {

    /** 裁决结果。 */
    public record Decision(UUID owner, List<ChunkKey> ownedChunks, String rejection) {

        /** 是否拒绝执行。 */
        public boolean rejected() {
            return rejection != null;
        }

        /** 是否"无人持有"（调用方交回发起组）。 */
        public boolean unowned() {
            return !rejected() && owner == null;
        }
    }

    private CommandOwnershipRouter() {
    }

    /**
     * 裁决哪些区块由哪个组持有，并校验目标组确实能安全执行。
     *
     * @param range       坐标包围盒（区块坐标，含两端）
     * @param dimension   维度名（构造 ChunkKey 用）
     * @param ownerOf     查归属（返回 null 表示空闲）
     * @param originGroup 发起组（"无人持有 → 交回发起组"时用它做过预演校验；可为 null）
     */
    public static Decision decide(CommandRouting.ChunkRange range, String dimension,
                                  Function<ChunkKey, UUID> ownerOf, UUID originGroup) {
        if (range == null) {
            return new Decision(null, List.of(), "无法确定指令影响的区块范围，未执行");
        }
        if (range.count() > CommandRouting.MAX_ROUTING_CHUNKS) {
            return new Decision(null, List.of(),
                    "指令影响的区块范围过大（" + range.count() + " 个区块），未执行");
        }
        Set<UUID> owners = new LinkedHashSet<>();
        for (int cx = range.minX(); cx <= range.maxX(); cx++) {
            for (int cz = range.minZ(); cz <= range.maxZ(); cz++) {
                UUID owner = ownerOf.apply(new ChunkKey(dimension, cx, cz));
                if (owner != null) {
                    owners.add(owner);
                    if (owners.size() > 1) {
                        return new Decision(null, List.of(),
                                "指令跨越了多个组的区块归属（" + owners.size() + " 组），未执行");
                    }
                }
            }
        }
        if (owners.isEmpty()) {
            if (originGroup == null) {
                return new Decision(null, List.of(), null);
            }
            // 无人持有 ⇒ 交回发起组按需申请：先预演"它能不能认领整个包围盒"，
            // 认领不了的形态（侧邻属他组的死区块）必须以清晰文案拒绝，而不是让它去忙等
            String veto = checkExecutableBy(range, dimension, originGroup, ownerOf);
            return veto == null ? new Decision(null, List.of(), null)
                    : new Decision(null, List.of(), veto);
        }
        UUID owner = owners.iterator().next();
        String veto = checkExecutableBy(range, dimension, owner, ownerOf);
        if (veto != null) {
            return new Decision(null, List.of(), veto);
        }
        List<ChunkKey> owned = new ArrayList<>();
        for (int cx = range.minX(); cx <= range.maxX(); cx++) {
            for (int cz = range.minZ(); cz <= range.maxZ(); cz++) {
                ChunkKey key = new ChunkKey(dimension, cx, cz);
                if (owner.equals(ownerOf.apply(key))) {
                    owned.add(key);
                }
            }
        }
        return new Decision(owner, List.copyOf(owned), null);
    }

    /**
     * 目标组能否安全执行"覆盖该包围盒"的指令（预演认领条件）。
     * <p>
     * 两条否决理由：
     * <ol>
     *   <li>包围盒内有<b>其他组</b>的区块 —— 执行时会写/读别人的区块（主线程永久忙等）；</li>
     *   <li>包围盒内有<b>本组无法认领</b>的无主区块 —— 认领规则要求"目标无主且四邻不得属
     *       他组"（{@code ChunkRegistry.checkBlockedLocked}），侧邻属他组的无主区块对全网
     *       都是死区块：认领必然被拒、门控无限重试 ⇒ 同样是主线程永久忙等。</li>
     * </ol>
     * 这两条也是 {@code /execute positioned … run <绝对坐标指令>} 这类"引用执行上下文"
     * 形态的兜底：它们只在发起组执行，但发起组同样可能越界 —— 越界就必须干净拒绝，
     * 而不是让整个组卡死。
     *
     * @return null = 可以执行；否则为拒绝文案
     */
    public static String checkExecutableBy(CommandRouting.ChunkRange range, String dimension,
                                           UUID group, Function<ChunkKey, UUID> ownerOf) {
        if (range == null) {
            return "无法确定指令影响的区块范围，未执行";
        }
        if (range.count() > CommandRouting.MAX_ROUTING_CHUNKS) {
            return "指令影响的区块范围过大（" + range.count() + " 个区块），未执行";
        }
        for (int cx = range.minX(); cx <= range.maxX(); cx++) {
            for (int cz = range.minZ(); cz <= range.maxZ(); cz++) {
                ChunkKey key = new ChunkKey(dimension, cx, cz);
                UUID owner = ownerOf.apply(key);
                if (owner != null) {
                    if (!owner.equals(group)) {
                        return "指令影响的区域包含其他组的区块（" + cx + "," + cz + "），未执行";
                    }
                    continue;
                }
                for (ChunkKey neighbor : key.neighbors4()) {
                    UUID neighborOwner = ownerOf.apply(neighbor);
                    if (neighborOwner != null && !neighborOwner.equals(group)) {
                        return "指令影响的区域包含本组无法认领的区块（" + cx + "," + cz
                                + "，侧邻属其他组），未执行";
                    }
                }
            }
        }
        return null;
    }
}
