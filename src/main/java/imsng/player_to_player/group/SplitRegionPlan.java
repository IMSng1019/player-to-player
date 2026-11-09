package imsng.player_to_player.group;

import imsng.player_to_player.registry.ChunkKey;
import imsng.player_to_player.registry.SplitRegion;

import java.util.Collection;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.HashMap;
import java.util.Map;

/** 分离区域的纯几何规划，不读取 Minecraft 实例或修改所有权。 */
final class SplitRegionPlan {
    private SplitRegionPlan() { }

    /** 重载只能清掉历史生成依赖，不能消除当前两组必需的加载/光照邻域。 */
    static boolean reloadDemandSeparated(Collection<SplitRegion> departing, Collection<SplitRegion> remaining) {
        return !departing.isEmpty() && !remaining.isEmpty()
                && departing.stream().noneMatch(a -> remaining.stream().anyMatch(a::overlaps));
    }

    /**
     * 渲染区域已经分开，但原版仍用 PLAYER 传播等级保留旧世代外圈时，不能从这个
     * 连通岛中直接截一半。预约整个原组的范围，让所有游戏连接暂离，由原版撤销
     * PLAYER 票据并完成最终写回/卸载；随后两组从已生成的存档重新建立加载需求。
     * 独立特殊票据不会随玩家离开而消失，因此此退路必须拒绝它们，不能强拆传送门桥。
     */
    static Optional<List<SplitRegion>> planReload(Set<ChunkKey> tracked,
                                                 Collection<SplitRegion> playerDemand,
                                                 Collection<SplitRegion> specialTickets) {
        if (tracked.isEmpty() || playerDemand.isEmpty() || !specialTickets.isEmpty()) return Optional.empty();
        Map<String, SplitRegion> bounds = new HashMap<>();
        try {
            for (ChunkKey key : tracked) {
                include(bounds, new SplitRegion(key.dimension(), key.x(), key.x(), key.z(), key.z()));
            }
            // 准备通知到达之前玩家仍可移动；与普通准备一致，保留当前世代需求边缘。
            for (SplitRegion demand : playerDemand) include(bounds, demand);
        } catch (IllegalArgumentException tooLarge) {
            return Optional.empty(); // 保留协议对预约范围大小的上限，不用溢出的矩形移交世界。
        }
        return Optional.of(List.copyOf(bounds.values()));
    }

    private static void include(Map<String, SplitRegion> bounds, SplitRegion region) {
        bounds.merge(region.dimension(), region, (a, b) -> new SplitRegion(a.dimension(),
                Math.min(a.minX(), b.minX()), Math.max(a.maxX(), b.maxX()),
                Math.min(a.minZ(), b.minZ()), Math.max(a.maxZ(), b.maxZ())));
    }

    static Optional<List<SplitRegion>> plan(Set<ChunkKey> tracked,
                                           Collection<SplitRegion> departing,
                                           Collection<SplitRegion> remaining,
                                           Collection<SplitRegion> specialTickets) {
        Set<ChunkKey> unvisited = new HashSet<>(tracked);
        List<SplitRegion> result = new ArrayList<>();
        for (ChunkKey seed : tracked) {
            if (!unvisited.contains(seed) || departing.stream().noneMatch(r -> r.contains(seed))) {
                continue;
            }
            ArrayDeque<ChunkKey> queue = new ArrayDeque<>();
            queue.add(seed);
            unvisited.remove(seed);
            int minX = seed.x(), maxX = seed.x(), minZ = seed.z(), maxZ = seed.z();
            while (!queue.isEmpty()) {
                ChunkKey key = queue.removeFirst();
                minX = Math.min(minX, key.x());
                maxX = Math.max(maxX, key.x());
                minZ = Math.min(minZ, key.z());
                maxZ = Math.max(maxZ, key.z());
                // 原版世代依赖可对角连接。采用八邻比所有权四邻更保守，避免从同一
                // 世代管线中间劈开；只有与离组视野相交的完整实际占用岛才进入计划。
                for (int dx = -1; dx <= 1; dx++) {
                    for (int dz = -1; dz <= 1; dz++) {
                        long x = (long) key.x() + dx, z = (long) key.z() + dz;
                        if ((dx == 0 && dz == 0) || x < Integer.MIN_VALUE || x > Integer.MAX_VALUE
                                || z < Integer.MIN_VALUE || z > Integer.MAX_VALUE) continue;
                        ChunkKey neighbor = new ChunkKey(key.dimension(), (int) x, (int) z);
                        if (unvisited.remove(neighbor)) queue.add(neighbor);
                    }
                }
            }
            SplitRegion bounds;
            try {
                bounds = new SplitRegion(seed.dimension(), minX, maxX, minZ, maxZ);
                // 预备窗口内仍可能补齐世代依赖，因此也预约传入的完整离组需求矩形。
                // 这里传入的是含世代边缘的需求，绝不裁回旧版625格渲染方形。
                for (SplitRegion demand : departing) {
                    if (bounds.overlaps(demand)) {
                        bounds = new SplitRegion(seed.dimension(),
                                Math.min(bounds.minX(), demand.minX()), Math.max(bounds.maxX(), demand.maxX()),
                                Math.min(bounds.minZ(), demand.minZ()), Math.max(bounds.maxZ(), demand.maxZ()));
                    }
                }
            } catch (IllegalArgumentException tooLarge) {
                return Optional.empty(); // 过大的连通岛留在原组，不把无界区域塞进控制协议。
            }
            for (SplitRegion retained : remaining) {
                if (bounds.overlaps(retained)) return Optional.empty();
            }
            for (SplitRegion special : specialTickets) {
                if (bounds.overlaps(special)) return Optional.empty();
            }
            result.add(bounds);
        }
        return result.isEmpty() ? Optional.empty() : Optional.of(List.copyOf(result));
    }
}
