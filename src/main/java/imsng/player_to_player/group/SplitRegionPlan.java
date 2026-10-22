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

/** 分离区域的纯几何规划，不读取 Minecraft 实例或修改所有权。 */
final class SplitRegionPlan {
    private SplitRegionPlan() { }

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
