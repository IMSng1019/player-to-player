package imsng.player_to_player.registry;

import imsng.player_to_player.util.ThreadPools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * 区块注册表（服务端，DESIGN.md 第 5 节；规范"区块注册表：区块列表 / MySQL"）。
 * <p>
 * 维护 区块 → 占用组 的权威映射，保证同一区块同一时刻只被一个组客户端加载：
 * <ul>
 *   <li>{@link #tryClaim}：<b>原子地</b>检查目标区块 + 东西南北四邻（规范的"缓冲层"）
 *       是否被<b>其他组</b>占用，全部空闲（或属本组）才授予并登记目标区块；</li>
 *   <li>{@link #probe}：与 tryClaim 同一套检查但<b>不登记</b>（Phase 4 传送门预检 /
 *       末影珍珠交接判定用 —— 只想知道"能不能占"，不想真的占）；</li>
 *   <li>{@link #release}：单区块释放（仅占用组本人可释放）；</li>
 *   <li>{@link #releaseAll}：整组释放（主客户端掉线时服务端调用）。</li>
 * </ul>
 * <p>
 * <b>持久化</b>：经 {@link RegistryStore} 后端（本地 JSON 或 MySQL，Phase 4），
 * 定期落盘 + 关闭时落盘，服务端重启不丢占用状态。
 * <p>
 * <b>线程模型</b>：claim/release 来自 Netty 事件循环，落盘在 scheduler/调用线程 ——
 * 写路径以内部锁串行化（五格检查+登记必须原子），读走 ConcurrentHashMap 无锁。
 */
public final class ChunkRegistry {

    private static final Logger LOGGER = LoggerFactory.getLogger("player_to_player/registry");

    /** 自动落盘周期（秒）：崩溃最多丢一个周期的变更。 */
    private static final long AUTO_PERSIST_SECONDS = 30;

    /** 占用信息：占用组 + 授予时刻（诊断用）。 */
    public record ClaimInfo(UUID groupId, long claimedAtMillis) {
    }

    /**
     * 申请结果。granted=false 时 blockingChunk/blockingGroup 说明拒绝原因
     * （哪个区块被哪个组占着 —— 客户端据此决定对该组发起预连接，规范"预连接"入口）；
     * granted=true 时 hasServerData 指示该区块走服务端数据还是种子生成。
     */
    public record ClaimResult(boolean granted, ChunkKey blockingChunk, UUID blockingGroup,
                              boolean hasServerData) {

        static ClaimResult grantedResult(boolean hasServerData) {
            return new ClaimResult(true, null, null, hasServerData);
        }

        static ClaimResult rejected(ChunkKey blockingChunk, UUID blockingGroup) {
            return new ClaimResult(false, blockingChunk, blockingGroup, false);
        }
    }

    /** 区块 → 占用信息（读无锁；写经 claimLock 串行化）。 */
    private final Map<ChunkKey, ClaimInfo> claims = new ConcurrentHashMap<>();

    /** 写路径互斥锁：五格检查 + 登记必须是不可分割的整体。 */
    private final Object claimLock = new Object();

    /** 持久化后端（本地 JSON / MySQL，Phase 4 可插拔）。 */
    private final RegistryStore store;

    /** region 文件探测器（hasServerData 判定）。 */
    private final RegionFileProbe probe;

    /** 自上次落盘后是否有变更（无变更跳过写盘，减少磁盘噪声）。 */
    private volatile boolean dirty;

    /** 自动落盘任务句柄；shutdown 时取消。 */
    private volatile ScheduledFuture<?> autoPersistTask;

    public ChunkRegistry(RegistryStore store, RegionFileProbe probe) {
        this.store = store;
        this.probe = probe;
    }

    // ------------------------------------------------------------ 申请与释放

    /**
     * 申请加载区块：原子检查 目标 + 四邻 是否被其他组占用，全部空闲（或属本组）
     * 才授予（只登记目标区块本身；缓冲层不登记 —— 邻块仍可被本组后续申请）。
     */
    public ClaimResult tryClaim(ChunkKey key, UUID groupId) {
        synchronized (claimLock) {
            ClaimResult blocked = checkBlockedLocked(key, groupId);
            if (blocked != null) {
                return blocked;
            }
            // 授予：登记目标区块（重复申请刷新占用时间，幂等）
            claims.put(key, new ClaimInfo(groupId, System.currentTimeMillis()));
            dirty = true;
        }
        // hasServerData 是磁盘探测，放锁外做（不占 claimLock，避免拖慢并发申请）
        return ClaimResult.grantedResult(probe != null && probe.hasServerData(key));
    }

    /**
     * 只读探测（Phase 4）：与 {@link #tryClaim} 同一套"目标 + 四邻"检查，
     * 但<b>不登记任何占用</b>。传送门预检与末影珍珠交接判定用 —— 若走 tryClaim，
     * 探测本身就会占下一个永远不会被加载（也就永远不会随卸载释放）的区块，泄漏占用。
     */
    public ClaimResult probe(ChunkKey key, UUID groupId) {
        synchronized (claimLock) {
            ClaimResult blocked = checkBlockedLocked(key, groupId);
            return blocked != null ? blocked : ClaimResult.grantedResult(false);
        }
    }

    /** 锁内的"目标 + 四邻"占用检查；被阻塞返回拒绝结果，可占返回 null。 */
    private ClaimResult checkBlockedLocked(ChunkKey key, UUID groupId) {
        // 目标区块本身：被其他组占用即拒绝
        ClaimInfo existing = claims.get(key);
        if (existing != null && !existing.groupId().equals(groupId)) {
            return ClaimResult.rejected(key, existing.groupId());
        }
        // 缓冲层（东西南北四邻）：任何一格被其他组占用即拒绝
        for (ChunkKey neighbor : key.neighbors4()) {
            ClaimInfo info = claims.get(neighbor);
            if (info != null && !info.groupId().equals(groupId)) {
                return ClaimResult.rejected(neighbor, info.groupId());
            }
        }
        return null;
    }

    /**
     * 释放单个区块（仅占用组本人可释放，防其他组恶意/误释放）。
     *
     * @return 是否真的释放了（false = 未被占用或占用者不是该组）
     */
    public boolean release(ChunkKey key, UUID groupId) {
        synchronized (claimLock) {
            ClaimInfo existing = claims.get(key);
            if (existing == null || !existing.groupId().equals(groupId)) {
                return false;
            }
            claims.remove(key);
            dirty = true;
            return true;
        }
    }

    /** 释放某组占用的全部区块（主客户端掉线时调用）。返回释放的区块数。 */
    public int releaseAll(UUID groupId) {
        if (groupId == null) {
            return 0;
        }
        int released = 0;
        synchronized (claimLock) {
            var it = claims.entrySet().iterator();
            while (it.hasNext()) {
                if (groupId.equals(it.next().getValue().groupId())) {
                    it.remove();
                    released++;
                }
            }
            if (released > 0) {
                dirty = true;
            }
        }
        if (released > 0) {
            LOGGER.info("已释放组 {} 的 {} 个区块占用", groupId, released);
        }
        return released;
    }

    /**
     * 整组迁移（Phase 3 合并）：把 from 组占用的<b>全部</b>区块原子地改挂到 to 组名下。
     * <p>
     * 与"释放再申请"相比，迁移不存在中间空窗 —— 释放瞬间第三组恰好申请到这些
     * 区块会让合并双方都拿不到；原子改挂则保证合并期间区块归属始终有主。
     *
     * @return 迁移的区块数
     */
    public int migrateAll(UUID from, UUID to) {
        if (from == null || to == null || from.equals(to)) {
            return 0;
        }
        int migrated = 0;
        synchronized (claimLock) {
            for (Map.Entry<ChunkKey, ClaimInfo> e : claims.entrySet()) {
                if (from.equals(e.getValue().groupId())) {
                    e.setValue(new ClaimInfo(to, System.currentTimeMillis()));
                    migrated++;
                }
            }
            if (migrated > 0) {
                dirty = true;
            }
        }
        if (migrated > 0) {
            LOGGER.info("区块占用整组迁移: {} → {} ({} 个区块)", from, to, migrated);
        }
        return migrated;
    }

    /**
     * 指定区块迁移（Phase 3 分离）：把列表中<b>仍属 from 组</b>的区块改挂到 to 组。
     * 不属 from 组的条目静默跳过（分离请求与实际占用之间允许弱一致）。
     *
     * @return 实际迁移的区块数
     */
    public int migrate(Collection<ChunkKey> keys, UUID from, UUID to) {
        if (keys == null || keys.isEmpty() || from == null || to == null || from.equals(to)) {
            return 0;
        }
        int migrated = 0;
        synchronized (claimLock) {
            for (ChunkKey key : keys) {
                ClaimInfo info = claims.get(key);
                if (info != null && from.equals(info.groupId())) {
                    claims.put(key, new ClaimInfo(to, System.currentTimeMillis()));
                    migrated++;
                }
            }
            if (migrated > 0) {
                dirty = true;
            }
        }
        if (migrated > 0) {
            LOGGER.info("区块占用定向迁移: {} → {} ({} / {} 个区块)", from, to, migrated, keys.size());
        }
        return migrated;
    }

    /** 某组当前占用的全部区块（快照；诊断与合并规模评估用）。 */
    public List<ChunkKey> claimsOf(UUID groupId) {
        List<ChunkKey> result = new ArrayList<>();
        if (groupId == null) {
            return result;
        }
        for (Map.Entry<ChunkKey, ClaimInfo> e : claims.entrySet()) {
            if (groupId.equals(e.getValue().groupId())) {
                result.add(e.getKey());
            }
        }
        return result;
    }

    /** 查询区块占用组；空闲返回 null。 */
    public UUID ownerOf(ChunkKey key) {
        ClaimInfo info = claims.get(key);
        return info != null ? info.groupId() : null;
    }

    /** 该区块在服务端存档中是否已有数据（探测器委托）。 */
    public boolean hasServerData(ChunkKey key) {
        return probe != null && probe.hasServerData(key);
    }

    // ------------------------------------------------------------ 持久化

    /**
     * 从持久化后端恢复上次运行的占用状态（服务端启动时调用一次）。
     * 坏数据由后端自行跳过（宁可丢一条记录也不能让服务端起不来）。
     */
    public void load() {
        Map<ChunkKey, UUID> loaded = store.load();
        long now = System.currentTimeMillis();
        synchronized (claimLock) {
            for (Map.Entry<ChunkKey, UUID> e : loaded.entrySet()) {
                claims.put(e.getKey(), new ClaimInfo(e.getValue(), now));
            }
        }
        LOGGER.info("区块注册表已恢复 {} 条占用记录（后端: {}）", loaded.size(), store.describe());
    }

    /** 启动定期落盘（scheduler 驱动；幂等）。 */
    public synchronized void startAutoPersist() {
        if (autoPersistTask != null) {
            return;
        }
        autoPersistTask = ThreadPools.scheduler().scheduleWithFixedDelay(() -> {
            try {
                persistIfDirty();
            } catch (Exception e) {
                LOGGER.error("区块注册表定期落盘失败", e);
            }
        }, AUTO_PERSIST_SECONDS, AUTO_PERSIST_SECONDS, TimeUnit.SECONDS);
    }

    /** 停止定期落盘并做最终 persist（服务端停止时调用；幂等）。 */
    public synchronized void shutdown() {
        ScheduledFuture<?> task = autoPersistTask;
        autoPersistTask = null;
        if (task != null) {
            task.cancel(false);
        }
        persist();
        store.close();
    }

    /** 有变更才落盘（定期任务用）。 */
    private void persistIfDirty() {
        if (dirty) {
            persist();
        }
    }

    /** 全量落盘：锁内取快照（与 claim/release 互斥），锁外交给后端做序列化与 IO。 */
    public void persist() {
        Map<ChunkKey, UUID> snapshot;
        synchronized (claimLock) {
            snapshot = new HashMap<>();
            for (Map.Entry<ChunkKey, ClaimInfo> e : claims.entrySet()) {
                snapshot.put(e.getKey(), e.getValue().groupId());
            }
            dirty = false;
        }
        store.save(snapshot);
    }

    /** 当前占用总数（诊断/测试用）。 */
    public int size() {
        return claims.size();
    }
}
