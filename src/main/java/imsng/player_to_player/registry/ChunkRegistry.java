package imsng.player_to_player.registry;

import com.google.gson.JsonObject;
import imsng.player_to_player.config.P2PPaths;
import imsng.player_to_player.util.JsonUtil;
import imsng.player_to_player.util.ThreadPools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
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
 *   <li>{@link #release}：单区块释放（仅占用组本人可释放）；</li>
 *   <li>{@link #releaseAll}：整组释放（主客户端掉线时服务端调用）。</li>
 * </ul>
 * <p>
 * <b>持久化</b>：{@code player-to-player/registry/<维度>.json}（维度名经
 * {@link P2PPaths#sanitize} 合法化），定期落盘 + 关闭时落盘，服务端重启不丢占用状态。
 * MySQL 后端留待 Phase 4（{@code GlobalConfig.mysql} 已预留配置）。
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

    /** 持久化目录（{@code player-to-player/registry/}）。 */
    private final Path persistDir;

    /** region 文件探测器（hasServerData 判定）。 */
    private final RegionFileProbe probe;

    /** 自上次落盘后是否有变更（无变更跳过写盘，减少磁盘噪声）。 */
    private volatile boolean dirty;

    /** 自动落盘任务句柄；shutdown 时取消。 */
    private volatile ScheduledFuture<?> autoPersistTask;

    public ChunkRegistry(Path persistDir, RegionFileProbe probe) {
        this.persistDir = persistDir;
        this.probe = probe;
    }

    // ------------------------------------------------------------ 申请与释放

    /**
     * 申请加载区块：原子检查 目标 + 四邻 是否被其他组占用，全部空闲（或属本组）
     * 才授予（只登记目标区块本身；缓冲层不登记 —— 邻块仍可被本组后续申请）。
     */
    public ClaimResult tryClaim(ChunkKey key, UUID groupId) {
        synchronized (claimLock) {
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
            // 授予：登记目标区块（重复申请刷新占用时间，幂等）
            claims.put(key, new ClaimInfo(groupId, System.currentTimeMillis()));
            dirty = true;
        }
        // hasServerData 是磁盘探测，放锁外做（不占 claimLock，避免拖慢并发申请）
        return ClaimResult.grantedResult(probe != null && probe.hasServerData(key));
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
     * 从持久化目录恢复上次运行的占用状态（服务端启动时调用一次）。
     * 单条坏数据跳过不中断（宁可丢一条记录也不能让服务端起不来）。
     */
    public void load() {
        if (!Files.isDirectory(persistDir)) {
            return; // 首次运行还没有注册表目录
        }
        int loaded = 0;
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(persistDir, "*.json")) {
            for (Path file : stream) {
                loaded += loadFile(file);
            }
        } catch (IOException e) {
            LOGGER.error("区块注册表目录读取失败: {}", persistDir, e);
        }
        LOGGER.info("区块注册表已恢复 {} 条占用记录", loaded);
    }

    /** 读单个维度文件；返回恢复的记录数。 */
    private int loadFile(Path file) {
        int count = 0;
        try {
            JsonObject root = JsonUtil.readFile(file, JsonObject.class);
            if (root == null || !root.has("claims") || !root.get("claims").isJsonObject()) {
                return 0;
            }
            JsonObject claimsObj = root.getAsJsonObject("claims");
            synchronized (claimLock) {
                for (String keyString : claimsObj.keySet()) {
                    try {
                        ChunkKey key = ChunkKey.parse(keyString);
                        UUID groupId = UUID.fromString(claimsObj.get(keyString).getAsString());
                        claims.put(key, new ClaimInfo(groupId, System.currentTimeMillis()));
                        count++;
                    } catch (Exception e) {
                        LOGGER.warn("跳过注册表坏记录: {} ({})", keyString, e.toString());
                    }
                }
            }
        } catch (Exception e) {
            LOGGER.warn("注册表文件读取失败，跳过: {}", file, e);
        }
        return count;
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
    }

    /** 有变更才落盘（定期任务用）。 */
    private void persistIfDirty() {
        if (dirty) {
            persist();
        }
    }

    /**
     * 全量落盘：按维度分组写 {@code <维度>.json}（原子写）。
     * 已清空的维度也要写空文件覆盖旧内容（否则重启会复活已释放的占用）。
     */
    public void persist() {
        // 锁内取快照（保证与 claim/release 互斥），锁外做序列化与磁盘 IO
        Map<ChunkKey, ClaimInfo> snapshot;
        synchronized (claimLock) {
            snapshot = new HashMap<>(claims);
            dirty = false;
        }
        // 按维度分组（TreeMap：输出键序稳定，便于人工 diff）
        Map<String, Map<String, String>> byDimension = new TreeMap<>();
        for (Map.Entry<ChunkKey, ClaimInfo> e : snapshot.entrySet()) {
            byDimension.computeIfAbsent(e.getKey().dimension(), d -> new TreeMap<>())
                    .put(e.getKey().asString(), e.getValue().groupId().toString());
        }
        try {
            Files.createDirectories(persistDir);
            // 现存文件对应的维度若本轮没有占用，也要写空覆盖（见方法 Javadoc）
            List<Path> existing = new ArrayList<>();
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(persistDir, "*.json")) {
                stream.forEach(existing::add);
            }
            for (Map.Entry<String, Map<String, String>> e : byDimension.entrySet()) {
                Path file = persistDir.resolve(P2PPaths.sanitize(e.getKey()) + ".json");
                JsonObject root = new JsonObject();
                JsonObject claimsObj = new JsonObject();
                e.getValue().forEach(claimsObj::addProperty);
                root.add("claims", claimsObj);
                JsonUtil.writeFileAtomic(file, root);
                existing.remove(file.toAbsolutePath().normalize());
                existing.remove(file);
            }
            for (Path stale : existing) {
                // 该维度已无任何占用：删除旧文件（等价写空）
                Files.deleteIfExists(stale);
            }
        } catch (IOException e) {
            LOGGER.error("区块注册表落盘失败: {}", persistDir, e);
        }
    }

    /** 当前占用总数（诊断/测试用）。 */
    public int size() {
        return claims.size();
    }
}
