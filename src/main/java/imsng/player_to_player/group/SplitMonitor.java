package imsng.player_to_player.group;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import imsng.player_to_player.core.NodeContext;
import imsng.player_to_player.metadata.MetadataClient;
import imsng.player_to_player.metadata.MetadataEvents;
import imsng.player_to_player.metadata.MetadataRuntime;
import imsng.player_to_player.metadata.MetadataSnapshot;
import imsng.player_to_player.metadata.MetadataTransferFence;
import imsng.player_to_player.netproto.ControlConnection;
import imsng.player_to_player.netproto.ControlMessage;
import imsng.player_to_player.netproto.MessageType;
import imsng.player_to_player.registry.ChunkKey;
import imsng.player_to_player.registry.SplitRegion;
import imsng.player_to_player.mixin.ChunkMapSplitAccess;
import imsng.player_to_player.mixin.DistanceManagerAccess;
import imsng.player_to_player.util.JsonUtil;
import net.minecraft.nbt.CompoundTag;
import net.minecraft.server.MinecraftServer;
import net.minecraft.server.level.ServerPlayer;
import net.minecraft.server.level.ServerLevel;
import net.minecraft.server.level.Ticket;
import net.minecraft.server.level.TicketType;
import net.minecraft.world.level.ChunkPos;
import net.minecraft.world.level.chunk.ChunkStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * 分离监视器（Phase 3 单端分离，Phase 4 扩展为组分离；主客户端集成服务端侧）。
 * <p>
 * 规范判据："两个加载区域的渲染区块的周围 4 个区块的周围 4 个区块无交集并且
 * 时长达到 10s 时则将其分为两个组客户端"。
 * <p>
 * <b>缓冲层的几何（易错点，曾实现错）</b>：规范正文明确"
 * <b>周围四个区块：指东西南北四个直接相邻区块</b>"，因此"周围 4 格"是
 * <b>四邻十字形</b>扩张，其扩张两次 = <b>曼哈顿距离 ≤ 2</b> 的十字带
 * （东西 ±2、南北 ±2 的十字，而非 5×5 方块）；两十字带无交集 ⟺ 两渲染矩形
 * 的<b>曼哈顿间隙 &gt; 2</b>。{@link #BLOCKS_INTERSECT} 即该判据（O(1) 矩形运算）。
 * <p>
 * 注意：曼哈顿间隙 = 两轴区间距的<b>最大值</b>（等价于逐块取 min 后的间隙），
 * 不是两轴之和。"取和"会把对角方向的分离也算作相交（把距离判据变成切比雪夫
 * 方块，缓冲层最厚处多出 2 格），使该分离的组迟迟不分离 —— 与 DESIGN.md
 * 第 12.5 节的"曼哈顿间隙"判据不符。
 * <p>
 * <b>组分离（Phase 4）</b>：把"间隙 ≤ 2 且同维度"视为连通边，对全组玩家做
 * <b>连通分量</b>划分 —— 不含主客户端本人的分量整体离组（规范"单端分离是特殊的
 * 组分离"：单人分量即单端分离，同一条代码路径）。分量的<b>成员构成</b>持续
 * {@value #SPLIT_HOLD_MILLIS} ms 不变才触发（成员进出视为交集变化，计时归零），
 * 新组内的算力分配（选主）由服务端裁决（它握有算力表权威数据）。
 * <p>
 * 达标后的两阶段分离动作（主线程采样，网络请求异步）：
 * <ol>
 *   <li>按实际已占用/未决区块构造完整加载岛；留组需求或特殊票据仍相连时延期；</li>
 *   <li>prepare只预约边界，服务端通知所有离组成员断开游戏连接，控制连接仍保持；</li>
 *   <li>DISCONNECT捕获最终玩家NBT并等待同一上行器的耐久确认；旧区块经过原版
 *       卸载链逐块最终写回并释放，局部未决加载以UNLOADED收束；</li>
 *   <li>确认玩家离线、所有权释放和旧holder移除后才commit；服务端此时改组并指派新主，
 *       新主重新申请区块并读取最终数据，绝不在旧主仍演算时直接迁移注册表。</li>
 * </ol>
 * 本实现复用耐久写回与加载门控，因此会有明确的等待阶段；完整影子实例热备仍是
 * 后续架构工作。超时不强制移交所有权，改走排空后回滚，避免旧副本继续写入。
 * <p>
 * 线程模型：{@link #tick} 在集成服务端主线程（GroupServerHooks 每秒驱动一次），
 * 只做矩形运算与计时；序列化在主线程（微秒级），上传/请求转 io 池。
 */
public final class SplitMonitor {

    private static final Logger LOGGER = LoggerFactory.getLogger("player_to_player/split");

    /** 无交集须持续的时长（规范：10s）。 */
    private static final long SPLIT_HOLD_MILLIS = 10_000;

    /** 膨胀边距（"周围4格的周围4格" = 四邻十字形扩张两次 = 曼哈顿距离 2）。 */
    private static final int MARGIN_CHUNKS = 2;

    /** 源端准备过久时请求回滚；仍须排空旧holder后才能重连，服务端超时会展示明确失败界面。 */
    private static final long DRAIN_TIMEOUT_MILLIS = 60_000;

    /** 候选计时：分量签名（成员 UUID 有序拼接）→ 首次检测到的时刻（仅主线程读写）。 */
    private static final Map<String, Long> candidates = new HashMap<>();

    /** 分离请求已在途的分量签名（防重复发送；主线程标记，io 完成后回主线程摘除）。 */
    private static final Set<String> inFlight = new HashSet<>();

    /** 同一个宿主一次只处理一个分离事务，避免两个分量同时争用票据/所有权边界。 */
    private static volatile SplitOperation activeSplit;

    private static final class SplitOperation {
        final UUID splitId = UUID.randomUUID();
        final MinecraftServer server;
        final ControlConnection connection;
        final ChunkClaimClient claims;
        final UUID groupId;
        final String signature;
        final Map<UUID, ServerPlayer> departing;
        final List<SplitRegion> regions;
        final Map<UUID, CompletableFuture<Void>> finalUploads = new HashMap<>();
        final long started = System.currentTimeMillis();
        final String metadataWorldId;
        final long metadataTerm;
        long metadataVersion;
        volatile boolean cancelRequested;
        boolean prepared;
        boolean drainStarted;
        boolean ownsTickFreeze;
        boolean metadataFlushPending;
        boolean finalMetadataFlushed;
        boolean requestInFlight;
        boolean resolveStatus;
        long nextRequestAt;
        long nextProgressAt;

        SplitOperation(MinecraftServer server, ControlConnection connection, ChunkClaimClient claims,
                       UUID groupId, String signature, Map<UUID, ServerPlayer> departing,
                       List<SplitRegion> regions, MetadataSnapshot metadata) {
            this.server = server;
            this.connection = connection;
            this.claims = claims;
            this.groupId = groupId;
            this.signature = signature;
            this.departing = Map.copyOf(departing);
            this.regions = List.copyOf(regions);
            this.metadataWorldId = metadata.worldId();
            this.metadataTerm = metadata.term();
            this.metadataVersion = metadata.version();
        }

        // 与服务端预约一致，包括四邻边界；否则边缘晚到claim会让服务端永远拒绝commit。
        boolean contains(ChunkKey key) { return regions.stream().anyMatch(region -> region.protects(key)); }
    }

    private SplitMonitor() {
    }

    /** 世界会话拆除时清空计时状态（幂等）。 */
    public static void reset() {
        candidates.clear();
        inFlight.clear();
        activeSplit = null;
    }

    /**
     * 每秒一轮的分离检测（集成服务端主线程，GroupServerHooks 驱动；
     * 调用方已确认 server 是被接管的集成服务端）。
     */
    public static void tick(MinecraftServer server) {
        if (activeSplit != null) {
            advanceSplit(server);
            return;
        }
        ChunkClaimClient claims = GroupRuntime.claims();
        ControlConnection conn = GroupRuntime.conn();
        UUID groupId = GroupRuntime.activeGroupId();
        if (claims == null || conn == null || !conn.isOpen() || groupId == null) {
            return;
        }
        List<ServerPlayer> players = server.getPlayerList().getPlayers();
        if (players.size() < 2) {
            candidates.clear(); // 只剩主客户端本人，无从分离
            return;
        }
        UUID hostId = NodeContext.get().clientId(); // 主客户端的控制协议档案身份
        if (players.stream().anyMatch(player -> canonicalId(player) == null)) {
            candidates.clear(); // 名单尚未到达：不能凭离线UUID把玩家分到错误组。
            return;
        }
        int viewDistance = server.getPlayerList().getViewDistance();
        long now = System.currentTimeMillis();

        // 连通分量划分（组内玩家数量级为个位数，O(n²) 并查集足够）
        List<List<ServerPlayer>> components = computeComponents(players, viewDistance);
        if (components.size() <= 1) {
            candidates.clear(); // 全组仍连通：所有计时归零（规范要求"持续"）
            return;
        }
        // 主客户端所在分量留守；其余每个分量都是独立的离组候选
        Set<String> currentSignatures = new HashSet<>();
        for (List<ServerPlayer> component : components) {
            if (component.stream().anyMatch(p -> hostId.equals(canonicalId(p)))) {
                continue;
            }
            String signature = signatureOf(component);
            currentSignatures.add(signature);
            if (inFlight.contains(signature)) {
                continue;
            }
            Long since = candidates.putIfAbsent(signature, now);
            if (since != null && now - since >= SPLIT_HOLD_MILLIS) {
                candidates.remove(signature);
                inFlight.add(signature);
                initiateSplit(server, conn, claims, groupId, signature, component, viewDistance);
                if (activeSplit != null) break;
            }
        }
        // 成员构成变化 / 分量消失：对应计时归零
        candidates.keySet().retainAll(currentSignatures);
    }

    // ------------------------------------------------------------ 判定

    /** 以"同维度且缓冲十字带相交（曼哈顿间隙 ≤ 2）"为连通边，划分玩家连通分量（并查集）。 */
    private static List<List<ServerPlayer>> computeComponents(List<ServerPlayer> players,
                                                              int viewDistance) {
        int n = players.size();
        int[] parent = new int[n];
        for (int i = 0; i < n; i++) {
            parent[i] = i;
        }
        for (int i = 0; i < n; i++) {
            for (int j = i + 1; j < n; j++) {
                ServerPlayer a = players.get(i);
                ServerPlayer b = players.get(j);
                // 跨维度不算交集（规范语义：渲染区块是维度内概念）
                if (a.serverLevel() == b.serverLevel()
                        && blocksIntersect(a.chunkPosition().x, a.chunkPosition().z,
                        b.chunkPosition().x, b.chunkPosition().z, viewDistance)) {
                    union(parent, i, j);
                }
            }
        }
        Map<Integer, List<ServerPlayer>> byRoot = new HashMap<>();
        for (int i = 0; i < n; i++) {
            byRoot.computeIfAbsent(find(parent, i), r -> new ArrayList<>()).add(players.get(i));
        }
        return new ArrayList<>(byRoot.values());
    }

    private static int find(int[] parent, int i) {
        while (parent[i] != i) {
            parent[i] = parent[parent[i]]; // 路径减半
            i = parent[i];
        }
        return i;
    }

    private static void union(int[] parent, int a, int b) {
        parent[find(parent, a)] = find(parent, b);
    }

    /** 分量签名：成员 UUID 排序拼接（成员进出 → 签名变化 → 计时归零）。 */
    private static String signatureOf(List<ServerPlayer> component) {
        TreeSet<String> ids = new TreeSet<>();
        for (ServerPlayer p : component) {
            ids.add(p.getUUID().toString());
        }
        StringJoiner joiner = new StringJoiner("|");
        ids.forEach(joiner::add);
        return joiner.toString();
    }

    /**
     * 两渲染区域（以玩家所在区块为中心、半边长 viewDistance 的矩形）的
     * "周围4格×2"缓冲十字带是否相交 —— 即{@link #MARGIN_CHUNKS}判据本身。
     * <p>
     * gapX/gapZ 分别为两矩形在 X / Z 轴上的区间距（重叠为 0）。逐块看：两矩形
     * 在轴上的最近距离分别是 gapX / gapZ，故曼哈顿间隙 = gapX + gapZ（对角时
     * 为两者之和）；而"两侧各扩张 2 格的十字带仍相交"⟺ gapX ≤ 2 <b>且</b>
     * gapZ ≤ 2 ⟺ max(gapX, gapZ) ≤ 2。两种写法在轴对齐时等价，但后者直接表达
     * "相交"、无需先算间隙再比大小，也更不容易被误写成切比雪夫方块判据。
     * <p>
     * 抽成纯静态方法是为了可单测：本包在单元测试环境无法构造 {@code ServerPlayer}。
     */
    static boolean blocksIntersect(int ax, int az, int bx, int bz, int viewDistance) {
        int size = viewDistance; // 半边长（渲染矩形边长 = 2*size+1）
        int gapX = Math.max(0, Math.abs(ax - bx) - 2 * size);
        int gapZ = Math.max(0, Math.abs(az - bz) - 2 * size);
        return Math.max(gapX, gapZ) <= MARGIN_CHUNKS;
    }

    // ------------------------------------------------------------ 执行

    /** 规划完整占用岛后预约，直到旧玩家、最终上行、claim和holder全部排空才允许提交。 */
    private static void initiateSplit(MinecraftServer server, ControlConnection conn,
                                      ChunkClaimClient claims, UUID groupId, String signature,
                                      List<ServerPlayer> component, int viewDistance) {
        String unsafeMetadata = metadataRejection(server, null);
        if (unsafeMetadata != null) {
            inFlight.remove(signature);
            LOGGER.info("分离延期，保持原组运行: {}", unsafeMetadata);
            return;
        }
        MetadataSnapshot metadata = MetadataClient.snapshot();
        List<SplitRegion> departingDemand = new ArrayList<>();
        List<SplitRegion> remainingDemand = new ArrayList<>();
        Map<UUID, ServerPlayer> departing = new HashMap<>();
        // 实际claim岛覆盖已发生的依赖，额外需求矩形覆盖准备消息在途时尚未补齐的世代边缘。
        int radius = viewDistance + ChunkStatus.maxDistance() + 2;
        for (ServerPlayer player : server.getPlayerList().getPlayers()) {
            SplitRegion demand = playerDemand(player, radius);
            if (component.contains(player)) {
                UUID id = canonicalId(player);
                if (id == null) { inFlight.remove(signature); return; }
                departing.put(id, player);
                departingDemand.add(demand);
            } else {
                remainingDemand.add(demand);
            }
        }
        java.util.Optional<List<SplitRegion>> planned;
        try {
            planned = SplitRegionPlan.plan(claims.trackedKeysSnapshot(), departingDemand,
                    remainingDemand, specialTicketRegions(server));
        } catch (IllegalArgumentException | IllegalStateException unsafeTickets) {
            inFlight.remove(signature);
            LOGGER.warn("分离延期：无法安全规划特殊票据范围: {}", unsafeTickets.toString());
            return;
        }
        if (planned.isEmpty()) {
            inFlight.remove(signature);
            LOGGER.info("分离延期：完整加载岛仍与留组需求或特殊票据相连，保持当前组");
            return;
        }
        SplitOperation operation = new SplitOperation(server, conn, claims, groupId,
                signature, departing, planned.get(), metadata);
        // 必须在发prepare前登记：离组端收到服务端通知后可能比prepared应答更早断线。
        activeSplit = operation;
        LOGGER.info("分离开始准备: split={} 离组人数={} 完整区域={}",
                operation.splitId, departing.size(), operation.regions);
        sendPhase(operation, "prepare");
    }

    private static UUID canonicalId(ServerPlayer player) {
        return LocalPermissions.canonicalMemberId(player.getUUID(), player.getGameProfile().getName());
    }

    private static SplitRegion playerDemand(ServerPlayer player, int radius) {
        ChunkPos pos = player.chunkPosition();
        return new SplitRegion(player.serverLevel().dimension().location().toString(),
                pos.x - radius, pos.x + radius, pos.z - radius, pos.z + radius);
    }

    /**
     * 只放行PLAYER和加载管线自己的LIGHT/UNKNOWN短期票据。PORTAL、FORCED、DRAGON
     * 以及任何模组自定义票据都保守视为独立加载需求，不能在没有迁移这些票据时拆走。
     * 票据影响随距离递增；33为FULL门槛，另外加maxDistance覆盖世代依赖边缘。
     */
    private static List<SplitRegion> specialTicketRegions(MinecraftServer server) {
        List<SplitRegion> result = new ArrayList<>();
        for (ServerLevel level : server.getAllLevels()) {
            var tickets = ((DistanceManagerAccess) level.getChunkSource().chunkMap
                    .getDistanceManager()).player_to_player$tickets();
            for (var entry : tickets.long2ObjectEntrySet()) {
                for (Ticket<?> ticket : entry.getValue()) {
                    TicketType<?> type = ticket.getType();
                    if (type == TicketType.PLAYER || type == TicketType.LIGHT || type == TicketType.UNKNOWN) continue;
                    ChunkPos pos = new ChunkPos(entry.getLongKey());
                    int radius = Math.max(0, 33 - ticket.getTicketLevel()) + ChunkStatus.maxDistance() + 2;
                    // 超大自定义票据保守保护整组，不尝试构造无界预约范围。
                    if (radius > 128) throw new IllegalStateException("特殊票据影响范围过大，不能安全分离");
                    result.add(new SplitRegion(level.dimension().location().toString(),
                            pos.x - radius, pos.x + radius, pos.z - radius, pos.z + radius));
                }
            }
        }
        return result;
    }

    /** DISCONNECT仍持有有效实体，在原版移除玩家前捕获真正最后的状态，而不是10秒前快照。 */
    public static void onPlayerDisconnect(MinecraftServer server, ServerPlayer player) {
        SplitOperation operation = activeSplit;
        if (operation == null || operation.server != server) return;
        for (Map.Entry<UUID, ServerPlayer> departing : operation.departing.entrySet()) {
            if (departing.getValue() != player) continue;
            PlayerStateUploadService uploads = GroupRuntime.playerUploads();
            if (uploads == null) {
                operation.cancelRequested = true;
                return;
            }
            ChunkKey location = new ChunkKey(player.serverLevel().dimension().location().toString(),
                    player.chunkPosition().x, player.chunkPosition().z);
            if (!operation.contains(location)) operation.cancelRequested = true;
            try {
                CompletableFuture<Void> saved = uploads.offerConfirmed(departing.getKey(),
                        player.saveWithoutId(new CompoundTag()));
                operation.finalUploads.put(departing.getKey(), saved);
                saved.whenComplete((ignored, failure) -> {
                    if (failure != null) operation.cancelRequested = true;
                });
            } catch (RuntimeException snapshotFailure) {
                // 模组玩家NBT/执行器失败不能从DISCONNECT钩子抛出并打断原版移除玩家。
                operation.cancelRequested = true;
                LOGGER.error("离组最终玩家快照失败，分离转安全回滚: split={} 玩家={}",
                        operation.splitId, departing.getKey(), snapshotFailure);
            }
            return;
        }
    }

    /** 服务端超时只请求取消；在源holder排空前仍保留fence，不提前恢复旧写者。 */
    public static void requestCancel(UUID splitId) {
        SplitOperation operation = activeSplit;
        if (operation != null && operation.splitId.equals(splitId)) operation.cancelRequested = true;
    }

    /** 主线程每秒推进，无主线程等待网络。网络失败转status，绝不把未知commit结果当作回滚。 */
    private static void advanceSplit(MinecraftServer server) {
        SplitOperation operation = activeSplit;
        if (operation == null || operation.server != server) return;
        long now = System.currentTimeMillis();
        if (now - operation.started > DRAIN_TIMEOUT_MILLIS) operation.cancelRequested = true;
        if (operation.requestInFlight || now < operation.nextRequestAt) return;
        if (operation.resolveStatus) { sendPhase(operation, "status"); return; }
        if (!operation.prepared) return;
        if (operation.metadataFlushPending) return; // 异步尾部/版本确认尚未完成，不把其临时暂停误作失败。
        if (!operation.cancelRequested) {
            String reason = metadataRejection(server, operation);
            if (reason != null) {
                operation.cancelRequested = true;
                LOGGER.warn("分离期间元数据屏障失效，转安全取消: split={} reason={}", operation.splitId, reason);
            }
        }
        if (!operation.drainStarted) {
            // 准备应答返回时发现不可迁移状态，尚未撤销任何本地加载岛；可直接取消预约，
            // 不必为了取消而卸载整个区域。原组始终是唯一写者，重连继续使用其现有对象。
            if (operation.cancelRequested) sendPhase(operation, "cancel");
            return;
        }
        boolean playersGone = server.getPlayerList().getPlayers().stream().noneMatch(player -> {
            UUID id = canonicalId(player);
            return operation.departing.containsValue(player)
                    || (id != null && operation.departing.containsKey(id));
        });
        boolean claimsGone = operation.claims.isSplitDrained();
        boolean holdersGone = holdersDrained(operation);
        boolean playersSaved = operation.finalUploads.size() == operation.departing.size()
                && operation.finalUploads.values().stream()
                .allMatch(future -> future.isDone() && !future.isCompletedExceptionally());
        if (playersGone && claimsGone && holdersGone && (playersSaved || operation.cancelRequested)) {
            if (!operation.cancelRequested && !operation.finalMetadataFlushed) {
                flushMetadataTail(operation, true);
                return;
            }
            MetadataSnapshot currentMetadata = MetadataClient.snapshot();
            if (!operation.cancelRequested && (currentMetadata == null
                    || currentMetadata.version() < operation.metadataVersion)) return;
            // 回滚的ROLE_ASSIGN可能经另一条连接先于本端cancel ACK到达。只有旧holder已全清
            // 才在发cancel前解除本地fence；服务端预约此刻仍阻止旧组提前重占，安全性不变。
            if (operation.cancelRequested) operation.claims.cancelSplitDrain();
            sendPhase(operation, operation.cancelRequested ? "cancel" : "commit");
        } else if (now >= operation.nextProgressAt) {
            operation.nextProgressAt = now + 5_000;
            LOGGER.info("分离排空等待: split={} 玩家离线={} 最终玩家写回={} 占用释放={} 旧holder卸载={} 取消={}",
                    operation.splitId, playersGone, playersSaved, claimsGone, holdersGone, operation.cancelRequested);
        }
    }

    private static boolean holdersDrained(SplitOperation operation) {
        for (ServerLevel level : operation.server.getAllLevels()) {
            String dimension = level.dimension().location().toString();
            ChunkMapSplitAccess chunks = (ChunkMapSplitAccess) level.getChunkSource().chunkMap;
            for (var holder : chunks.player_to_player$updatingChunks().values()) {
                ChunkPos pos = holder.getPos();
                if (operation.contains(new ChunkKey(dimension, pos.x, pos.z))) return false;
            }
            for (var holder : chunks.player_to_player$pendingUnloads().values()) {
                ChunkPos pos = holder.getPos();
                if (operation.contains(new ChunkKey(dimension, pos.x, pos.z))) return false;
            }
        }
        return true;
    }

    private static void sendPhase(SplitOperation operation, String phase) {
        if (activeSplit != operation || operation.requestInFlight) return;
        operation.requestInFlight = true;
        JsonObject request = new JsonObject();
        request.addProperty("phase", phase);
        request.addProperty("splitId", operation.splitId.toString());
        request.addProperty("groupId", operation.groupId.toString());
        MetadataSnapshot metadata = MetadataClient.snapshot();
        if (metadata != null) {
            request.addProperty("metadataWorldId", metadata.worldId());
            request.addProperty("metadataTerm", metadata.term());
            request.addProperty("metadataVersion", metadata.version());
        }
        if ("prepare".equals(phase)) {
            JsonArray members = new JsonArray();
            operation.departing.keySet().forEach(id -> members.add(id.toString()));
            request.add("departingClientIds", members);
            JsonArray regions = new JsonArray();
            operation.regions.forEach(region -> regions.add(region.toJson()));
            request.add("regions", regions);
        }
        try {
            operation.connection.request(ControlMessage.of(MessageType.SPLIT_REQUEST, request))
                    .whenComplete((reply, failure) -> operation.server.execute(() ->
                            receivePhase(operation, phase, reply, failure)));
        } catch (RuntimeException failure) {
            receivePhase(operation, phase, null, failure);
        }
    }

    private static void receivePhase(SplitOperation operation, String sentPhase,
                                     ControlMessage reply, Throwable failure) {
        if (activeSplit != operation || !GroupRuntime.isManagedServer(operation.server)) return;
        operation.requestInFlight = false;
        if (reply != null && reply.type() == MessageType.ERROR) {
            String code = JsonUtil.getString(reply.json(), "code", "?");
            if (!operation.prepared && ("prepare".equals(sentPhase)
                    || ("status".equals(sentPhase) && "unknown_split".equals(code)))) {
                // 服务端明确拒绝prepare时未创建事务/暂停玩家，和网络超时的未知结果不同。
                LOGGER.info("分离准备被拒，保持原组: split={} code={}", operation.splitId, code);
                finishSplit(operation, false);
                return;
            }
            if ("commit".equals(sentPhase)) {
                // 有明确错误应答就没有提交；先回滚，不能把not_drained等错误当成功移交。
                operation.cancelRequested = true;
                operation.resolveStatus = false;
                operation.nextRequestAt = System.currentTimeMillis() + 1_000;
                LOGGER.warn("分离提交被拒，等待安全排空后回滚: split={} code={}", operation.splitId, code);
                return;
            }
        }
        if (failure != null || reply == null || reply.type() != MessageType.SPLIT_ACK) {
            operation.resolveStatus = true;
            operation.nextRequestAt = System.currentTimeMillis() + 2_000;
            LOGGER.warn("分离{}结果未知，查询服务端事务状态而不提前解锁: split={}", sentPhase, operation.splitId);
            return;
        }
        String phase = JsonUtil.getString(reply.json(), "phase", "");
        operation.resolveStatus = false;
        if ("committed".equals(phase)) {
            finishSplit(operation, true);
        } else if ("cancelled".equals(phase) || "rollback".equals(phase)
                || (!JsonUtil.getBoolean(reply.json(), "granted", false) && "prepare".equals(sentPhase))) {
            finishSplit(operation, false);
        } else if ("prepared".equals(phase) || "cancel_requested".equals(phase)) {
            if (!operation.prepared) {
                operation.prepared = true;
                operation.metadataVersion = Math.max(operation.metadataVersion,
                        JsonUtil.getLong(reply.json(), "metadataVersion", operation.metadataVersion));
                String metadataProblem = metadataRejection(operation.server, operation);
                boolean sameBarrier = operation.metadataWorldId.equals(JsonUtil.getString(reply.json(),
                        "metadataWorldId", operation.metadataWorldId))
                        && operation.metadataTerm == JsonUtil.getLong(reply.json(), "metadataTerm", operation.metadataTerm);
                if (metadataProblem != null || !sameBarrier || "cancel_requested".equals(phase)) {
                    operation.cancelRequested = true;
                    operation.nextRequestAt = System.currentTimeMillis();
                    LOGGER.warn("分离尚未开始区域排空，取消失效准备: split={} reason={}", operation.splitId,
                            metadataProblem != null ? metadataProblem : "准备任期变化或中心已请求取消");
                    return;
                }
                // 先冻结并提交模拟字段，再允许卸载它们所属区块。否则注册表释放后中心可能
                // 已撤销末地模拟权，最后的 DragonFight 便再也无法以原写者提交。
                flushMetadataTail(operation, false);
            }
            if ("cancel_requested".equals(phase)) operation.cancelRequested = true;
            operation.nextRequestAt = System.currentTimeMillis() + 1_000;
        } else {
            // 不认识的状态不改变所有权；持续status由服务端TTL给离组端显示明确失败。
            operation.resolveStatus = true;
            operation.nextRequestAt = System.currentTimeMillis() + 2_000;
        }
    }

    private static void finishSplit(SplitOperation operation, boolean committed) {
        if (operation.drainStarted) operation.claims.cancelSplitDrain();
        if (operation.ownsTickFreeze && !GroupRuntime.isExitDraining()) GroupRuntime.setTickFrozen(false);
        activeSplit = null;
        inFlight.remove(operation.signature);
        candidates.clear();
        LOGGER.info("分离事务{}: split={} 离组人数={}（旧区块经最终写回释放，无直接所有权迁移）",
                committed ? "提交完成" : "已回滚", operation.splitId, operation.departing.size());
    }

    /** 本地真实运行对象是最终依据，不能把中心最近一次字段报告当作当前复活中间态的证明。 */
    private static String metadataRejection(MinecraftServer server, SplitOperation operation) {
        if ((GroupRuntime.isTickFrozen() && (operation == null || !operation.ownsTickFreeze))
                || GroupRuntime.isExitDraining()) return "原组正在冻结或退出排空，不能并发分离";
        MetadataSnapshot metadata = MetadataClient.snapshot();
        if (metadata == null || !MetadataClient.isSimulationReady()) return "世界元数据尚未确认，不能迁移模拟权";
        if (operation != null && (!operation.metadataWorldId.equals(metadata.worldId())
                || operation.metadataTerm != metadata.term())) return "分离准备期间世界身份或写入任期已变化";
        try {
            MetadataRuntime.assertTransferSafe(server);
            return null;
        } catch (IllegalStateException unsafe) {
            return unsafe.getMessage();
        }
    }

    /**
     * 分离也有模拟字段尾部。为避免卸载区域与字段所有权之间出现空窗，当前实现保守冻结
     * 留组模拟直到排空/提交或取消；区块服务仍继续卸载。网络等待全部异步，绝不占主线程。
     */
    private static void flushMetadataTail(SplitOperation operation, boolean finalPass) {
        if (operation.metadataFlushPending || activeSplit != operation) return;
        if (!operation.ownsTickFreeze) {
            if (GroupRuntime.isTickFrozen()) { operation.cancelRequested = true; return; }
            operation.ownsTickFreeze = true;
            GroupRuntime.setTickFrozen(true);
        }
        operation.metadataFlushPending = true;
        MetadataEvents.flushTransferState(operation.server)
                .thenCompose(durable -> MetadataClient.synchronize(operation.server, operation.connection).thenApply(applied -> {
                    if (!MetadataTransferFence.of(durable).coveredBy(applied))
                        throw new IllegalStateException("分离模拟字段未达到已提交尾部版本");
                    return applied;
                }))
                .whenComplete((applied, failure) -> operation.server.execute(() -> {
                    if (activeSplit != operation || !GroupRuntime.isManagedServer(operation.server)) return;
                    operation.metadataFlushPending = false;
                    String unsafe = failure == null ? metadataRejection(operation.server, operation) : failure.toString();
                    if (unsafe != null) {
                        operation.cancelRequested = true;
                        LOGGER.warn("分离模拟字段尾部未确认，保持旧组并取消: split={} reason={}", operation.splitId, unsafe);
                    } else {
                        operation.metadataVersion = Math.max(operation.metadataVersion, applied.version());
                        if (finalPass) operation.finalMetadataFlushed = true;
                        else if (!operation.cancelRequested) {
                            operation.claims.beginSplitDrain(operation::contains);
                            operation.drainStarted = true;
                        }
                    }
                    operation.nextRequestAt = System.currentTimeMillis();
                    advanceSplit(operation.server);
                }));
    }
}
