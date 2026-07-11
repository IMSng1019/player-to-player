package imsng.player_to_player.group;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import imsng.player_to_player.core.NodeContext;
import imsng.player_to_player.netproto.ControlConnection;
import imsng.player_to_player.netproto.ControlMessage;
import imsng.player_to_player.netproto.MessageType;
import imsng.player_to_player.registry.ChunkKey;
import imsng.player_to_player.util.JsonUtil;
import imsng.player_to_player.util.ThreadPools;
import net.minecraft.nbt.CompoundTag;
import net.minecraft.server.MinecraftServer;
import net.minecraft.server.level.ServerPlayer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;
import java.util.TreeSet;
import java.util.UUID;

/**
 * 分离监视器（Phase 3 单端分离，Phase 4 扩展为组分离；主客户端集成服务端侧）。
 * <p>
 * 规范判据："两个加载区域的渲染区块的周围 4 个区块的周围 4 个区块无交集并且
 * 时长达到 10s 时则将其分为两个组客户端"。四邻域膨胀两次 = 曼哈顿距离 ≤ 2 的
 * 膨胀，因此"无交集"等价于两渲染矩形的<b>曼哈顿间隙 &gt; 2</b>（间隙 = X 轴
 * 区间距 + Z 轴区间距），矩形运算 O(1)。
 * <p>
 * <b>组分离（Phase 4）</b>：把"间隙 ≤ 2 且同维度"视为连通边，对全组玩家做
 * <b>连通分量</b>划分 —— 不含主客户端本人的分量整体离组（规范"单端分离是特殊的
 * 组分离"：单人分量即单端分离，同一条代码路径）。分量的<b>成员构成</b>持续
 * {@value #SPLIT_HOLD_MILLIS} ms 不变才触发（成员进出视为交集变化，计时归零），
 * 新组内的算力分配（选主）由服务端裁决（它握有算力表权威数据）。
 * <p>
 * 达标后的分离动作（io 线程）：
 * <ol>
 *   <li>上传每名离组玩家的最终数据（PLAYER_DATA_UPLOAD —— 新组的集成服务端经
 *       PLAYER_DATA_REQUEST 取回，位置/背包连续）；</li>
 *   <li>SPLIT_REQUEST（departingClientIds[] + 离组者渲染区内本组占用的区块）→
 *       服务端算力分配选主 + 组表分离 + 注册表定向迁移；新主直收
 *       ROLE_ASSIGN(primary)，其余成员的 ROLE_ASSIGN(secondary) 待新主
 *       GROUP_WORLD_READY 后冲刷（服务端 MergeCoordinator 暂存）；</li>
 *   <li>granted → 本地 {@link ChunkClaimClient#forgetLocal 摘除}已迁移区块 +
 *       {@link MergeTriggers#suppressFor 抑制}合并触发（防分离↔合并震荡）。</li>
 * </ol>
 * <b>Phase 3 简化（对规范"这 10s 期间副客户端进行预同步"）</b>：离组者的区块
 * 数据不走 P2P 预同步而由服务端供给 —— 主客户端的实时存盘上行保证服务端
 * 存档始终新鲜，离组者申请区块时从服务端拉取即可，数据一致性等价；代价是
 * 新主要经历一次本地世界加载（数秒），无缝接管留待影子实例成熟后收口。
 * <p>
 * 线程模型：{@link #tick} 在集成服务端主线程（GroupServerHooks 每秒驱动一次），
 * 只做矩形运算与计时；序列化在主线程（微秒级），上传/请求转 io 池。
 */
public final class SplitMonitor {

    private static final Logger LOGGER = LoggerFactory.getLogger("player_to_player/split");

    /** 无交集须持续的时长（规范：10s）。 */
    private static final long SPLIT_HOLD_MILLIS = 10_000;

    /** 膨胀边距（"周围4格的周围4格" = 曼哈顿距离 2）。 */
    private static final int MARGIN_CHUNKS = 2;

    /** 分离执行后的合并触发抑制窗口（毫秒）。 */
    private static final long MERGE_SUPPRESS_MILLIS = 30_000;

    /** 候选计时：分量签名（成员 UUID 有序拼接）→ 首次检测到的时刻（仅主线程读写）。 */
    private static final Map<String, Long> candidates = new HashMap<>();

    /** 分离请求已在途的分量签名（防重复发送；主线程标记，io 完成后回主线程摘除）。 */
    private static final Set<String> inFlight = new HashSet<>();

    private SplitMonitor() {
    }

    /** 世界会话拆除时清空计时状态（幂等）。 */
    public static void reset() {
        candidates.clear();
        inFlight.clear();
    }

    /**
     * 每秒一轮的分离检测（集成服务端主线程，GroupServerHooks 驱动；
     * 调用方已确认 server 是被接管的集成服务端）。
     */
    public static void tick(MinecraftServer server) {
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
        UUID hostId = NodeContext.get().clientId(); // 主客户端玩家（其分量留守原组）
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
            if (component.stream().anyMatch(p -> p.getUUID().equals(hostId))) {
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
            }
        }
        // 成员构成变化 / 分量消失：对应计时归零
        candidates.keySet().retainAll(currentSignatures);
    }

    // ------------------------------------------------------------ 判定

    /** 以"同维度且曼哈顿间隙 ≤ 2"为连通边，划分玩家连通分量（并查集）。 */
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
                        && manhattanGap(a, b, viewDistance) <= MARGIN_CHUNKS) {
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

    /** 两玩家渲染矩形（边长 2*vd+1）的曼哈顿间隙（区块数；重叠为 0）。 */
    private static int manhattanGap(ServerPlayer a, ServerPlayer b, int viewDistance) {
        int size = viewDistance; // 半边长
        int ax = a.chunkPosition().x;
        int az = a.chunkPosition().z;
        int bx = b.chunkPosition().x;
        int bz = b.chunkPosition().z;
        int gapX = Math.max(0, Math.abs(ax - bx) - 2 * size);
        int gapZ = Math.max(0, Math.abs(az - bz) - 2 * size);
        return gapX + gapZ;
    }

    // ------------------------------------------------------------ 执行

    /** 一名离组成员的主线程快照（NBT 生成后不可变，跨线程只读安全）。 */
    private record DepartingSnapshot(UUID playerId, String playerName, CompoundTag playerTag) {
    }

    /** 触发一次（组）分离（主线程：序列化玩家 NBT + 收集区块；网络转 io）。 */
    private static void initiateSplit(MinecraftServer server, ControlConnection conn,
                                      ChunkClaimClient claims, UUID groupId, String signature,
                                      List<ServerPlayer> component, int viewDistance) {
        LOGGER.info("组分离触发: {} 名玩家的渲染区域与组 {} 其他成员无交集已持续 {}s",
                component.size(), groupId, SPLIT_HOLD_MILLIS / 1000);

        // 主线程序列化每名离组玩家的最终数据 + 收集渲染区内本组占用的区块并集
        List<DepartingSnapshot> snapshots = new ArrayList<>(component.size());
        Set<ChunkKey> migrating = new LinkedHashSet<>();
        for (ServerPlayer player : component) {
            snapshots.add(new DepartingSnapshot(player.getUUID(),
                    player.getGameProfile().getName(),
                    player.saveWithoutId(new CompoundTag())));
            String dimension = player.serverLevel().dimension().location().toString();
            int cx = player.chunkPosition().x;
            int cz = player.chunkPosition().z;
            for (int x = cx - viewDistance; x <= cx + viewDistance; x++) {
                for (int z = cz - viewDistance; z <= cz + viewDistance; z++) {
                    ChunkKey key = new ChunkKey(dimension, x, z);
                    if (claims.isClaimed(key)) {
                        migrating.add(key);
                    }
                }
            }
        }

        ThreadPools.io().execute(() -> {
            try {
                // 1. 逐人上传最终数据（新组的集成服务端从服务端取回）
                for (DepartingSnapshot snapshot : snapshots) {
                    byte[] gzip = ChunkUploadService.compress(snapshot.playerTag());
                    if (gzip != null) {
                        JsonObject up = new JsonObject();
                        up.addProperty("playerUuid", snapshot.playerId().toString());
                        up.addProperty("playerName", snapshot.playerName());
                        conn.send(ControlMessage.of(MessageType.PLAYER_DATA_UPLOAD, up, gzip));
                    }
                }
                // 2. 分离申请（多成员数组；服务端算力分配选新主）
                JsonObject req = new JsonObject();
                req.addProperty("groupId", groupId.toString());
                JsonArray departingIds = new JsonArray();
                for (DepartingSnapshot snapshot : snapshots) {
                    departingIds.add(snapshot.playerId().toString());
                }
                req.add("departingClientIds", departingIds);
                JsonArray chunks = new JsonArray();
                for (ChunkKey key : migrating) {
                    chunks.add(key.asString());
                }
                req.add("chunks", chunks);
                ControlMessage ack = conn.request(
                                ControlMessage.of(MessageType.SPLIT_REQUEST, req))
                        .get(30, java.util.concurrent.TimeUnit.SECONDS);
                boolean granted = ack.type() == MessageType.SPLIT_ACK
                        && JsonUtil.getBoolean(ack.json(), "granted", false);
                server.execute(() -> inFlight.remove(signature));
                if (!granted) {
                    LOGGER.warn("分离申请被拒: {} 名成员（保持现组）", snapshots.size());
                    return;
                }
                // 3. 本地摘除已迁移区块 + 抑制合并触发（防震荡）。
                //    离组玩家的隧道会随其客户端切换/重定向自然断开。
                for (ChunkKey key : migrating) {
                    claims.forgetLocal(key);
                }
                MergeTriggers.suppressFor(MERGE_SUPPRESS_MILLIS);
                LOGGER.info("组分离完成: {} 名成员自立新组 {}（新主 {}，迁移 {} 个区块）",
                        snapshots.size(),
                        JsonUtil.getString(ack.json(), "newGroupId", "?"),
                        JsonUtil.getString(ack.json(), "newPrimaryClientId", "?"),
                        JsonUtil.getInt(ack.json(), "migratedChunks", 0));
            } catch (Exception e) {
                server.execute(() -> inFlight.remove(signature));
                LOGGER.warn("分离流程失败（下轮检测重试）: {} 名成员", snapshots.size(), e);
            }
        });
    }
}
