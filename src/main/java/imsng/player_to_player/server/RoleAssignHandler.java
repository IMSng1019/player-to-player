package imsng.player_to_player.server;

import com.google.gson.JsonObject;
import imsng.player_to_player.compute.ComputeScore;
import imsng.player_to_player.compute.ComputeTable;
import imsng.player_to_player.config.GlobalConfig;
import imsng.player_to_player.netproto.ControlConnection;
import imsng.player_to_player.netproto.ControlMessage;
import imsng.player_to_player.netproto.MessageHandler;
import imsng.player_to_player.netproto.MessageType;
import imsng.player_to_player.registry.ChunkKey;
import imsng.player_to_player.registry.ChunkRegistry;
import imsng.player_to_player.util.ThreadPools;
import net.minecraft.core.BlockPos;
import net.minecraft.nbt.CompoundTag;
import net.minecraft.nbt.ListTag;
import net.minecraft.nbt.NbtAccounter;
import net.minecraft.nbt.NbtIo;
import net.minecraft.nbt.Tag;
import net.minecraft.server.MinecraftServer;
import net.minecraft.world.level.storage.LevelResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;

/**
 * 角色指派处理器（服务端，Phase 2；对应规范"玩家加入世界"事件的角色决策段）。
 * <p>
 * 客户端环境同步完成后发 {@link MessageType#ROLE_REQUEST}，本处理器决定其主/副角色，
 * 以 {@link MessageType#ROLE_ASSIGN}（携带 _rid）应答：
 * <ol>
 *   <li>定位玩家目标位置：读服务端存档 {@code playerdata/<uuid>.dat}（Pos + Dimension；
 *       环境文件同步已把同一份数据带给客户端，两侧视图一致），无存档则用主世界出生点；</li>
 *   <li>查目标区块及其四邻的占用组（{@link ChunkRegistry}）——规范"确认玩家所需要
 *       加载的未知区块是否被加载"；</li>
 *   <li>被某个在线组占用 → 指派为该组<b>副客户端</b>（json 附 primaryClientId，
 *       客户端据此发起 P2P 预连接走隧道加入）；</li>
 *   <li>未被占用 → 指派为<b>主客户端</b>（新建组，groupId == clientId）。</li>
 * </ol>
 * <b>Phase 3 收口（规范"玩家加入世界"的算力比较段）</b>：目标区块被在线组占用时
 * 比较加入者与该组主客户端的算力 ——
 * <ul>
 *   <li>加入者<b>更强</b>（单核分更高且满足内存门槛）→ 指派为<b>主客户端</b>
 *       （自建新组）。其集成服务端随后的区块申请必然被占用组挡住 →
 *       {@code MergeTriggers} → MERGE_REQUEST → {@code MergeCoordinator} 按算力
 *       选它为新主 → 原主预同步让出 —— 恰好组合出规范"与原主客户端进行预连接
 *       以及预同步 合并为一个组客户端"的完整链路，无需专用协议；</li>
 *   <li>否则 → 副客户端加入该组（Phase 2 路径）。</li>
 * </ul>
 * <p>
 * 线程模型：handle 在 Netty 事件循环，playerdata 磁盘读转 {@link ThreadPools#io()}。
 */
public final class RoleAssignHandler implements MessageHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger("player_to_player/role");

    /** playerdata NBT 解析上限（正常玩家数据远小于此；防解压炸弹）。 */
    private static final long MAX_PLAYERDATA_BYTES = 16L * 1024 * 1024;

    private final MinecraftServer server;
    private final GlobalConfig config;
    private final ChunkRegistry registry;
    private final GroupTable groups;
    private final ComputeTable computeTable;

    public RoleAssignHandler(MinecraftServer server, GlobalConfig config, ChunkRegistry registry,
                             GroupTable groups, ComputeTable computeTable) {
        this.server = server;
        this.config = config;
        this.registry = registry;
        this.groups = groups;
        this.computeTable = computeTable;
    }

    @Override
    public void handle(ControlConnection conn, ControlMessage msg) {
        UUID peer = conn.peerId();
        if (peer == null) {
            conn.send(error(msg, "not_authenticated", "须先完成 HELLO 握手"));
            return;
        }
        // playerdata 读取是磁盘 IO，转 io 池后应答
        ThreadPools.io().execute(() -> {
            try {
                assignRole(conn, msg, peer);
            } catch (Exception e) {
                LOGGER.error("角色指派失败: {}", peer, e);
                conn.send(error(msg, "assign_failed", e.toString()));
            }
        });
    }

    /** 角色决策主流程（io 线程）。 */
    private void assignRole(ControlConnection conn, ControlMessage msg, UUID peer) {
        // ---- 1. 目标区块：玩家存档位置，无存档用主世界出生点 ----
        ChunkKey target = locatePlayerChunk(peer);

        // ---- 2. 查目标区块 + 四邻的占用组（规范：检查该区块以及周围四个区块）----
        UUID owningGroup = findOwningGroup(target, peer);

        JsonObject out = new JsonObject();
        if (owningGroup != null) {
            // ---- 3. 有在线组占用：算力比较决定加入方式（规范"玩家加入世界"）----
            UUID primary = groups.primaryOf(owningGroup);
            if (primary != null && !joinerIsStronger(peer, primary)) {
                // 加入者不更强（或算力未知）→ 副客户端加入该组（Phase 2 路径）
                if (groups.addSecondary(owningGroup, peer)) {
                    out.addProperty("role", "secondary");
                    out.addProperty("groupId", owningGroup.toString());
                    out.addProperty("primaryClientId", primary.toString());
                    conn.send(msg.reply(MessageType.ROLE_ASSIGN, out, null));
                    LOGGER.info("角色指派: {} → 副客户端（组 {}，目标区块 {}）",
                            peer, owningGroup, target.asString());
                    return;
                }
                // 组表与注册表短暂不一致（主客户端刚掉线等）：落到建组路径
                LOGGER.warn("区块 {} 的占用组 {} 已不在线，改走主客户端指派",
                        target.asString(), owningGroup);
            } else if (primary != null) {
                // 加入者更强 → 主客户端建组（Phase 3）：其后续区块申请被占用组
                // 挡住时自动触发 MERGE_REQUEST，合并选主必选它 —— 组合出规范
                // "加入玩家算力更强则与原主预连接预同步合并"的完整链路
                LOGGER.info("加入者 {} 算力强于占用组 {} 的主客户端 {}，"
                        + "指派为主客户端（合并流程将随区块申请自动触发）",
                        peer, owningGroup, primary);
            }
        }

        // ---- 4. 无人占用 → 主客户端（规范：主客户端资格含剩余内存门槛）----
        ComputeScore score = computeTable.get(peer);
        if (score == null) {
            LOGGER.warn("客户端 {} 未上报算力，仍指派为主客户端（无其他候选）", peer);
        } else if (score.freeMemoryBytes() < config.minFreeMemoryBytes) {
            // 该区域没有别的组可加入，只能降级放行 —— Phase 3 合并成熟后可改为拒绝
            LOGGER.warn("客户端 {} 可用内存 {}MB 低于主客户端门槛 {}MB，降级放行",
                    peer, score.freeMemoryBytes() / (1024 * 1024),
                    config.minFreeMemoryBytes / (1024 * 1024));
        }
        groups.createGroup(peer);
        out.addProperty("role", "primary");
        out.addProperty("groupId", peer.toString());
        out.addProperty("primaryClientId", peer.toString());
        conn.send(msg.reply(MessageType.ROLE_ASSIGN, out, null));
        LOGGER.info("角色指派: {} → 主客户端（目标区块 {}）", peer, target.asString());
    }

    /**
     * 加入者是否算力更强（规范"如果加入玩家的算力更强则将该玩家作为主客户端"）：
     * 用 {@link ComputeTable#selectPrimary} 的同一套规则（单核分 + 内存门槛 +
     * UUID 决胜）在两人之间选主，胜者恰为加入者才算"更强" —— 与合并选主
     * （MergeCoordinator）口径完全一致，两处决策永不分叉。
     */
    private boolean joinerIsStronger(UUID joiner, UUID incumbentPrimary) {
        UUID winner = computeTable.selectPrimary(
                java.util.List.of(joiner, incumbentPrimary), config.minFreeMemoryBytes);
        return joiner.equals(winner);
    }

    /**
     * 查目标区块及其四邻的占用组；只认<b>在线</b>组（组表存在且主客户端连接活跃）。
     * 排除请求方自己残留的占用（快速重进时注册表可能还挂着旧占用，等断连清理）。
     */
    private UUID findOwningGroup(ChunkKey target, UUID peer) {
        UUID owner = onlineOwnerOf(target, peer);
        if (owner != null) {
            return owner;
        }
        for (ChunkKey neighbor : target.neighbors4()) {
            owner = onlineOwnerOf(neighbor, peer);
            if (owner != null) {
                return owner;
            }
        }
        return null;
    }

    /** 单区块的在线占用组；空闲/离线/是请求方自己则返回 null。 */
    private UUID onlineOwnerOf(ChunkKey key, UUID peer) {
        UUID owner = registry.ownerOf(key);
        if (owner == null || owner.equals(peer) || !groups.exists(owner)) {
            return null;
        }
        UUID primary = groups.primaryOf(owner);
        if (primary == null) {
            return null; // 查询间隙组恰被解散
        }
        ControlConnection primaryConn = HelloHandler.connectionOf(primary);
        return primaryConn != null && primaryConn.isOpen() ? owner : null;
    }

    /**
     * 定位玩家目标区块：读 {@code playerdata/<uuid>.dat} 的 Pos/Dimension；
     * 文件缺失或解析失败回退主世界出生点（首次加入的新玩家）。
     */
    private ChunkKey locatePlayerChunk(UUID player) {
        Path dataFile = server.getWorldPath(LevelResource.PLAYER_DATA_DIR)
                .resolve(player.toString() + ".dat");
        try {
            if (Files.isRegularFile(dataFile)) {
                CompoundTag tag = NbtIo.readCompressed(dataFile,
                        NbtAccounter.create(MAX_PLAYERDATA_BYTES));
                ListTag pos = tag.getList("Pos", Tag.TAG_DOUBLE);
                String dimension = tag.getString("Dimension");
                if (pos.size() == 3 && !dimension.isEmpty()) {
                    int chunkX = ((int) Math.floor(pos.getDouble(0))) >> 4;
                    int chunkZ = ((int) Math.floor(pos.getDouble(2))) >> 4;
                    return new ChunkKey(dimension, chunkX, chunkZ);
                }
            }
        } catch (Exception e) {
            LOGGER.warn("玩家存档解析失败，按出生点处理: {}", dataFile, e);
        }
        BlockPos spawn = server.overworld().getSharedSpawnPos();
        return new ChunkKey(server.overworld().dimension().location().toString(),
                spawn.getX() >> 4, spawn.getZ() >> 4);
    }

    /** 构造保留 _rid 的 ERROR 应答。 */
    private static ControlMessage error(ControlMessage request, String code, String message) {
        JsonObject out = new JsonObject();
        out.addProperty("code", code);
        out.addProperty("message", message);
        return request.reply(MessageType.ERROR, out, null);
    }
}
