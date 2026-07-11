package imsng.player_to_player.group;

import com.google.gson.JsonObject;
import imsng.player_to_player.netproto.ControlConnection;
import imsng.player_to_player.netproto.ControlMessage;
import imsng.player_to_player.netproto.MessageType;
import imsng.player_to_player.registry.ChunkKey;
import imsng.player_to_player.util.JsonUtil;
import net.minecraft.core.BlockPos;
import net.minecraft.core.registries.Registries;
import net.minecraft.resources.ResourceKey;
import net.minecraft.resources.ResourceLocation;
import net.minecraft.server.level.ServerLevel;
import net.minecraft.server.level.ServerPlayer;
import net.minecraft.world.level.Level;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 传送门预检器（主客户端侧，Phase 4；规范"玩家进入传送门：当玩家进入传送门时
 * 传送门对应的区块开始提前加载另外一个维度的未知区块检查是否被其他组加载……
 * 如果被其他组加载则进行合并"）。
 * <p>
 * <b>与加载门控的分工</b>：跨维度加载的<b>正确性</b>由既有门控保证 ——
 * {@code ChunkMapMixin.readChunk} 对被接管集成服务端的<b>所有维度</b>生效
 * （ChunkKey 含维度），换维度时目的地区块照常走"申请-授予"，被拒则悬置并触发
 * 合并。本类做的是规范要求的"提前"半拍：玩家<b>站进下界传送门</b>（4 秒等待窗口
 * 开始，Entity.handleInsidePortal 驱动）时就用只读探测查询目的维度落点归属，
 * 被其他组占用 → 立刻打给 {@link MergeTriggers}（与区块申请被拒同一入口）——
 * 传送门的等待窗口正好给预连接/预同步当缓冲，玩家穿过门时合并大概率已完成，
 * 目的地不再卡授予。
 * <p>
 * <b>为何用探测而非预申请</b>：claimWithRetry 会真的占下区块 —— 玩家若中途退出
 * 传送门，那个永远不会被加载的占用也永远不会随卸载释放（泄漏）。只读探测
 * （CHUNK_PROBE_REQUEST）无副作用。末地传送门无等待窗口（接触即传），
 * 无"提前"可言，由门控兜底，不在此处理。
 * <p>
 * 线程模型：{@link #onPlayerInPortal} 在服务器主线程（Entity tick 内），只做
 * 限速查表 + 坐标换算；探测请求异步，回调在 Netty 线程（只打触发信号，不阻塞）。
 */
public final class PortalPreloader {

    private static final Logger LOGGER = LoggerFactory.getLogger("player_to_player/portal");

    /** 同一玩家两次预检的最小间隔（毫秒）：站在门里每 tick 都会触发事件，须限速。 */
    private static final long PROBE_INTERVAL_MILLIS = 5_000;

    /** 目的地探测半径（区块）：落点区块 + 一圈（传送门搜索半径 16 格 ≈ 1 区块）。 */
    private static final int PROBE_RADIUS_CHUNKS = 1;

    /** 限速表：玩家 UUID → 上次预检时刻。 */
    private static final Map<UUID, Long> lastProbe = new ConcurrentHashMap<>();

    private PortalPreloader() {
    }

    /** 世界会话拆除时清理状态（幂等）。 */
    public static void reset() {
        lastProbe.clear();
    }

    /**
     * 玩家站进下界传送门（EntityPortalMixin 驱动，服务器主线程；
     * 调用方已确认是被接管集成服务端上的 ServerPlayer）。
     */
    public static void onPlayerInPortal(ServerPlayer player) {
        ControlConnection conn = GroupRuntime.conn();
        UUID groupId = GroupRuntime.activeGroupId();
        if (conn == null || !conn.isOpen() || groupId == null) {
            return;
        }
        long now = System.currentTimeMillis();
        Long last = lastProbe.get(player.getUUID());
        if (last != null && now - last < PROBE_INTERVAL_MILLIS) {
            return;
        }
        lastProbe.put(player.getUUID(), now);

        // 目的维度与坐标换算（原版规则：主世界↔下界按 coordinateScale 比值缩放，
        // 主世界 1.0 / 下界 8.0 —— 已用 javap 核实 DimensionType.coordinateScale()）
        ServerLevel from = player.serverLevel();
        ResourceKey<Level> targetKey = from.dimension() == Level.NETHER
                ? Level.OVERWORLD : Level.NETHER;
        ServerLevel target = from.getServer().getLevel(targetKey);
        if (target == null) {
            return;
        }
        double scale = from.dimensionType().coordinateScale()
                / target.dimensionType().coordinateScale();
        BlockPos pos = player.blockPosition();
        int destChunkX = ((int) Math.floor(pos.getX() * scale)) >> 4;
        int destChunkZ = ((int) Math.floor(pos.getZ() * scale)) >> 4;
        String dimension = targetKey.location().toString();

        // 落点区块 + 一圈的只读探测；任何一格被其他组占用 → 提前触发预连接/合并
        for (int dx = -PROBE_RADIUS_CHUNKS; dx <= PROBE_RADIUS_CHUNKS; dx++) {
            for (int dz = -PROBE_RADIUS_CHUNKS; dz <= PROBE_RADIUS_CHUNKS; dz++) {
                probe(conn, groupId, new ChunkKey(dimension, destChunkX + dx, destChunkZ + dz));
            }
        }
        LOGGER.debug("传送门预检: 玩家 {} → {} 区块({},{})",
                player.getGameProfile().getName(), dimension, destChunkX, destChunkZ);
    }

    /** 单区块只读探测（异步；被其他组阻塞则打合并触发信号）。 */
    private static void probe(ControlConnection conn, UUID groupId, ChunkKey key) {
        JsonObject json = new JsonObject();
        json.addProperty("dimension", key.dimension());
        json.addProperty("x", key.x());
        json.addProperty("z", key.z());
        json.addProperty("groupId", groupId.toString());
        conn.request(ControlMessage.of(MessageType.CHUNK_PROBE_REQUEST, json))
                .whenComplete((resp, err) -> {
                    if (err != null || resp == null
                            || resp.type() != MessageType.CHUNK_PROBE_RESPONSE
                            || !JsonUtil.getBoolean(resp.json(), "blocked", false)) {
                        return; // 空闲/探测失败：等玩家真正穿门时由门控兜底
                    }
                    try {
                        UUID blockingGroup = UUID.fromString(
                                JsonUtil.getString(resp.json(), "blockingGroup", ""));
                        if (!blockingGroup.equals(groupId)) {
                            // 与区块申请被拒同一入口：MergeClient 决定是否发起合并
                            // （抑制窗口/去重都在下游，此处只管报信）
                            MergeTriggers.onClaimBlocked(blockingGroup, key);
                        }
                    } catch (IllegalArgumentException ignored) {
                        // blockingGroup 非法（入站不可信）：忽略
                    }
                });
    }
}
