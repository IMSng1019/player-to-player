package imsng.player_to_player.group;

import com.google.gson.JsonObject;
import imsng.player_to_player.netproto.ControlConnection;
import imsng.player_to_player.netproto.ControlMessage;
import imsng.player_to_player.netproto.HandlerRegistry;
import imsng.player_to_player.netproto.MessageType;
import imsng.player_to_player.registry.ChunkKey;
import imsng.player_to_player.util.JsonUtil;
import imsng.player_to_player.util.ThreadPools;
import net.minecraft.core.registries.Registries;
import net.minecraft.nbt.CompoundTag;
import net.minecraft.nbt.NbtAccounter;
import net.minecraft.nbt.NbtIo;
import net.minecraft.resources.ResourceKey;
import net.minecraft.resources.ResourceLocation;
import net.minecraft.server.MinecraftServer;
import net.minecraft.server.level.ServerLevel;
import net.minecraft.server.level.ServerPlayer;
import net.minecraft.world.entity.Entity;
import net.minecraft.world.entity.EntityType;
import net.minecraft.world.entity.projectile.ThrownEnderpearl;
import net.minecraft.world.phys.Vec3;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 末影珍珠特殊加载（主客户端侧，Phase 4；规范"末影珍珠"）。
 * <p>
 * 规范规则：珍珠经过的区块 ①被本组加载 → 正常演算；②被其他组加载 → "将末影
 * 珍珠的向量数据交给其他组客户端计算"；③未被加载但处于其他组加载区块的
 * 周围四格（缓冲层，本组永远申请不下来）→ 同样交给该组；④其余 → 抛出者计算
 * （原版语义：区块未加载则珍珠暂停）。整套方案的目的（规范原话）是
 * "使合并次数大量减少"——不为一颗过路珍珠触发预连接/合并。
 * <p>
 * 实现：
 * <ul>
 *   <li><b>出界检测</b>（{@link #onPearlTick}，ThrownEnderpearlMixin 每 tick 驱动）：
 *       珍珠下一 tick 将进入的区块不属于本组时，用 {@code CHUNK_PROBE_REQUEST}
 *       （只读探测，四邻检查天然覆盖规则③的缓冲层判定）问服务端归属 ——
 *       被其他组占用/缓冲 → 交接：珍珠 NBT 整体（位置/速度/Tags）经服务端转发给
 *       目标组，对方确认（handedOff=true）后本地 discard；探测/交接期间珍珠
 *       飞入未加载区块自然暂停，与原版行为一致，无需冻结逻辑；</li>
 *   <li><b>注入</b>（PEARL_HANDOFF 入站）：在对应维度重生成珍珠（UUID 重掷防碰撞，
 *       打 {@value #REMOTE_TAG} 记号 + 抛出者 UUID 记号 —— 用原版命令 Tag 而非
 *       附加数据结构，NBT 序列化天然携带）继续飞行；</li>
 *   <li><b>落点回报</b>（{@link #onRemoteHit}，ThrownEnderpearlMixin onHit 驱动）：
 *       远端珍珠落地 → PEARL_LANDED 经服务端回程给抛出者所在组；</li>
 *   <li><b>小型 tp</b>（PEARL_LANDED 入站）：规范"当末影珍珠落地后则看做一个小型
 *       的 tp 指令"——把抛出者传送到落点（可跨维度）+ 原版 5 点摔落伤害；落点
 *       属其他组时，加载门控/分离监视的既有机制接管合并或分离。</li>
 * </ul>
 * 线程模型：tick 检测在服务器主线程（只做矩形/查表 + 限速，微秒级）；探测与
 * 交接请求在 Netty/io 线程，实体操作一律回服务器主线程。
 */
public final class PearlHandoff {

    private static final Logger LOGGER = LoggerFactory.getLogger("player_to_player/pearl");

    /** 远端注入珍珠的命令 Tag 记号（落地时据此回报而非本地传送）。 */
    public static final String REMOTE_TAG = "p2p_remote_pearl";

    /** 抛出者 UUID 记号前缀（命令 Tag 随 NBT 序列化跨端携带）。 */
    public static final String OWNER_TAG_PREFIX = "p2p_pearl_owner:";

    /** 同一珍珠两次归属探测的最小间隔（毫秒）：探测在途/自由区块时不刷请求。 */
    private static final long PROBE_INTERVAL_MILLIS = 1_000;

    /** 珍珠 NBT 大小上限（与服务端 PearlHandlers 口径一致）。 */
    private static final long MAX_PEARL_NBT_BYTES = 64 * 1024;

    /** 探测限速表：珍珠实体 UUID → 上次探测时刻。 */
    private static final Map<UUID, Long> probeCooldown = new ConcurrentHashMap<>();

    /** 交接已在途的珍珠（防重复交接；主线程置位/摘除）。 */
    private static final Map<UUID, Boolean> handingOff = new ConcurrentHashMap<>();

    private PearlHandoff() {
    }

    /** 世界会话拆除时清理状态（幂等）。 */
    public static void reset() {
        probeCooldown.clear();
        handingOff.clear();
    }

    // ------------------------------------------------------------ 出界检测（Mixin 入口）

    /**
     * 珍珠 tick 前的出界检测（服务器主线程，ThrownEnderpearlMixin 驱动；
     * 调用方已确认 level 是被接管集成服务端的维度）。
     */
    public static void onPearlTick(ThrownEnderpearl pearl, ServerLevel level) {
        if (pearl.getTags().contains(REMOTE_TAG)) {
            return; // 远端注入的珍珠：本地正常演算，落点走 onRemoteHit 回报
        }
        ChunkClaimClient claims = GroupRuntime.claims();
        ControlConnection conn = GroupRuntime.conn();
        UUID groupId = GroupRuntime.activeGroupId();
        if (claims == null || conn == null || !conn.isOpen() || groupId == null) {
            return;
        }
        // 下一 tick 的位置（位置 + 速度）所在区块；仍属本组则无事发生
        Vec3 next = pearl.position().add(pearl.getDeltaMovement());
        String dimension = level.dimension().location().toString();
        // floor 后右移 4 = 区块坐标（负坐标正确取整）
        ChunkKey nextKey = new ChunkKey(dimension,
                ((int) Math.floor(next.x)) >> 4, ((int) Math.floor(next.z)) >> 4);
        if (claims.isClaimed(nextKey)) {
            return; // 规则①：本组加载，正常
        }
        UUID pearlId = pearl.getUUID();
        if (handingOff.containsKey(pearlId)) {
            return; // 交接判定/在途：等结果
        }
        long now = System.currentTimeMillis();
        Long last = probeCooldown.get(pearlId);
        if (last != null && now - last < PROBE_INTERVAL_MILLIS) {
            return;
        }
        probeCooldown.put(pearlId, now);
        // 只读探测归属（四邻检查覆盖规则③的缓冲层）；期间珍珠进未加载区块自然暂停
        JsonObject json = new JsonObject();
        json.addProperty("dimension", nextKey.dimension());
        json.addProperty("x", nextKey.x());
        json.addProperty("z", nextKey.z());
        json.addProperty("groupId", groupId.toString());
        MinecraftServer server = level.getServer();
        conn.request(ControlMessage.of(MessageType.CHUNK_PROBE_REQUEST, json))
                .whenComplete((resp, err) -> {
                    if (err != null || resp == null
                            || resp.type() != MessageType.CHUNK_PROBE_RESPONSE
                            || !JsonUtil.getBoolean(resp.json(), "blocked", false)) {
                        return; // 规则④：自由区块（或探测失败）→ 抛出者保留计算
                    }
                    String blocking = JsonUtil.getString(resp.json(), "blockingGroup", "");
                    try {
                        UUID blockingGroup = UUID.fromString(blocking);
                        if (!blockingGroup.equals(groupId)) {
                            // 规则②/③：被其他组占用或处于其缓冲层 → 回主线程交接
                            server.execute(() -> handOff(pearl, level, conn, blockingGroup));
                        }
                    } catch (IllegalArgumentException ignored) {
                        // blockingGroup 非法（入站不可信）：按自由区块处理
                    }
                });
    }

    /** 交接一颗珍珠给目标组（服务器主线程：序列化 + 打记号；发送与确认转 io）。 */
    private static void handOff(ThrownEnderpearl pearl, ServerLevel level,
                                ControlConnection conn, UUID targetGroup) {
        if (pearl.isRemoved() || handingOff.putIfAbsent(pearl.getUUID(), true) != null) {
            return; // 已落地/已在交接
        }
        // 抛出者身份走命令 Tag（随 NBT 跨端携带；接收端玩家缺席时 Owner 字段解析不出）
        Entity owner = pearl.getOwner();
        UUID throwerUuid = owner != null ? owner.getUUID() : null;
        String throwerName = owner instanceof ServerPlayer p
                ? p.getGameProfile().getName() : "?";
        if (throwerUuid != null) {
            pearl.addTag(OWNER_TAG_PREFIX + throwerUuid);
        }
        CompoundTag tag = new CompoundTag();
        if (!pearl.save(tag)) {
            handingOff.remove(pearl.getUUID());
            return; // 序列化失败（乘骑等边界）：保留本地珍珠
        }
        UUID pearlId = pearl.getUUID();
        ThreadPools.io().execute(() -> {
            byte[] gzip = ChunkUploadService.compress(tag);
            if (gzip == null) {
                handingOff.remove(pearlId);
                return;
            }
            JsonObject json = new JsonObject();
            json.addProperty("targetGroupId", targetGroup.toString());
            json.addProperty("dimension", level.dimension().location().toString());
            json.addProperty("throwerUuid", throwerUuid != null ? throwerUuid.toString() : "");
            json.addProperty("throwerName", throwerName);
            conn.request(ControlMessage.of(MessageType.PEARL_HANDOFF, json, gzip))
                    .whenComplete((resp, err) -> {
                        boolean handedOff = err == null && resp != null
                                && resp.type() == MessageType.PEARL_HANDOFF
                                && JsonUtil.getBoolean(resp.json(), "handedOff", false);
                        MinecraftServer server = level.getServer();
                        server.execute(() -> {
                            handingOff.remove(pearlId);
                            if (handedOff && !pearl.isRemoved()) {
                                // 对方已接手：本地实体退场（先确认后丢弃，杜绝珍珠凭空消失）
                                pearl.discard();
                                LOGGER.info("珍珠已交接给组 {}（抛出者 {}）", targetGroup, throwerName);
                            }
                        });
                    });
        });
    }

    // ------------------------------------------------------------ 落点回报（Mixin 入口）

    /**
     * 远端珍珠落地（服务器主线程，ThrownEnderpearlMixin onHit HEAD 驱动）。
     *
     * @return true = 这是远端注入的珍珠，已回报落点，调用方应取消原版 onHit
     *         （原版路径拿不到抛出者实体，只会白白 discard —— 由本方 discard 并回报）
     */
    public static boolean onRemoteHit(ThrownEnderpearl pearl, ServerLevel level) {
        if (!pearl.getTags().contains(REMOTE_TAG)) {
            return false;
        }
        ControlConnection conn = GroupRuntime.conn();
        // 抛出者 UUID 从命令 Tag 解出（注入时随 NBT 携带）
        UUID throwerUuid = null;
        for (String tag : pearl.getTags()) {
            if (tag.startsWith(OWNER_TAG_PREFIX)) {
                try {
                    throwerUuid = UUID.fromString(tag.substring(OWNER_TAG_PREFIX.length()));
                } catch (IllegalArgumentException ignored) {
                    // 记号损坏：按无主处理
                }
                break;
            }
        }
        if (conn != null && conn.isOpen() && throwerUuid != null) {
            JsonObject json = new JsonObject();
            json.addProperty("throwerUuid", throwerUuid.toString());
            json.addProperty("dimension", level.dimension().location().toString());
            json.addProperty("x", pearl.getX());
            json.addProperty("y", pearl.getY());
            json.addProperty("z", pearl.getZ());
            conn.send(ControlMessage.of(MessageType.PEARL_LANDED, json));
        }
        pearl.discard();
        return true;
    }

    // ------------------------------------------------------------ 入站处理

    /** 注册入站 PEARL_HANDOFF / PEARL_LANDED 处理器（世界会话建立时挂接）。 */
    public static void register(HandlerRegistry reg) {
        // 其他组交来的珍珠：在本组集成服务端重生成继续飞行
        reg.on(MessageType.PEARL_HANDOFF, (conn, msg) -> {
            MinecraftServer server = GroupRuntime.server();
            byte[] gzip = msg.binary();
            if (server == null || gzip == null || gzip.length == 0
                    || gzip.length > MAX_PEARL_NBT_BYTES) {
                return;
            }
            String dimension = JsonUtil.getString(msg.json(), "dimension", "");
            ThreadPools.io().execute(() -> {
                CompoundTag tag;
                try {
                    tag = NbtIo.readCompressed(new ByteArrayInputStream(gzip),
                            NbtAccounter.create(MAX_PEARL_NBT_BYTES));
                } catch (Exception e) {
                    LOGGER.warn("交接珍珠 NBT 解析失败，丢弃", e);
                    return;
                }
                server.execute(() -> injectPearl(server, dimension, tag));
            });
        });

        // 本组抛出的珍珠在远端落地：小型 tp（规范"看做一个小型的 tp 指令"）
        reg.on(MessageType.PEARL_LANDED, (conn, msg) -> {
            MinecraftServer server = GroupRuntime.server();
            if (server == null) {
                return;
            }
            String throwerRaw = JsonUtil.getString(msg.json(), "throwerUuid", "");
            String dimension = JsonUtil.getString(msg.json(), "dimension", "");
            double x = JsonUtil.getDouble(msg.json(), "x", Double.NaN);
            double y = JsonUtil.getDouble(msg.json(), "y", Double.NaN);
            double z = JsonUtil.getDouble(msg.json(), "z", Double.NaN);
            if (throwerRaw.isEmpty() || dimension.isEmpty()
                    || Double.isNaN(x) || Double.isNaN(y) || Double.isNaN(z)) {
                return; // 入站不可信：字段不全丢弃
            }
            UUID throwerUuid;
            try {
                throwerUuid = UUID.fromString(throwerRaw);
            } catch (IllegalArgumentException e) {
                return;
            }
            server.execute(() -> {
                ServerPlayer player = server.getPlayerList().getPlayer(throwerUuid);
                if (player == null) {
                    return; // 抛出者已下线：珍珠作废（与原版死亡后落地一致）
                }
                ServerLevel level = resolveLevel(server, dimension);
                if (level == null) {
                    return;
                }
                // 原版 onHit 语义：传送 + 5 点摔落伤害；落点属其他组时，随后的
                // 区块申请被拒 → 预连接/合并，或渲染区无交集 → 分离，均由既有机制接管
                player.teleportTo(level, x, y, z, player.getYRot(), player.getXRot());
                player.hurt(player.damageSources().fall(), 5.0F);
                LOGGER.info("珍珠落点 tp: 玩家 {} → {} ({}, {}, {})",
                        throwerUuid, dimension, (int) x, (int) y, (int) z);
            });
        });
    }

    /** 重生成交接来的珍珠（服务器主线程）。 */
    private static void injectPearl(MinecraftServer server, String dimension, CompoundTag tag) {
        ServerLevel level = resolveLevel(server, dimension);
        if (level == null) {
            return;
        }
        Entity entity = EntityType.loadEntityRecursive(tag, level, e -> e);
        if (!(entity instanceof ThrownEnderpearl pearl)) {
            LOGGER.warn("交接载荷不是末影珍珠实体，丢弃（入站不可信）");
            return;
        }
        pearl.setUUID(UUID.randomUUID()); // 防 UUID 撞车（对端 discard 与本端注入非原子）
        pearl.addTag(REMOTE_TAG);
        if (!level.addFreshEntity(pearl)) {
            LOGGER.warn("交接珍珠注入失败: {}", dimension);
        }
    }

    /** 维度名 → ServerLevel（入站不可信：非法名返回 null）。 */
    private static ServerLevel resolveLevel(MinecraftServer server, String dimension) {
        try {
            return server.getLevel(
                    ResourceKey.create(Registries.DIMENSION, new ResourceLocation(dimension)));
        } catch (Exception e) {
            return null;
        }
    }
}
