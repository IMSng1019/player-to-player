package imsng.player_to_player.server;

import com.google.gson.JsonObject;
import imsng.player_to_player.netproto.ControlConnection;
import imsng.player_to_player.netproto.ControlMessage;
import imsng.player_to_player.netproto.HandlerRegistry;
import imsng.player_to_player.netproto.MessageType;
import imsng.player_to_player.util.JsonUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

/**
 * 末影珍珠特殊加载的服务端路由（Phase 4；规范"末影珍珠"）。
 * <p>
 * 服务端只做<b>转发</b>，不解析珍珠 NBT（binary 原样透传）：
 * <ul>
 *   <li>{@code PEARL_HANDOFF}：抛出者所在组 → 目标组主客户端。珍珠即将飞入
 *       被其他组占用/缓冲的区块时，把实体整体交给对方演算（规范"将末影珍珠的
 *       向量数据交给其他组客户端计算"，避免为一颗珍珠触发合并）。
 *       应答 handedOff 告知发起方是否可以安全丢弃本地实体 —— 目标组不在线时
 *       必须应答 false，否则珍珠凭空消失；</li>
 *   <li>{@code PEARL_LANDED}：落点组 → 抛出者所在组主客户端。规范"当末影珍珠
 *       落地后则看做一个小型的 tp 指令"——抛出者的主客户端把玩家传送到落点，
 *       随后的合并/分离由加载门控与分离监视自然接管。</li>
 * </ul>
 * 线程模型：纯查表 + 转发，Netty 事件循环直接处理。
 */
public final class PearlHandlers {

    private static final Logger LOGGER = LoggerFactory.getLogger("player_to_player/pearl");

    /** 珍珠 NBT 大小上限（入站不可信；一颗珍珠的 NBT 只有几百字节）。 */
    private static final int MAX_PEARL_NBT_BYTES = 64 * 1024;

    private final GroupTable groups;

    private PearlHandlers(GroupTable groups) {
        this.groups = groups;
    }

    /** 注册 PEARL_HANDOFF / PEARL_LANDED 路由。 */
    public static void register(HandlerRegistry reg, GroupTable groups) {
        PearlHandlers handlers = new PearlHandlers(groups);
        reg.on(MessageType.PEARL_HANDOFF, handlers::handleHandoff);
        reg.on(MessageType.PEARL_LANDED, handlers::handleLanded);
    }

    // ------------------------------------------------------------ 交接转发

    private void handleHandoff(ControlConnection conn, ControlMessage msg) {
        UUID requester = conn.peerId();
        UUID targetGroup = parseUuid(JsonUtil.getString(msg.json(), "targetGroupId", ""));
        byte[] nbt = msg.binary();
        if (requester == null || targetGroup == null
                || nbt == null || nbt.length == 0 || nbt.length > MAX_PEARL_NBT_BYTES) {
            replyHandedOff(conn, msg, false);
            return;
        }
        UUID targetPrimary = groups.primaryOf(targetGroup);
        ControlConnection target = targetPrimary != null
                ? HelloHandler.connectionOf(targetPrimary) : null;
        if (target == null || !target.isOpen()) {
            // 目标组已下线：发起方保留本地珍珠（在未加载区块边缘暂停，与原版一致）
            replyHandedOff(conn, msg, false);
            return;
        }
        // 原样透传（json 带抛出者身份与维度，binary 是珍珠 NBT）
        target.send(ControlMessage.of(MessageType.PEARL_HANDOFF, msg.json().deepCopy(), nbt));
        replyHandedOff(conn, msg, true);
        LOGGER.info("珍珠交接: 组 {} → 组 {} (抛出者 {})", groups.groupOf(requester), targetGroup,
                JsonUtil.getString(msg.json(), "throwerName", "?"));
    }

    private void replyHandedOff(ControlConnection conn, ControlMessage msg, boolean handedOff) {
        JsonObject out = new JsonObject();
        out.addProperty("handedOff", handedOff);
        conn.send(msg.reply(MessageType.PEARL_HANDOFF, out, null));
    }

    // ------------------------------------------------------------ 落点回程

    private void handleLanded(ControlConnection conn, ControlMessage msg) {
        UUID thrower = parseUuid(JsonUtil.getString(msg.json(), "throwerUuid", ""));
        if (conn.peerId() == null || thrower == null) {
            return; // 推送语义无应答；非法回报静默丢弃
        }
        // 抛出者当前所在组的主客户端（交接期间玩家可能又换了组，按最新组表路由）
        UUID throwerGroup = groups.groupOf(thrower);
        UUID primary = throwerGroup != null ? groups.primaryOf(throwerGroup) : null;
        ControlConnection target = primary != null ? HelloHandler.connectionOf(primary) : null;
        if (target == null || !target.isOpen()) {
            LOGGER.warn("珍珠落点无法回程: 抛出者 {} 不在线或组已解散", thrower);
            return;
        }
        target.send(ControlMessage.of(MessageType.PEARL_LANDED, msg.json().deepCopy()));
    }

    /** 防御性 UUID 解析。 */
    private static UUID parseUuid(String raw) {
        if (raw == null || raw.isBlank()) {
            return null;
        }
        try {
            return UUID.fromString(raw);
        } catch (IllegalArgumentException e) {
            return null;
        }
    }
}
