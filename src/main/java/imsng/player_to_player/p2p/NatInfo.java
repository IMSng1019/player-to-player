package imsng.player_to_player.p2p;

import com.google.gson.JsonObject;
import imsng.player_to_player.util.JsonUtil;

/**
 * NAT 探测结果（模组加载时测得，HELLO 握手时上报服务端，
 * 服务端在 P2P_ENDPOINT_EXCHANGE 时转发给打洞对端）。
 *
 * @param type       NAT 类型
 * @param publicIp   STUN 观测到的公网 IP（探测失败为空串）
 * @param publicPort STUN 观测到的公网端口（探测失败为 0）
 * @param localPort  本机 P2P UDP socket 绑定的本地端口
 */
public record NatInfo(NatType type, String publicIp, int publicPort, int localPort) {

    /** 未探测状态的占位值。 */
    public static final NatInfo UNKNOWN = new NatInfo(NatType.UNKNOWN, "", 0, 0);

    public JsonObject toJson() {
        JsonObject obj = new JsonObject();
        obj.addProperty("type", type.name());
        obj.addProperty("publicIp", publicIp);
        obj.addProperty("publicPort", publicPort);
        obj.addProperty("localPort", localPort);
        return obj;
    }

    public static NatInfo fromJson(JsonObject obj) {
        NatType type;
        try {
            type = NatType.valueOf(JsonUtil.getString(obj, "type", NatType.UNKNOWN.name()));
        } catch (IllegalArgumentException e) {
            type = NatType.UNKNOWN; // 容忍未知枚举值（跨版本兼容）
        }
        return new NatInfo(
                type,
                JsonUtil.getString(obj, "publicIp", ""),
                JsonUtil.getInt(obj, "publicPort", 0),
                JsonUtil.getInt(obj, "localPort", 0));
    }
}
