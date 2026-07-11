package imsng.player_to_player.netproto;

import com.google.gson.JsonObject;

import java.util.Objects;

/**
 * 控制协议消息（一帧）。
 *
 * <h2>线上帧格式</h2>
 * <pre>
 * int32 totalLen   # 后续字节总长 = 4(type) + 4(jsonLen) + jsonLen + binary.length
 * int32 type       # MessageType.id
 * int32 jsonLen    # JSON 段字节长
 * byte[jsonLen]    # UTF-8 编码的 JSON 对象（结构化字段）
 * byte[...]        # 可选二进制附件（文件块/区块数据等）
 * </pre>
 * 编解码由 netproto.codec 包实现；本类只承载数据。
 * <p>
 * JSON + 二进制分离的原因：结构化字段用 JSON 便于扩展与调试，
 * 大块数据（文件内容、区块 NBT）直接走二进制避免 Base64 膨胀 33%。
 */
public final class ControlMessage {

    /** 空二进制附件（共享常量，避免到处 new byte[0]）。 */
    public static final byte[] NO_BINARY = new byte[0];

    private final MessageType type;
    private final JsonObject json;
    private final byte[] binary;

    public ControlMessage(MessageType type, JsonObject json, byte[] binary) {
        this.type = Objects.requireNonNull(type, "type");
        this.json = json != null ? json : new JsonObject();
        this.binary = binary != null ? binary : NO_BINARY;
    }

    /** 创建只有类型的空消息（如 PING）。 */
    public static ControlMessage of(MessageType type) {
        return new ControlMessage(type, new JsonObject(), NO_BINARY);
    }

    /** 创建带 JSON 字段的消息。 */
    public static ControlMessage of(MessageType type, JsonObject json) {
        return new ControlMessage(type, json, NO_BINARY);
    }

    /** 创建带 JSON 字段与二进制附件的消息。 */
    public static ControlMessage of(MessageType type, JsonObject json, byte[] binary) {
        return new ControlMessage(type, json, binary);
    }

    public MessageType type() {
        return type;
    }

    /** JSON 段（永不为 null；空消息返回空对象）。 */
    public JsonObject json() {
        return json;
    }

    /** 二进制附件（永不为 null；无附件返回长度 0 数组）。 */
    public byte[] binary() {
        return binary;
    }

    /** 是否携带请求-响应关联号（须为数字原始值：入站 JSON 不可信，非数字 _rid 会使 {@link #rid()} 抛异常断连）。 */
    public boolean hasRid() {
        com.google.gson.JsonElement el = json.get(Protocol.RID_FIELD);
        return el != null && el.isJsonPrimitive() && el.getAsJsonPrimitive().isNumber();
    }

    /** 读取请求-响应关联号（调用前须先判断 {@link #hasRid()}）。 */
    public long rid() {
        return json.get(Protocol.RID_FIELD).getAsLong();
    }

    /**
     * 基于本消息构造应答：复制请求的 _rid 到应答 JSON，使发起方的
     * {@code ControlConnection.request()} future 得以完成。
     */
    public ControlMessage reply(MessageType replyType, JsonObject replyJson, byte[] replyBinary) {
        JsonObject out = replyJson != null ? replyJson : new JsonObject();
        if (hasRid()) {
            out.addProperty(Protocol.RID_FIELD, rid());
        }
        return new ControlMessage(replyType, out, replyBinary);
    }

    @Override
    public String toString() {
        // 日志友好：不打印二进制内容，只打印长度
        return "ControlMessage{" + type + ", json=" + json + ", binary=" + binary.length + "B}";
    }
}
