package imsng.player_to_player.netproto;

import com.google.gson.JsonObject;
import imsng.player_to_player.util.JsonUtil;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import io.netty.handler.codec.MessageToMessageDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * 控制协议帧编解码器（帧格式见 {@link ControlMessage} 类注释）。
 * <p>
 * 解码侧假定管线前置了 {@code LengthFieldBasedFrameDecoder}（按 totalLen 切帧并剥掉
 * 长度字段），本解码器只处理帧体：{@code type + jsonLen + json + binary}。
 * <p>
 * 入站数据一律按不可信处理：jsonLen 越界 / JSON 语法非法 → 记日志并关连接；
 * 未知消息类型 → 按 {@link MessageType#fromId} 的约定应答 ERROR（携带 _rid 使对端
 * request() 快速失败）而不抛异常，防止恶意帧打崩连接。
 */
final class ControlCodec {

    private static final Logger LOGGER = LoggerFactory.getLogger("player_to_player/netproto");

    private ControlCodec() {
    }

    /** 出站编码：ControlMessage → 线上帧（含 totalLen 前缀）。 */
    static final class Encoder extends MessageToByteEncoder<ControlMessage> {
        @Override
        protected void encode(ChannelHandlerContext ctx, ControlMessage msg, ByteBuf out) {
            byte[] json = JsonUtil.GSON.toJson(msg.json()).getBytes(StandardCharsets.UTF_8);
            byte[] binary = msg.binary();
            out.writeInt(8 + json.length + binary.length); // totalLen = type(4) + jsonLen(4) + json + binary
            out.writeInt(msg.type().id());
            out.writeInt(json.length);
            out.writeBytes(json);
            out.writeBytes(binary);
        }
    }

    /** 入站解码：帧体 → ControlMessage（前置 LengthFieldBasedFrameDecoder 已切好帧）。 */
    static final class Decoder extends MessageToMessageDecoder<ByteBuf> {
        @Override
        protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
            if (in.readableBytes() < 8) {
                LOGGER.warn("帧体过短({} 字节)，关闭连接: {}", in.readableBytes(), ctx.channel().remoteAddress());
                ctx.close();
                return;
            }
            int typeId = in.readInt();
            int jsonLen = in.readInt();
            if (jsonLen < 0 || jsonLen > in.readableBytes()) {
                LOGGER.warn("jsonLen 越界({}), 关闭连接: {}", jsonLen, ctx.channel().remoteAddress());
                ctx.close();
                return;
            }
            byte[] jsonBytes = new byte[jsonLen];
            in.readBytes(jsonBytes);
            byte[] binary = new byte[in.readableBytes()];
            in.readBytes(binary);

            JsonObject json;
            try {
                json = jsonLen == 0
                        ? new JsonObject()
                        : JsonUtil.parseObject(new String(jsonBytes, StandardCharsets.UTF_8));
            } catch (Exception e) {
                LOGGER.warn("JSON 段解析失败，关闭连接: {} — {}", ctx.channel().remoteAddress(), e.toString());
                ctx.close();
                return;
            }

            MessageType type = MessageType.fromId(typeId);
            if (type == null) {
                // 未知编号：应答 ERROR 并记日志（协议约定），复制 _rid 让对端请求快速失败
                JsonObject err = new JsonObject();
                err.addProperty("code", "unknown_type");
                err.addProperty("message", "未知消息类型: " + typeId);
                if (json.has(Protocol.RID_FIELD)) {
                    err.add(Protocol.RID_FIELD, json.get(Protocol.RID_FIELD));
                }
                // 必须从 channel（管线尾）发起写：本 decoder 位于 Encoder 之前，
                // 若用 ctx.writeAndFlush 则出站事件只向 head 传播、跳过 Encoder，
                // 未编码的 ControlMessage 会被 HeadContext 拒绝且 promise 静默失败
                ctx.channel().writeAndFlush(ControlMessage.of(MessageType.ERROR, err))
                        .addListener((ChannelFutureListener) f -> {
                            if (!f.isSuccess()) {
                                LOGGER.warn("ERROR 应答发送失败（目标 {}）: {}",
                                        f.channel().remoteAddress(), String.valueOf(f.cause()));
                            }
                        });
                LOGGER.warn("收到未知消息类型 {}（来自 {}），已应答 ERROR", typeId, ctx.channel().remoteAddress());
                return;
            }
            out.add(ControlMessage.of(type, json, binary));
        }
    }
}
