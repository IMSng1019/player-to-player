package imsng.player_to_player.compute;

import imsng.player_to_player.netproto.HandlerRegistry;
import imsng.player_to_player.netproto.MessageType;
import imsng.player_to_player.util.JsonUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

/**
 * 算力子系统的控制协议消息处理器（服务端侧）。
 * <p>
 * 规范出处：player_to_player-prompt.txt "玩家加入世界：玩家向服务端给出算力能力"；
 * 协议约定：COMPUTE_REPORT 的 JSON = {@code clientId} + {@link ComputeScore#toJson()} 平铺
 * （即 cpuModel / singleCoreScore / source / freeMemoryBytes / totalMemoryBytes
 * 与 clientId 同级）。
 * <p>
 * <b>线程模型</b>：handler 在 Netty 事件循环上执行 —— 只做 JSON 解析与
 * ConcurrentHashMap 写入（{@link ComputeTable#report}），均为非阻塞轻量操作，
 * 无需转线程池。
 */
public final class ComputeHandlers {

    private static final Logger LOGGER = LoggerFactory.getLogger("player_to_player/compute");

    private ComputeHandlers() {
    }

    /**
     * 把算力上报处理器注册到分发表（服务端 ControlServer 启动时调用）。
     *
     * @param reg   处理器注册表（ControlServer）
     * @param table 服务端算力表
     */
    public static void register(HandlerRegistry reg, ComputeTable table) {
        reg.on(MessageType.COMPUTE_REPORT, (connection, message) -> {
            // 入站数据不可信：clientId 缺失或非法 UUID 一律丢弃并记日志，绝不抛异常打崩事件循环
            String rawClientId = JsonUtil.getString(message.json(), "clientId", null);
            UUID clientId = parseUuid(rawClientId);
            if (clientId == null) {
                LOGGER.warn("COMPUTE_REPORT 缺失或非法 clientId，已丢弃: from={} raw={}",
                        connection.remoteAddress(), rawClientId);
                return;
            }
            // ComputeScore 字段与 clientId 平铺在同一 JSON 对象里，fromJson 对缺失字段自带安全默认值
            ComputeScore score = ComputeScore.fromJson(message.json());
            table.report(clientId, score);
            LOGGER.info("收到算力上报: clientId={} cpu={} singleCore={} source={} freeMem={}MB",
                    clientId, score.cpuModel(), score.singleCoreScore(), score.source(),
                    score.freeMemoryBytes() / (1024 * 1024));
        });
    }

    /** 防御性 UUID 解析：null / 格式非法返回 null 而非抛异常。 */
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
