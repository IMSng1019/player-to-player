package imsng.player_to_player.server;

import imsng.player_to_player.config.P2PPaths;
import imsng.player_to_player.netproto.ControlConnection;
import imsng.player_to_player.netproto.ControlMessage;
import imsng.player_to_player.netproto.HandlerRegistry;
import imsng.player_to_player.netproto.MessageType;
import imsng.player_to_player.util.JsonUtil;
import imsng.player_to_player.util.ThreadPools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * 主客户端日志收集器（服务端）。
 * <p>
 * 规范"日志处理"：主客户端的日志需要发给服务端储存，
 * 在服务端的储存格式为 {@code 玩家名-latest.log}（存放于
 * {@code player-to-player/logs/} 目录，见 DESIGN.md 第 2、8 节）。
 * <ul>
 *   <li>{@code LOG_UPLOAD}（json: playerName, append；binary: UTF-8 日志文本）：
 *       append=true 追加到既有文件（增量上传），false 覆盖重写
 *       （对应客户端新一次启动 —— 与原版 latest.log 滚动语义一致）。</li>
 * </ul>
 * <p>
 * 线程模型：Netty 事件循环上只做字段解析，磁盘写转交 {@link ThreadPools#io()}。
 * 同一玩家的写入按到达顺序提交到 IO 池，日志乱序在 Phase 1 可容忍
 * （客户端按批次串行上传，正常情况下不会并发同名写）。
 */
public final class LogCollector {

    private static final Logger LOGGER = LoggerFactory.getLogger("player_to_player/log-collector");

    /** 单次上传的日志块大小上限（4 MB）：入站数据不可信，防止恶意大帧撑爆磁盘。 */
    private static final int MAX_LOG_CHUNK_BYTES = 4 * 1024 * 1024;

    private LogCollector() {
    }

    /**
     * 注册 LOG_UPLOAD 处理器。
     *
     * @param reg     消息分发表
     * @param logsDir 日志储存目录（{@link P2PPaths#logsDir()}）
     */
    public static void register(HandlerRegistry reg, Path logsDir) {
        reg.on(MessageType.LOG_UPLOAD, (connection, message) -> handle(connection, message, logsDir));
    }

    private static void handle(ControlConnection connection, ControlMessage message, Path logsDir) {
        String playerName = JsonUtil.getString(message.json(), "playerName", "");
        boolean append = JsonUtil.getBoolean(message.json(), "append", true);
        byte[] data = message.binary();

        if (playerName.isBlank()) {
            LOGGER.warn("丢弃缺少 playerName 的日志上传（来自 {}）", connection.peerId());
            return;
        }
        if (data.length > MAX_LOG_CHUNK_BYTES) {
            LOGGER.warn("丢弃超大日志块({} B > {} B)，来自玩家 {}",
                    data.length, MAX_LOG_CHUNK_BYTES, playerName);
            return;
        }

        // 玩家名做文件名合法化：既处理 Windows 保留字符，也防路径穿越
        //（"../x" 之类的恶意名字被 sanitize 的替换规则中和为普通字符）
        Path target = logsDir.resolve(P2PPaths.sanitize(playerName) + "-latest.log");

        // 磁盘写是阻塞操作，铁律：不得占用 Netty 事件循环，转交 IO 线程池
        ThreadPools.io().execute(() -> {
            try {
                Files.createDirectories(logsDir);
                if (append) {
                    // 追加：文件不存在则创建（客户端首块也可能标记 append）
                    Files.write(target, data,
                            StandardOpenOption.CREATE, StandardOpenOption.APPEND);
                } else {
                    // 覆盖：客户端新一次启动的首块，语义同原版 latest.log 滚动
                    Files.write(target, data,
                            StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING,
                            StandardOpenOption.WRITE);
                }
            } catch (IOException e) {
                LOGGER.error("写入玩家日志失败: {}", target, e);
            }
        });
    }
}
