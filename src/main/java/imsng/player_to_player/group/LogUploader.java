package imsng.player_to_player.group;

import com.google.gson.JsonObject;
import imsng.player_to_player.netproto.ControlConnection;
import imsng.player_to_player.netproto.ControlMessage;
import imsng.player_to_player.netproto.MessageType;
import imsng.player_to_player.util.ThreadPools;
import net.fabricmc.loader.api.FabricLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * 主客户端日志上传器（Phase 4；规范"日志处理：主客户端的日志需要发给服务端储存
 * 储存格式为 玩家名-lastest.log"）。
 * <p>
 * 增量尾随本端游戏日志 {@code logs/latest.log}（log4j 持续写入的同一份文件 ——
 * "所有端都需要像原版一样有 latest.log"由原版自身保证，本类只做上行）：
 * <ul>
 *   <li>接管开始（{@link #start}）后每 {@value #UPLOAD_INTERVAL_SECONDS}s 读一次
 *       新增字节，经 {@code LOG_UPLOAD} 上行（服务端 LogCollector 落到
 *       {@code logs/<玩家名>-latest.log}）；</li>
 *   <li>首次上传 append=false（服务端截断旧会话内容，对齐 latest.log 的
 *       "本次会话"语义）且只回溯最近 {@value #FIRST_UPLOAD_TAIL_BYTES} 字节
 *       （长会话的陈年日志没有回收价值）；后续 append=true 增量追加；</li>
 *   <li>文件长度变小视为轮转（新会话重开），重置游标并重新截断上传。</li>
 * </ul>
 * 中转服务端的日志规范要求"由自己储存"（原版 latest.log 即是），服务端储存
 * 自己与主客户端的日志 —— 两者在本类之外天然成立。
 * <p>
 * 线程模型：scheduler 定时触发，文件读取与发送整体在 io 池（读的是 log4j 正在
 * 写的文件，Windows 下共享读打开没有锁冲突；读失败跳过本轮，绝不抛出中断调度）。
 */
public final class LogUploader {

    private static final Logger LOGGER = LoggerFactory.getLogger("player_to_player/log-upload");

    /** 上传周期（秒）。 */
    private static final long UPLOAD_INTERVAL_SECONDS = 30;

    /** 首次上传的最大回溯量（1 MB）。 */
    private static final long FIRST_UPLOAD_TAIL_BYTES = 1024 * 1024;

    /** 单帧最大载荷（512 KB；LogCollector 上限 4 MB，留足余量，超量分多轮追平）。 */
    private static final int MAX_CHUNK_BYTES = 512 * 1024;

    /** 定时任务句柄；null = 未运行。 */
    private static volatile ScheduledFuture<?> task;

    /** 已上传到的文件偏移（仅 io 池任务线程读写 —— scheduleWithFixedDelay 串行）。 */
    private static long offset;

    /** 下一次发送是否为会话首帧（append=false 截断服务端旧内容）。 */
    private static boolean firstUpload;

    private LogUploader() {
    }

    /**
     * 开始上传（GroupRuntime attach 时调用；幂等 —— 已在运行则忽略）。
     *
     * @param conn       与物理服务端的控制连接
     * @param playerName 主客户端玩家名（服务端落盘文件名 {@code <玩家名>-latest.log}）
     */
    public static synchronized void start(ControlConnection conn, String playerName) {
        if (task != null) {
            return;
        }
        String name = (playerName == null || playerName.isBlank()) ? "unknown" : playerName;
        offset = -1; // 首轮定位
        firstUpload = true;
        task = ThreadPools.scheduler().scheduleWithFixedDelay(
                () -> ThreadPools.io().execute(() -> uploadOnce(conn, name)),
                10, UPLOAD_INTERVAL_SECONDS, TimeUnit.SECONDS);
        LOGGER.info("日志上传已启动: {} → 服务端 logs/{}-latest.log", logPath(), name);
    }

    /** 停止上传（GroupRuntime detach 时调用；幂等）。 */
    public static synchronized void stop() {
        ScheduledFuture<?> current = task;
        task = null;
        if (current != null) {
            current.cancel(false);
            LOGGER.info("日志上传已停止");
        }
    }

    /** 本端游戏日志路径。 */
    private static Path logPath() {
        return FabricLoader.getInstance().getGameDir().resolve("logs").resolve("latest.log");
    }

    /** 单轮上传（io 池；scheduleWithFixedDelay 保证不与上一轮并发）。 */
    private static void uploadOnce(ControlConnection conn, String playerName) {
        if (task == null || conn == null || !conn.isOpen()) {
            return; // 已停止/连接不可用：本轮跳过（下轮自愈）
        }
        Path path = logPath();
        try {
            if (!Files.isRegularFile(path)) {
                return;
            }
            long length = Files.size(path);
            if (offset < 0 || length < offset) {
                // 首轮定位 / 文件轮转：只回溯尾部，重新截断上传
                offset = Math.max(0, length - FIRST_UPLOAD_TAIL_BYTES);
                firstUpload = true;
            }
            if (length == offset) {
                return; // 无新增
            }
            try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ)) {
                // 每轮最多两帧（1 MB），大量积压分多轮追平，不独占 io 线程
                for (int i = 0; i < 2 && offset < length; i++) {
                    int toRead = (int) Math.min(MAX_CHUNK_BYTES, length - offset);
                    ByteBuffer buffer = ByteBuffer.allocate(toRead);
                    int read = channel.read(buffer, offset);
                    if (read <= 0) {
                        break;
                    }
                    byte[] data = read == toRead
                            ? buffer.array() : Arrays.copyOf(buffer.array(), read);
                    JsonObject json = new JsonObject();
                    json.addProperty("playerName", playerName);
                    json.addProperty("append", !firstUpload);
                    conn.send(ControlMessage.of(MessageType.LOG_UPLOAD, json, data));
                    offset += read;
                    firstUpload = false;
                }
            }
        } catch (IOException e) {
            // 读失败（防病毒软件占用等）：跳过本轮，下轮重试
            LOGGER.debug("日志读取失败，本轮跳过: {}", e.toString());
        }
    }
}
