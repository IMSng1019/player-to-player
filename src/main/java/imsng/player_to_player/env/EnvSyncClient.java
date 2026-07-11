package imsng.player_to_player.env;

import com.google.gson.JsonObject;
import imsng.player_to_player.config.GlobalConfig;
import imsng.player_to_player.netproto.ControlConnection;
import imsng.player_to_player.netproto.ControlMessage;
import imsng.player_to_player.netproto.MessageType;
import imsng.player_to_player.util.JsonUtil;
import imsng.player_to_player.util.Sha256;
import imsng.player_to_player.util.ThreadPools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * 环境同步 —— 客户端侧驱动器。
 * <p>
 * 规范出处：玩家加入世界时"校验本地环境是否与服务端的环境的哈希值相同……
 * 如果不同则服务端向客户端发送新的环境文件更新环境"；
 * "主客户端和副客户端随时可以切换 所以下载环境文件时需要同时下载" ——
 * 因此调用方应以 {@link ModPrefixResolver.Target#PRIMARY_CLIENT} 与
 * {@link ModPrefixResolver.Target#SECONDARY_CLIENT} 各调用一次 {@link #syncTo}，
 * 分别写入 primary/environment 与 secondary/environment 两套目录。
 *
 * <h2>同步流程</h2>
 * <ol>
 *   <li>ENV_MANIFEST_REQUEST(target) → 服务端过滤后的清单 + filteredHash；</li>
 *   <li>本地扫描 targetEnvDir（空排除表 —— 本地环境目录整个都是环境文件）；</li>
 *   <li>{@link EnvironmentManifest#diffAgainst} 求需下载列表；</li>
 *   <li>逐文件循环 ENV_FILE_REQUEST 直到 last=true：写 .tmp 临时文件 →
 *       校验 SHA-256 → 原子 move 就位；哈希不匹配重试一次，再失败则整体失败；</li>
 *   <li>全部完成后以服务端 filteredHash 完成 future。</li>
 * </ol>
 *
 * <h2>线程模型</h2>
 * {@link #syncTo} 立即返回，全流程在 {@link ThreadPools#io()} 上驱动
 * （io 池为无界缓存池，在其上阻塞等待请求应答是允许的）；
 * 严禁在 MC 主线程 / Netty 事件循环上 join 返回的 future。
 */
public final class EnvSyncClient {

    private static final Logger LOGGER = LoggerFactory.getLogger("player_to_player/env");

    /** 单文件哈希校验失败后的重试次数（重试仍失败则整次同步失败）。 */
    private static final int HASH_RETRY = 1;

    /** 等待单次请求应答的兜底超时：略大于协议层请求超时，理论不会先于它触发。 */
    private static final long AWAIT_MILLIS = 20_000;

    private final ControlConnection conn;
    private final GlobalConfig config;

    public EnvSyncClient(ControlConnection conn, GlobalConfig config) {
        this.conn = conn;
        this.config = config;
    }

    /**
     * 把本地环境目录同步到与服务端一致。
     *
     * @param targetEnvDir 本地环境目录（primary/environment 或 secondary/environment）
     * @param target       分发目标端（决定 mods/ 的前缀过滤视角）
     * @return 完成时携带服务端过滤视图全局哈希（filteredHash）的 future；
     *         任何文件下载/校验失败则异常完成
     */
    public CompletableFuture<String> syncTo(Path targetEnvDir, ModPrefixResolver.Target target) {
        // supplyAsync 到 io() 池：调用线程（可能是 MC 主线程）立即拿到 future 返回
        return CompletableFuture.supplyAsync(() -> doSync(targetEnvDir, target), ThreadPools.io());
    }

    /** 同步主流程（运行在 io 线程，可阻塞）。 */
    private String doSync(Path targetEnvDir, ModPrefixResolver.Target target) {
        try {
            // ---- 1. 拉取服务端清单（按 target 过滤后的视图）----
            JsonObject req = new JsonObject();
            req.addProperty("target", target.name());
            ControlMessage manifestReply =
                    await(conn.request(ControlMessage.of(MessageType.ENV_MANIFEST_REQUEST, req)));
            requireType(manifestReply, MessageType.ENV_MANIFEST);
            String filteredHash = JsonUtil.getString(manifestReply.json(), "filteredHash", "");
            JsonObject manifestJson = manifestReply.json().has("manifest")
                    && manifestReply.json().get("manifest").isJsonObject()
                    ? manifestReply.json().getAsJsonObject("manifest") : new JsonObject();
            EnvironmentManifest remote = EnvironmentManifest.fromJson(manifestJson);

            // ---- 2. 本地扫描 + diff（空排除表：环境目录内全部都是环境文件）----
            EnvironmentManifest local = EnvironmentScanner.scan(targetEnvDir, List.of());
            List<String> toDownload = remote.diffAgainst(local);
            long totalBytes = toDownload.stream()
                    .mapToLong(p -> remote.files().get(p).size()).sum();
            LOGGER.info("[{}] 环境同步开始: 远端 {} 个文件, 需下载 {} 个 ({} 字节) → {}",
                    target, remote.files().size(), toDownload.size(), totalBytes, targetEnvDir);

            // ---- 3. 逐文件下载 ----
            Path normalizedDir = targetEnvDir.toAbsolutePath().normalize();
            int done = 0;
            long doneBytes = 0;
            for (String path : toDownload) {
                EnvironmentManifest.Entry entry = remote.files().get(path);
                downloadWithRetry(normalizedDir, path, entry);
                done++;
                doneBytes += entry.size();
                LOGGER.info("[{}] 环境同步进度: {}/{} 文件, {}/{} 字节 — {}",
                        target, done, toDownload.size(), doneBytes, totalBytes, path);
            }

            LOGGER.info("[{}] 环境同步完成: {} 个文件, {} 字节, filteredHash={}",
                    target, done, doneBytes, filteredHash);
            return filteredHash;
        } catch (IOException e) {
            // 统一转非受检异常让 future 异常完成（CompletableFuture 不吃受检异常）
            throw new IllegalStateException("环境同步失败: " + e.getMessage(), e);
        }
    }

    /** 下载单文件（哈希不匹配重试 {@value #HASH_RETRY} 次，仍失败抛异常终止整次同步）。 */
    private void downloadWithRetry(Path envDir, String path, EnvironmentManifest.Entry entry)
            throws IOException {
        IOException lastFailure = null;
        for (int attempt = 0; attempt <= HASH_RETRY; attempt++) {
            try {
                downloadOne(envDir, path, entry);
                return;
            } catch (IOException e) {
                lastFailure = e;
                LOGGER.warn("环境文件下载失败 (第 {} 次): {} — {}",
                        attempt + 1, path, e.getMessage());
            }
        }
        throw new IOException("环境文件重试后仍下载失败: " + path, lastFailure);
    }

    /**
     * 下载单文件：循环 ENV_FILE_REQUEST 收块写入 .tmp → 校验 SHA-256 → 原子 move 就位。
     * 临时文件带 .tmp 后缀 —— EnvironmentScanner 内置排除 *.tmp，中途残留的
     * 半截文件不会污染下次本地扫描的清单。
     */
    private void downloadOne(Path envDir, String path, EnvironmentManifest.Entry entry)
            throws IOException {
        // 清单来自网络（服务端数据也按不可信处理）：防目录穿越，落点必须在环境目录内
        Path dest = envDir.resolve(path).normalize();
        if (!dest.startsWith(envDir)) {
            throw new IOException("清单含非法路径（目录穿越）: " + path);
        }
        Files.createDirectories(dest.getParent());
        Path tmp = dest.resolveSibling(dest.getFileName() + ".p2pdl.tmp");

        try {
            try (OutputStream out = Files.newOutputStream(tmp,
                    StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING,
                    StandardOpenOption.WRITE)) {
                long offset = 0;
                boolean last = false;
                while (!last) {
                    JsonObject req = new JsonObject();
                    req.addProperty("path", path);
                    req.addProperty("offset", offset);
                    ControlMessage reply = await(
                            conn.request(ControlMessage.of(MessageType.ENV_FILE_REQUEST, req)));
                    requireType(reply, MessageType.ENV_FILE_DATA);

                    // 防御性核对应答归属：无状态分块协议下服务端按请求回显 path/offset，
                    // 不一致说明服务端实现异常或数据被篡改
                    String replyPath = JsonUtil.getString(reply.json(), "path", "");
                    long replyOffset = JsonUtil.getLong(reply.json(), "offset", -1L);
                    if (!path.equals(replyPath) || replyOffset != offset) {
                        throw new IOException("ENV_FILE_DATA 应答与请求不匹配: "
                                + replyPath + "@" + replyOffset + " 期望 " + path + "@" + offset);
                    }
                    byte[] chunk = reply.binary();
                    last = JsonUtil.getBoolean(reply.json(), "last", false);
                    if (chunk.length == 0 && !last) {
                        // 零字节且非末块 → 死循环风险，直接判协议错误
                        throw new IOException("服务端返回空数据块且未标记 last: " + path);
                    }
                    out.write(chunk);
                    offset += chunk.length;
                    if (offset > entry.size()) {
                        // 实际字节数超过清单声称大小：数据异常，及早失败省流量
                        throw new IOException("下载字节数超过清单大小: " + path);
                    }
                }
            }

            // ---- SHA-256 校验后原子就位 ----
            String actual = Sha256.hexOfFile(tmp);
            if (!actual.equals(entry.sha256())) {
                throw new IOException("SHA-256 校验失败: " + path
                        + " 期望 " + entry.sha256() + " 实际 " + actual);
            }
            try {
                Files.move(tmp, dest, StandardCopyOption.REPLACE_EXISTING,
                        StandardCopyOption.ATOMIC_MOVE);
            } catch (IOException e) {
                // 部分文件系统不支持原子移动，降级普通替换（与 JsonUtil.writeFileAtomic 同策略）
                Files.move(tmp, dest, StandardCopyOption.REPLACE_EXISTING);
            }
        } finally {
            Files.deleteIfExists(tmp); // 成功时已被 move 走，此处只清理失败残留
        }
    }

    // ------------------------------------------------------------ 工具

    /** 在 io 线程上阻塞等待请求应答，把各种失败统一包装为 IOException。 */
    private static ControlMessage await(CompletableFuture<ControlMessage> future)
            throws IOException {
        try {
            return future.get(AWAIT_MILLIS, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("同步被中断", e);
        } catch (TimeoutException e) {
            throw new IOException("请求应答超时", e);
        } catch (ExecutionException e) {
            throw new IOException("请求失败: " + e.getCause(), e.getCause());
        }
    }

    /** 校验应答类型；服务端以 ERROR 应答（携带 code/message）时抛出可读异常。 */
    private static void requireType(ControlMessage reply, MessageType expected)
            throws IOException {
        if (reply.type() == expected) {
            return;
        }
        if (reply.type() == MessageType.ERROR) {
            throw new IOException("服务端返回错误: "
                    + JsonUtil.getString(reply.json(), "code", "unknown") + " — "
                    + JsonUtil.getString(reply.json(), "message", ""));
        }
        throw new IOException("意外的应答类型: " + reply.type() + " 期望 " + expected);
    }
}
