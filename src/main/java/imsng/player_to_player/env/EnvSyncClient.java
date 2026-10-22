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
 * 因此客户端通过 {@link #syncPair} 分别取得主副角色清单，写入
 * primary/environment 与 secondary/environment 两套独立目录；相同内容只下载一次。
 * 中转端仍可使用 {@link #syncTo} 拉取不按角色过滤的完整环境。
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
 * （io 池现在是有界线程池 + 无界队列：在其上阻塞等待请求应答仍允许，但每个阻塞的
 * 同步流程都占住一条池内线程，故 <b>不要</b>再新增"池内任务等池内任务"的写法）；
 * 严禁在 MC 主线程 / Netty 事件循环上 join 返回的 future。
 */
public final class EnvSyncClient {

    private static final Logger LOGGER = LoggerFactory.getLogger("player_to_player/env");

    /** 单文件哈希校验失败后的重试次数（重试仍失败则整次同步失败）。 */
    private static final int HASH_RETRY = 1;

    /** 下载期间快照被回收时，重新拉取清单的最大次数。 */
    private static final int SNAPSHOT_RESTARTS = 2;

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
     * @param target       分发目标端（决定 mods/ 的前缀过滤视角）；
     *                     <b>null = 不过滤的全量视图</b>（Phase 2 中转端从上级
     *                     服务端整仓拉取用 —— 中转端要能向任意目标端二次分发，
     *                     必须持有全量环境，"宁可多发不可漏发"）
     * @return 完成时携带服务端过滤视图全局哈希（filteredHash）的 future；
     *         任何文件下载/校验失败则异常完成
     */
    public CompletableFuture<String> syncTo(Path targetEnvDir, ModPrefixResolver.Target target) {
        // supplyAsync 到 io() 池：调用线程（可能是 MC 主线程）立即拿到 future 返回
        return CompletableFuture.supplyAsync(() -> doSync(targetEnvDir, target, null, null), ThreadPools.io());
    }

    /** 双角色各自清单的最终哈希；不能把两个角色的过滤视图合并为一个哈希。 */
    public record PairResult(String primaryHash, String secondaryHash) {
    }

    /**
     * 在同一世界的一次同步批次内复用主副环境的已验证内容。
     * <p>
     * 两个角色仍各自请求清单并绑定各自的 snapshotId，只把清单确认过的普通文件
     * 作为复用源。复用以 SHA-256 和大小共同匹配，通过独立文件复制、复验再替换，
     * 因而运行中的配置修改不会通过硬链接传播到另一角色。索引仅存在于本次调用，
     * 不保留其他世界或前一次连接的路径。
     * <p>
     * 在一个 IO 任务内顺序驱动，避免在有界 IO 池中提交两个子同步任务再互相等待。
     */
    public CompletableFuture<PairResult> syncPair(Path primaryDir, Path secondaryDir) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                Path primary = primaryDir.toAbsolutePath().normalize();
                Path secondary = secondaryDir.toAbsolutePath().normalize();
                if (primary.startsWith(secondary) || secondary.startsWith(primary)
                        || (Files.exists(primary) && Files.exists(secondary)
                        && Files.isSameFile(primary, secondary))) {
                    throw new IOException("主副环境必须使用互不重叠的独立目录");
                }
                ManifestSnapshot primaryManifest = requestManifest(ModPrefixResolver.Target.PRIMARY_CLIENT);
                ManifestSnapshot secondaryManifest = requestManifest(ModPrefixResolver.Target.SECONDARY_CLIENT);
                EnvironmentContentReuse reuse = new EnvironmentContentReuse();
                // 先检查两个角色已存在的清单文件，使副端已有文件也可以反向供主端复用。
                reuse.indexMatching(primary, primaryManifest.manifest());
                reuse.indexMatching(secondary, secondaryManifest.manifest());
                String primaryHash = doSync(primary, ModPrefixResolver.Target.PRIMARY_CLIENT,
                        reuse, primaryManifest);
                String secondaryHash = doSync(secondary, ModPrefixResolver.Target.SECONDARY_CLIENT,
                        reuse, secondaryManifest);
                return new PairResult(primaryHash, secondaryHash);
            } catch (IOException e) {
                throw new IllegalStateException("双角色环境同步失败: " + e.getMessage(), e);
            }
        }, ThreadPools.io());
    }

    /** 同步主流程（运行在 io 线程，可阻塞）。 */
    private String doSync(Path targetEnvDir, ModPrefixResolver.Target target,
                          EnvironmentContentReuse reuse, ManifestSnapshot initialManifest) {
        // 日志标签：null 目标显示为 FULL（全量视图）
        String label = target != null ? target.name() : "FULL";
        SnapshotNotFoundException lastExpired = null;
        for (int attempt = 0; attempt <= SNAPSHOT_RESTARTS; attempt++) {
            try {
                return doSyncAttempt(targetEnvDir, target, label, reuse,
                        attempt == 0 ? initialManifest : null);
            } catch (SnapshotNotFoundException e) {
                lastExpired = e;
                LOGGER.info("[{}] 环境快照已过期，重新拉取清单（第 {}/{} 次）",
                        label, attempt + 1, SNAPSHOT_RESTARTS + 1);
            } catch (IOException e) {
                throw new IllegalStateException("环境同步失败: " + e.getMessage(), e);
            }
        }
        throw new IllegalStateException("环境同步失败: 快照连续过期", lastExpired);
    }

    /** 使用单一 snapshotId 完成一轮清单 diff 与文件下载。 */
    private String doSyncAttempt(Path targetEnvDir, ModPrefixResolver.Target target,
                                 String label, EnvironmentContentReuse reuse,
                                 ManifestSnapshot initialManifest) throws IOException {
        try {
            // ---- 1. 拉取服务端清单（按 target 过滤后的视图；无 target 字段 = 全量）----
            ManifestSnapshot snapshot = initialManifest != null ? initialManifest : requestManifest(target);
            String snapshotId = snapshot.snapshotId();
            String filteredHash = snapshot.filteredHash();
            EnvironmentManifest remote = snapshot.manifest();

            // ---- 2. 本地扫描 + diff（空排除表：环境目录内全部都是环境文件）----
            // 已匹配项也必须校验：否则 scanner 跟随文件链接后会把共享配置误认为独立副本。
            for (String path : remote.files().keySet()) {
                EnvironmentContentReuse.resolveDestination(targetEnvDir, path);
            }
            EnvironmentManifest local = EnvironmentScanner.scan(targetEnvDir, List.of());
            // 大小也必须匹配；内容复用不能仅依据哈希忽略清单声明的长度。
            List<String> toDownload = remote.files().entrySet().stream()
                    .filter(entry -> !entry.getValue().equals(local.files().get(entry.getKey())))
                    .map(java.util.Map.Entry::getKey).toList();
            long totalBytes = toDownload.stream()
                    .mapToLong(p -> remote.files().get(p).size()).sum();
            LOGGER.info("[{}] 环境同步开始: 远端 {} 个文件, 需下载 {} 个 ({} 字节) → {}",
                    label, remote.files().size(), toDownload.size(), totalBytes, targetEnvDir);

            // ---- 3. 逐文件下载 ----
            Path normalizedDir = targetEnvDir.toAbsolutePath().normalize();
            int done = 0;
            long doneBytes = 0;
            int reusedFiles = 0;
            long reusedBytes = 0;
            for (String path : toDownload) {
                EnvironmentManifest.Entry entry = remote.files().get(path);
                if (reuse != null && reuse.copyTo(normalizedDir, path, entry)) {
                    reusedFiles++;
                    reusedBytes += entry.size();
                } else {
                    downloadWithRetry(normalizedDir, snapshotId, path, entry);
                }
                if (reuse != null) {
                    reuse.remember(normalizedDir, path, entry);
                }
                done++;
                doneBytes += entry.size();
                LOGGER.info("[{}] 环境同步进度: {}/{} 文件, {}/{} 字节 — {}",
                        label, done, toDownload.size(), doneBytes, totalBytes, path);
            }

            LOGGER.info("[{}] 环境同步完成: {} 个文件, {} 字节, 本地复用 {} 个/{} 字节, filteredHash={}",
                    label, done, doneBytes, reusedFiles, reusedBytes, filteredHash);
            return filteredHash;
        } catch (SnapshotNotFoundException e) {
            throw e;
        }
    }

    /** 清单与其版本号绑定，不能在文件下载过程中改用其他清单的 snapshotId。 */
    private record ManifestSnapshot(String snapshotId, String filteredHash, EnvironmentManifest manifest) {
    }

    private ManifestSnapshot requestManifest(ModPrefixResolver.Target target) throws IOException {
        JsonObject req = new JsonObject();
        if (target != null) {
            req.addProperty("target", target.name());
        }
        ControlMessage reply = await(conn.request(ControlMessage.of(MessageType.ENV_MANIFEST_REQUEST, req)));
        requireType(reply, MessageType.ENV_MANIFEST);
        String snapshotId = JsonUtil.getString(reply.json(), "snapshotId", "");
        if (snapshotId.isBlank()) {
            throw new IOException("服务端清单缺少 snapshotId");
        }
        String filteredHash = JsonUtil.getString(reply.json(), "filteredHash", "");
        JsonObject manifestJson = reply.json().has("manifest") && reply.json().get("manifest").isJsonObject()
                ? reply.json().getAsJsonObject("manifest") : new JsonObject();
        return new ManifestSnapshot(snapshotId, filteredHash, EnvironmentManifest.fromJson(manifestJson));
    }

    /** 下载单文件（哈希不匹配重试 {@value #HASH_RETRY} 次，仍失败抛异常终止整次同步）。 */
    private void downloadWithRetry(Path envDir, String snapshotId, String path,
                                   EnvironmentManifest.Entry entry) throws IOException {
        IOException lastFailure = null;
        for (int attempt = 0; attempt <= HASH_RETRY; attempt++) {
            try {
                downloadOne(envDir, snapshotId, path, entry);
                return;
            } catch (SnapshotNotFoundException e) {
                throw e; // 快照级失败必须重拉清单，不能在旧 ID 上重复文件请求
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
    private void downloadOne(Path envDir, String snapshotId, String path,
                             EnvironmentManifest.Entry entry) throws IOException {
        // 清单来自网络（服务端数据也按不可信处理）：防目录穿越，落点必须在环境目录内
        Path dest = EnvironmentContentReuse.resolveDestination(envDir, path);
        Files.createDirectories(dest.getParent());
        // 唯一临时文件避免跟随预存 .p2pdl.tmp 链接；失败残留仍由扫描器忽略。
        Path tmp = Files.createTempFile(dest.getParent(), ".p2pdl-", ".tmp");

        try {
            try (OutputStream out = Files.newOutputStream(tmp,
                    StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING,
                    StandardOpenOption.WRITE)) {
                long offset = 0;
                boolean last = false;
                while (!last) {
                    JsonObject req = new JsonObject();
                    req.addProperty("snapshotId", snapshotId);
                    req.addProperty("path", path);
                    req.addProperty("offset", offset);
                    ControlMessage reply = await(
                            conn.request(ControlMessage.of(MessageType.ENV_FILE_REQUEST, req)));
                    if (reply.type() == MessageType.ERROR
                            && "snapshot_not_found".equals(
                            JsonUtil.getString(reply.json(), "code", ""))) {
                        throw new SnapshotNotFoundException(
                                JsonUtil.getString(reply.json(), "message", "快照已过期"));
                    }
                    requireType(reply, MessageType.ENV_FILE_DATA);

                    // 防御性核对应答归属：无状态分块协议下服务端按请求回显 path/offset，
                    // 不一致说明服务端实现异常或数据被篡改
                    String replySnapshotId = JsonUtil.getString(
                            reply.json(), "snapshotId", "");
                    String replyPath = JsonUtil.getString(reply.json(), "path", "");
                    long replyOffset = JsonUtil.getLong(reply.json(), "offset", -1L);
                    long replyTotal = JsonUtil.getLong(reply.json(), "total", -1L);
                    if (!snapshotId.equals(replySnapshotId)
                            || !path.equals(replyPath) || replyOffset != offset
                            || replyTotal != entry.size()) {
                        throw new IOException("ENV_FILE_DATA 应答与请求不匹配: "
                                + replySnapshotId + ":" + replyPath + "@" + replyOffset
                                + " 期望 " + snapshotId + ":" + path + "@" + offset);
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
                if (offset != entry.size()) {
                    throw new IOException("下载字节数与清单大小不一致: " + path
                            + " 实际=" + offset + " 期望=" + entry.size());
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

    /** 表示服务端已回收本轮绑定的旧快照，调用方必须从清单阶段重新开始。 */
    private static final class SnapshotNotFoundException extends IOException {
        private SnapshotNotFoundException(String message) {
            super(message);
        }
    }
}
