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
 *   <li>恢复上次中断的目录发布，再请求并验证清单和 filteredHash；</li>
 *   <li>主副视图显式绑定同一个 snapshotId；</li>
 *   <li>在空暂存目录中按清单复制已有内容或分块下载，逐文件校验 SHA-256；</li>
 *   <li>完整复验两套暂存文件集，事务替换目录；远端已删除的文件不会进入新目录；</li>
 *   <li>全部发布完成才完成 future；失败保留/恢复旧目录。</li>
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
    private final EnvironmentPathPolicy pathPolicy;

    public EnvSyncClient(ControlConnection conn, GlobalConfig config) {
        this(conn, config, "world");
    }

    public EnvSyncClient(ControlConnection conn, GlobalConfig config, String worldPath) {
        this.conn = conn;
        this.pathPolicy = EnvironmentPathPolicy.create(EnvironmentPathPolicy.worldMetadataExclusions(worldPath));
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
        return syncTo(targetEnvDir, target, null);
    }

    /** 指定版本用于预检/在线更新；该版本过期时必须失败，不能悄悄改用最新版。 */
    public CompletableFuture<String> syncTo(Path targetEnvDir, ModPrefixResolver.Target target,
                                             String expectedSnapshotId) {
        return CompletableFuture.supplyAsync(() -> {
            for (int attempt = 0; ; attempt++) {
                try {
                    Path destination = targetEnvDir.toAbsolutePath().normalize();
                    EnvironmentDirectoryTransaction.validateRoot(destination);
                    EnvironmentDirectoryTransaction.recover(destination);
                    ManifestSnapshot snapshot = requestManifest(target, expectedSnapshotId);
                    EnvironmentContentReuse reuse = new EnvironmentContentReuse();
                    validateDestination(destination, snapshot.manifest());
                    reuse.indexMatching(destination, snapshot.manifest());
                    try (EnvironmentDirectoryTransaction transaction = new EnvironmentDirectoryTransaction()) {
                        Path staging = transaction.stage(destination, snapshot.manifest());
                        prepare(staging, snapshot, reuse);
                        transaction.commit();
                    }
                    return snapshot.filteredHash();
                } catch (SnapshotNotFoundException expired) {
                    if (expectedSnapshotId != null || attempt >= SNAPSHOT_RESTARTS) {
                        throw new IllegalStateException("环境同步失败: 指定快照不存在或连续过期", expired);
                    }
                    LOGGER.info("环境快照过期，整轮重新准备（第 {} 次）", attempt + 1);
                } catch (IOException failure) {
                    throw new IllegalStateException("环境同步失败: " + failure.getMessage(), failure);
                }
            }
        }, ThreadPools.io());
    }

    /** 同一上游版本的两个角色视图；运行启动器可据此校验实际加载的模组及环境版本。 */
    public record PairResult(String primaryHash, String secondaryHash, String snapshotId,
                             EnvironmentManifest primaryManifest, EnvironmentManifest secondaryManifest) {
    }

    public CompletableFuture<PairResult> syncPair(Path primaryDir, Path secondaryDir) {
        return syncPair(primaryDir, secondaryDir, null);
    }

    /**
     * 主清单先确定上游版本，再明确用该 ID 获取副清单。两份目录均在暂存区完成，
     * 所有文件通过复验后才发布；任何角色快照过期都重试整对，绝不只更新一个角色。
     * 指定 expectedSnapshotId 的调用只接受该版，供登录前检查与在线更新确认使用。
     */
    public CompletableFuture<PairResult> syncPair(Path primaryDir, Path secondaryDir,
                                                  String expectedSnapshotId) {
        return syncPair(primaryDir, secondaryDir, expectedSnapshotId, () -> true);
    }

    /**
     * 取消检查进入实际目录提交锁。若旧下载晚于新下载完成，它在同一锁内看到失效票据，
     * 无权再覆盖新目录；若旧提交已经开始，新提交只能排在它之后，最终仍以新版本为准。
     */
    public CompletableFuture<PairResult> syncPair(Path primaryDir, Path secondaryDir,
                                                  String expectedSnapshotId,
                                                  java.util.function.BooleanSupplier stillCurrent) {
        java.util.Objects.requireNonNull(stillCurrent);
        return CompletableFuture.supplyAsync(() -> {
            for (int attempt = 0; ; attempt++) {
                try {
                    if (!stillCurrent.getAsBoolean()) throw new java.util.concurrent.CancellationException("环境同步会话已取消");
                    Path primary = primaryDir.toAbsolutePath().normalize();
                    Path secondary = secondaryDir.toAbsolutePath().normalize();
                    if (primary.startsWith(secondary) || secondary.startsWith(primary)
                            || (Files.exists(primary) && Files.exists(secondary)
                            && Files.isSameFile(primary, secondary))) {
                        throw new IOException("主副环境必须使用互不重叠的独立目录");
                    }
                    EnvironmentDirectoryTransaction.validateRoot(primary);
                    EnvironmentDirectoryTransaction.validateRoot(secondary);
                    EnvironmentDirectoryTransaction.recover(primary, secondary);
                    ManifestSnapshot primarySnapshot = requestManifest(
                            ModPrefixResolver.Target.PRIMARY_CLIENT, expectedSnapshotId);
                    ManifestSnapshot secondarySnapshot = requestManifest(
                            ModPrefixResolver.Target.SECONDARY_CLIENT, primarySnapshot.snapshotId());
                    validateDestination(primary, primarySnapshot.manifest());
                    validateDestination(secondary, secondarySnapshot.manifest());
                    EnvironmentContentReuse reuse = new EnvironmentContentReuse();
                    reuse.indexMatching(primary, primarySnapshot.manifest());
                    reuse.indexMatching(secondary, secondarySnapshot.manifest());
                    try (EnvironmentDirectoryTransaction transaction = new EnvironmentDirectoryTransaction()) {
                        Path primaryStage = transaction.stage(primary, primarySnapshot.manifest());
                        Path secondaryStage = transaction.stage(secondary, secondarySnapshot.manifest());
                        prepare(primaryStage, primarySnapshot, reuse);
                        prepare(secondaryStage, secondarySnapshot, reuse);
                        transaction.commit(stillCurrent);
                    }
                    return new PairResult(primarySnapshot.filteredHash(), secondarySnapshot.filteredHash(),
                            primarySnapshot.snapshotId(), primarySnapshot.manifest(), secondarySnapshot.manifest());
                } catch (SnapshotNotFoundException expired) {
                    if (expectedSnapshotId != null || attempt >= SNAPSHOT_RESTARTS) {
                        throw new IllegalStateException("双角色环境同步失败: 指定快照不存在或连续过期", expired);
                    }
                    LOGGER.info("双角色环境快照过期，整对重新准备（第 {} 次）", attempt + 1);
                } catch (IOException failure) {
                    throw new IllegalStateException("双角色环境同步失败: " + failure.getMessage(), failure);
                }
            }
        }, ThreadPools.io());
    }

    /** 先验证旧目录中的对应路径，不能通过暂存下载绕过原有的符号链接拒绝规则。 */
    private static void validateDestination(Path destination, EnvironmentManifest manifest) throws IOException {
        for (String path : manifest.files().keySet()) {
            EnvironmentDirectoryTransaction.validateManifestPath(destination, path);
        }
    }

    /** 空暂存目录只装入清单内容；按哈希复用旧目录及另一角色已完成的暂存文件。 */
    private void prepare(Path staging, ManifestSnapshot snapshot, EnvironmentContentReuse reuse) throws IOException {
        int reused = 0;
        int downloaded = 0;
        for (var file : snapshot.manifest().files().entrySet()) {
            if (reuse.copyTo(staging, file.getKey(), file.getValue())) {
                reused++;
            } else {
                downloadWithRetry(staging, snapshot.snapshotId(), file.getKey(), file.getValue());
                downloaded++;
            }
            reuse.remember(staging, file.getKey(), file.getValue());
        }
        EnvironmentDirectoryTransaction.verify(staging, snapshot.manifest());
        LOGGER.info("环境暂存校验完成: snapshot={} 文件={} 下载={} 复用={}",
                snapshot.snapshotId(), snapshot.manifest().files().size(), downloaded, reused);
    }

    /** 清单与其版本号绑定，不能在文件下载过程中改用其他清单的 snapshotId。 */
    private record ManifestSnapshot(String snapshotId, String filteredHash, EnvironmentManifest manifest) {
    }

    private ManifestSnapshot requestManifest(ModPrefixResolver.Target target, String expectedSnapshotId)
            throws IOException {
        JsonObject req = new JsonObject();
        if (target != null) req.addProperty("target", target.name());
        if (expectedSnapshotId != null) {
            if (expectedSnapshotId.isBlank()) throw new IOException("指定的 snapshotId 不能为空");
            req.addProperty("snapshotId", expectedSnapshotId);
        }
        ControlMessage reply = await(conn.request(ControlMessage.of(MessageType.ENV_MANIFEST_REQUEST, req)));
        requireType(reply, MessageType.ENV_MANIFEST);
        String snapshotId = JsonUtil.getString(reply.json(), "snapshotId", "");
        if (snapshotId.isBlank() || (expectedSnapshotId != null && !expectedSnapshotId.equals(snapshotId))) {
            throw new IOException("服务端清单 snapshotId 缺失或与指定版本不一致");
        }
        String filteredHash = JsonUtil.getString(reply.json(), "filteredHash", "");
        if (!reply.json().has("manifest") || !reply.json().get("manifest").isJsonObject()) {
            throw new IOException("服务端清单格式错误");
        }
        JsonObject manifestJson = reply.json().getAsJsonObject("manifest");
        if (!manifestJson.has("files") || !manifestJson.get("files").isJsonObject()) {
            throw new IOException("服务端清单缺少 files 对象");
        }
        EnvironmentManifest manifest = EnvironmentManifest.fromJson(manifestJson);
        for (String path : manifest.files().keySet()) {
            if (!pathPolicy.includes(path)) throw new IOException("环境清单含已排除文件，必须重新获取v9环境快照: " + path);
        }
        // 解析器供其他调用方兼容旧数据而允许忽略坏条目；网络同步必须拒绝丢条目以及
        // 不可信 filteredHash，避免空清单/截断清单被误当作成功并删除已有环境。
        if (manifest.files().size() != manifestJson.getAsJsonObject("files").size()
                || !manifest.globalHash().equals(filteredHash)) {
            throw new IOException("服务端清单 filteredHash 或文件条目校验失败");
        }
        for (var entry : manifest.files().values()) {
            if (!entry.sha256().matches("[0-9a-f]{64}") || entry.size() < 0) {
                throw new IOException("服务端清单含非法文件摘要或长度");
            }
        }
        return new ManifestSnapshot(snapshotId, filteredHash, manifest);
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
     * 临时文件仅存在于本次暂存目录；finally 删除半截文件，发布前的完整复验也会
     * 拒绝意外残留，不能依赖源扫描器对 .tmp 的忽略策略来隐藏不完整下载。
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
            if ("snapshot_not_found".equals(JsonUtil.getString(reply.json(), "code", ""))) {
                throw new SnapshotNotFoundException(JsonUtil.getString(reply.json(), "message", "快照已过期"));
            }
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
