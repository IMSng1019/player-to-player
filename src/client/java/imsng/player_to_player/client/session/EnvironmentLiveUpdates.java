package imsng.player_to_player.client.session;

import com.google.gson.JsonObject;
import imsng.player_to_player.core.NodeContext;
import imsng.player_to_player.env.*;
import imsng.player_to_player.netproto.*;
import imsng.player_to_player.runtime.WorkerProcess;
import imsng.player_to_player.util.JsonUtil;
import imsng.player_to_player.util.Sha256;
import imsng.player_to_player.util.ThreadPools;
import net.minecraft.client.Minecraft;
import net.minecraft.network.chat.Component;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 每个正式游戏会话的环境后台服务。新版本只下载到 pending，不覆写运行中的环境；
 * 重新连接会再次走预检和独立运行目录安装。主客户端配置上传采用文件基础哈希，
 * 冲突文件保留在本地且显示提示，绝不把未知存档数据纳入通用文件同步。
 */
public final class EnvironmentLiveUpdates {
    private static final Logger LOGGER = LoggerFactory.getLogger("player_to_player/env-live");
    private static volatile Session active;
    private EnvironmentLiveUpdates() { }

    public static synchronized void start(ControlClient handlers, ControlConnection connection,
                                           Path folder, EnvironmentPreflightInfo info,
                                           EnvSyncClient.PairResult installed) {
        stop();
        Session session = new Session(connection, folder, info, installed);
        active = session;
        handlers.on(MessageType.ENV_CHANGED, (conn, message) -> {
            if (active != session) return;
            try { session.changed(EnvironmentPreflightInfo.fromJson(message.json())); }
            catch (RuntimeException invalid) { LOGGER.warn("忽略无效的环境更新通知", invalid); }
        });
        // 捕获本次会话，不能在旧 worker 停止时通过全局 active 把旧世界文件回传给新服务器。
        WorkerProcess.setStoppedFlush(path -> flushSession(session, path));
        session.timer = ThreadPools.scheduler().scheduleWithFixedDelay(session::tick, 10, 10, TimeUnit.SECONDS);
    }

    public static synchronized void stop() {
        Session session = active;
        active = null;
        if (session != null && session.timer != null) session.timer.cancel(false);
    }

    /** 子进程完成停服存盘后调用；future 完成前父会话仍保持连接，覆盖停服时修改的配置。 */
    public static CompletableFuture<Void> flushRuntime(Path runtime) {
        Session session = active;
        if (session == null || runtime == null) return CompletableFuture.completedFuture(null);
        return flushSession(session, runtime);
    }

    private static CompletableFuture<Void> flushSession(Session session, Path runtime) {
        CompletableFuture<Void> done = new CompletableFuture<>();
        ThreadPools.startBlocking("environment-final-upload", () -> {
            try { session.upload(runtime); done.complete(null); }
            catch (Exception failure) { done.completeExceptionally(failure); }
        });
        return done;
    }

    private static final class Session {
        final ControlConnection connection;
        final Path folder;
        final EnvironmentPreflightInfo initial;
        final Map<String, EnvironmentManifest.Entry> baseline;
        final Map<String, String> blocked = new HashMap<>();
        final AtomicBoolean polling = new AtomicBoolean();
        final AtomicBoolean downloading = new AtomicBoolean();
        final AtomicReference<EnvironmentPreflightInfo> desired = new AtomicReference<>();
        volatile String preparedHash = "";
        volatile ScheduledFuture<?> timer;
        List<String> allowed;
        int maxBytes;

        Session(ControlConnection connection, Path folder, EnvironmentPreflightInfo info,
                EnvSyncClient.PairResult installed) {
            this.connection = connection;
            this.folder = folder;
            this.initial = info;
            this.baseline = new TreeMap<>(installed.primaryManifest().files());
        }

        void tick() {
            if (active != this || !connection.isOpen() || !polling.compareAndSet(false, true)) return;
            ThreadPools.startBlocking("environment-update", () -> {
                try {
                    // 周期查询覆盖客户端刚注册处理器前丢失的通知，不依赖某一次推送必达。
                    ControlMessage reply = connection.request(ControlMessage.of(MessageType.ENV_CHANGED))
                            .get(20, TimeUnit.SECONDS);
                    if (reply.type() == MessageType.ENV_CHANGED) changed(EnvironmentPreflightInfo.fromJson(reply.json()));
                    if (active == this && WorkerProcess.isManagedWorld()) upload(WorkerProcess.currentDirectory());
                } catch (Exception failure) {
                    if (active == this) LOGGER.warn("后台环境检查暂未完成，将在下次检查重试: {}", failure.toString());
                } finally { polling.set(false); }
            });
        }

        void changed(EnvironmentPreflightInfo info) {
            if (active != this || !info.ready() || !info.worldName().equals(initial.worldName())
                    || !info.worldPath().equals(initial.worldPath())
                    || info.runtimeHash().equals(initial.runtimeHash()) || info.runtimeHash().equals(preparedHash)) return;
            desired.set(info);
            if (!downloading.compareAndSet(false, true)) return;
            ThreadPools.startBlocking("environment-download-update", () -> {
                try {
                    EnvironmentPreflightInfo next;
                    while (active == this && (next = desired.getAndSet(null)) != null) {
                        Path pending = folder.resolve("pending-environment");
                        new EnvSyncClient(connection, NodeContext.get().config(), next.worldPath()).syncPair(
                                pending.resolve("primary"), pending.resolve("secondary"), next.snapshotId(),
                                () -> active == this && connection.isOpen()).join();
                        if (active != this) return;
                        JsonUtil.writeFileAtomic(pending.resolve("version.json"), next);
                        preparedHash = next.runtimeHash();
                        notifyPlayer("服务器环境更新已下载，重新连接后应用。当前世界继续使用已启动的版本。");
                    }
                } catch (Exception failure) {
                    if (active == this) LOGGER.warn("新环境暂存失败，保留正在使用的版本: {}", failure.toString());
                } finally {
                    downloading.set(false);
                    EnvironmentPreflightInfo next = desired.get();
                    if (next != null && active == this) changed(next);
                }
            });
        }

        /** 串行普通轮询与停服收尾，避免同一本地变更被两个任务用不同基础版本提交。 */
        synchronized void upload(Path runtime) throws Exception {
            if (!ownsRuntime(runtime) || !connection.isOpen()) return;
            if (allowed == null) {
                JsonObject request = new JsonObject();
                request.addProperty("action", "policy");
                ControlMessage reply = connection.request(ControlMessage.of(MessageType.ENV_UPLOAD, request))
                        .get(20, TimeUnit.SECONDS);
                if (!"policy".equals(JsonUtil.getString(reply.json(), "status", ""))) return;
                List<String> paths = new ArrayList<>();
                if (reply.json().has("allowedPaths")) reply.json().getAsJsonArray("allowedPaths")
                        .forEach(path -> paths.add(path.getAsString()));
                allowed = List.copyOf(paths);
                maxBytes = Math.min(8 * 1024 * 1024, JsonUtil.getInt(reply.json(), "maxBytes", 0));
            }
            Path root = runtime.toAbsolutePath().normalize();
            Path save = WorkerProcess.currentSaveDirectory();
            TreeSet<String> candidates = new TreeSet<>();
            for (String prefix : allowed) {
                Path start = RuntimeEnvironmentPaths.resolve(root, save, initial.worldPath(), prefix);
                if (!Files.exists(start)) continue;
                try (var walk = Files.walk(start)) {
                    for (Path file : walk.filter(path -> Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS)).toList()) {
                        candidates.add(RuntimeEnvironmentPaths.manifestPath(root, save, initial.worldPath(), file));
                    }
                }
            }
            baseline.keySet().stream().filter(path -> EnvironmentUploadStore.allowed(path, allowed)).forEach(candidates::add);
            for (String path : candidates) {
                if (!ownsRuntime(runtime) || !connection.isOpen()) return;
                if (!EnvironmentUploadStore.allowed(path, allowed)
                        || !EnvironmentRuntimeIdentity.isRuntimePath(path, initial.worldPath())) continue;
                Path file = RuntimeEnvironmentPaths.resolve(root, save, initial.worldPath(), path);
                // 只有明确的“不存在”才是删除；权限/磁盘错误不能被 exists=false 吞掉后反传删除。
                java.nio.file.attribute.BasicFileAttributes attributes = null;
                try { attributes = Files.readAttributes(file, java.nio.file.attribute.BasicFileAttributes.class,
                        LinkOption.NOFOLLOW_LINKS); }
                catch (java.nio.file.NoSuchFileException missing) { /* 服务端版本文件在本地被删除。 */ }
                boolean exists = attributes != null;
                if (exists && (!attributes.isRegularFile() || attributes.size() > maxBytes)) continue;
                byte[] bytes = exists ? Files.readAllBytes(file) : null;
                if (bytes != null && bytes.length > maxBytes) continue;
                String nextHash = bytes == null ? "" : Sha256.hex(bytes);
                EnvironmentManifest.Entry base = baseline.get(path);
                String baseHash = base == null ? "" : base.sha256();
                if (nextHash.equals(baseHash) || nextHash.equals(blocked.get(path))) continue;
                JsonObject request = new JsonObject();
                request.addProperty("path", path);
                request.addProperty("baseSha256", baseHash);
                request.addProperty("delete", bytes == null);
                if (!ownsRuntime(runtime)) return;
                ControlMessage reply = connection.request(ControlMessage.of(MessageType.ENV_UPLOAD, request, bytes))
                        .get(20, TimeUnit.SECONDS);
                String status = JsonUtil.getString(reply.json(), "status", "failed");
                if (status.equals("applied") || status.equals("unchanged")) {
                    if (bytes == null) baseline.remove(path);
                    else baseline.put(path, new EnvironmentManifest.Entry(nextHash, bytes.length));
                    blocked.remove(path);
                } else if (status.equals("conflict") || status.equals("forbidden")) {
                    blocked.put(path, nextHash);
                    LOGGER.warn("环境配置回传被拒绝，保留本地修改: {} ({})", path, status);
                    notifyPlayer("环境配置未上传：" + path + "（" + (status.equals("conflict") ? "服务端版本已变化" : "不允许回传") + "）。");
                }
            }
        }

        private boolean ownsRuntime(Path runtime) {
            Path actual = WorkerProcess.currentDirectory();
            Path actualFolder = WorkerProcess.currentWorldFolder();
            return active == this && runtime != null && actual != null && actualFolder != null
                    && connection == WorkerProcess.connection()
                    && folder.toAbsolutePath().normalize().equals(actualFolder.toAbsolutePath().normalize())
                    && runtime.toAbsolutePath().normalize().equals(actual.toAbsolutePath().normalize());
        }
    }

    private static void notifyPlayer(String message) {
        Minecraft minecraft = Minecraft.getInstance();
        minecraft.execute(() -> {
            if (minecraft.player != null) minecraft.player.displayClientMessage(Component.literal(message), false);
        });
    }
}
