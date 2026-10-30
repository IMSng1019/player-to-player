package imsng.player_to_player.client.env;

import imsng.player_to_player.core.NodeContext;
import imsng.player_to_player.env.ClientLocalSettings;
import imsng.player_to_player.env.restart.RestartCoordinator;
import imsng.player_to_player.env.restart.RestartHelperProcess;
import imsng.player_to_player.env.restart.RestartTask;
import imsng.player_to_player.env.restart.RestartTaskStore;
import imsng.player_to_player.util.ThreadPools;
import net.fabricmc.fabric.api.client.event.lifecycle.v1.ClientLifecycleEvents;
import net.fabricmc.fabric.api.client.event.lifecycle.v1.ClientTickEvents;
import net.fabricmc.loader.api.FabricLoader;
import net.minecraft.client.Minecraft;
import net.minecraft.client.gui.screens.TitleScreen;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.UUID;
import java.util.function.BooleanSupplier;

/**
 * 游戏侧统一重启入口。UI/连接代次的最终核验只在主线程执行，文件及进程握手全部在后台；
 * 唯一的退出动作仍是 Minecraft.stop()，由已有 MinecraftExitMixin 保存退出屏障。
 */
final class ClientEnvironmentRestart {
    private static final Logger LOGGER = LoggerFactory.getLogger("player_to_player/restart");
    private static RestartCoordinator flow = new RestartCoordinator(null);
    private static RestartTaskStore store;
    private static Attempt active;
    private static boolean invalidResume;
    private static volatile RestartTask recovery;
    private static boolean recoveryShown;

    private static final class Attempt {
        final RestartCoordinator.Intent intent;
        volatile boolean cancelled;
        volatile RestartHelperProcess helper;
        Attempt(RestartCoordinator.Intent intent) { this.intent = intent; }
        void abort() {
            cancelled = true;
            RestartHelperProcess value = helper;
            if (value != null) ThreadPools.startBlocking("restart-abort", value::abort);
        }
    }
    private ClientEnvironmentRestart() { }

    static void register() {
        store = new RestartTaskStore(NodeContext.get().paths().root());
        // 此处处于客户端初始化（还未进入游戏循环），仅读取一个有界本机元数据文件。
        // 必须在 QuickPlay 的首次 onConnect 前恢复预算；异步恢复会留下自动循环的竞态窗口。
        RestartTask resumed = null;
        String marker = System.getProperty(RestartTask.TASK_PROPERTY, "");
        if (!marker.isBlank()) {
            try { resumed = store.read(UUID.fromString(marker)); }
            catch (Exception failure) { invalidResume = true; }
        }
        flow = new RestartCoordinator(resumed);
        RestartTask task = resumed;
        ClientLifecycleEvents.CLIENT_STARTED.register(client -> ThreadPools.io().execute(() -> {
            try {
                if (task != null) store.clientStarted(task.id(), FabricLoader.getInstance().getGameDir(),
                        System.getProperty(ClientRuntimeProfile.RUNTIME_PROPERTY, ""));
                List<RestartTask> unfinished = store.recoverable();
                if (!unfinished.isEmpty()) recovery = unfinished.get(0);
            } catch (IOException failure) {
                LOGGER.warn("无法读取或确认本机重启状态；不会据此再次自动启动游戏");
            }
        }));
        ClientLifecycleEvents.CLIENT_STOPPING.register(client -> {
            // 退出阶段同步保存少量本机选项，不能投递到即将关闭的后台线程池。
            // 先让原版写出内存中的最终选项，再归档到该服务器的稳定个人设置目录。
            client.options.save();
            try { ClientLocalSettings.saveCurrent(FabricLoader.getInstance().getGameDir()); }
            catch (IOException failure) { LOGGER.warn("保存服务器专属客户端设置失败，当前游戏目录中的设置仍保留", failure); }
        });
        ClientTickEvents.END_CLIENT_TICK.register(client -> {
            if (recoveryShown || recovery == null || !(client.screen instanceof TitleScreen)
                    || client.getOverlay() != null) return;
            recoveryShown = true;
            client.setScreen(new EnvironmentRestartRecoveryScreen(client.screen, recovery));
        });
    }

    static RestartCoordinator.Intent begin(String address) {
        if (flow.committed()) return null;
        if (active != null) active.abort();
        active = null;
        return flow.begin(address);
    }
    static boolean committed() { return flow.committed(); }
    static boolean cancel(RestartCoordinator.Intent intent) {
        if (!flow.cancel(intent)) return false;
        if (active != null && active.intent == intent) active.abort();
        return true;
    }

    static void retryRecovered(RestartTask task) {
        // 只恢复非敏感连接链；实际命令在本次预检完成后从当前登录身份重新生成。
        flow = new RestartCoordinator(task);
        invalidResume = false;
        ThreadPools.io().execute(() -> {
            try { store.update(task.id(), old -> old.stage(RestartTask.Stage.CANCELLED, RestartTask.Failure.NONE)); }
            catch (IOException failure) { LOGGER.warn("无法标记旧重启任务已由手动重试接替"); }
        });
    }

    static void restart(Minecraft client, EnvironmentPreflightScreen screen, RestartCoordinator.Intent intent,
                        ClientRuntimeProfile.Installation installation, boolean manual, BooleanSupplier current) {
        if (!installation.canLaunch() || !current.getAsBoolean() || client.screen != screen) return;
        if (invalidResume && !manual) {
            screen.restartFailed("上一次重启记录不可读，已停止自动重启。可使用当前登录身份手动重试。");
            return;
        }
        if (active != null && active.intent == intent && !active.cancelled) return;
        if (!flow.reserve(intent, manual)) {
            screen.restartFailed("本次连接流程已尝试过自动重启，但环境仍不匹配或服务器已更新。请检查原因后手动重试。");
            return;
        }
        Attempt attempt = new Attempt(intent);
        active = attempt;
        screen.preparingRestart();
        // 当前身份只在内存中捕获，辅助进程管道关闭后不再保留命令副本。
        final List<String> command;
        try {
            client.options.save();
            command = installation.commandForRestart();
            if (command.isEmpty()) throw new IOException("无法重建启动方式");
        } catch (Exception failure) {
            failed(client, screen, attempt, current);
            return;
        }
        ThreadPools.startBlocking("restart-prepare", () -> {
            try {
                if (attempt.cancelled) return;
                // 文件复制留在后台；失败时不提交重启，避免用默认音量/语言启动新进程。
                installation.prepareLocalSettings();
                RestartTask task = store.create(intent.chainId(), intent.address(), installation.runtimeHash(), installation.directory());
                attempt.helper = RestartHelperProcess.prepare(store, task, command);
                if (attempt.cancelled) { attempt.helper.abort(); return; }
                client.execute(() -> {
                    // 最终检查必须在主线程进行，取消/切服和 COMMIT 因此具有清晰的先后顺序。
                    if (!current.getAsBoolean() || client.screen != screen || attempt.cancelled || !flow.commit(intent)) {
                        cancel(intent);
                        attempt.abort();
                        return;
                    }
                    screen.restartCommitted();
                    ThreadPools.startBlocking("restart-commit", () -> {
                        try {
                            attempt.helper.commit();
                            client.execute(client::stop);
                        } catch (Exception failure) { failed(client, screen, attempt, current); }
                    });
                });
            } catch (Exception failure) { failed(client, screen, attempt, current); }
        });
    }

    private static void failed(Minecraft client, EnvironmentPreflightScreen screen, Attempt attempt, BooleanSupplier current) {
        // 失败前 HelperProcess 已确认助手停止；此处只恢复按钮，绝不退出正在运行的游戏。
        if (attempt.helper != null) attempt.helper.abort();
        attempt.cancelled = true;
        client.execute(() -> {
            if (!flow.fail(attempt.intent) || !current.getAsBoolean() || client.screen != screen) return;
            screen.restartFailed("自动重启未能准备或确认，当前游戏已保留。可手动重试，或打开目录按启动说明运行。");
            LOGGER.warn("环境重启准备/提交失败，原客户端继续运行");
        });
    }
}
