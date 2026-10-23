package imsng.player_to_player.client.session;

import imsng.player_to_player.client.group.WorldSwitcher;
import imsng.player_to_player.client.mixin.PauseScreenAccessor;
import imsng.player_to_player.group.ExitBundle;
import imsng.player_to_player.group.ExitManifest;
import imsng.player_to_player.group.ExitTransactionClient;
import imsng.player_to_player.util.ThreadPools;
import imsng.player_to_player.runtime.WorkerProcess;
import net.minecraft.client.Minecraft;
import net.minecraft.client.gui.screens.ProgressScreen;
import net.minecraft.client.gui.screens.PauseScreen;
import net.minecraft.client.gui.screens.Screen;
import net.minecraft.client.gui.screens.TitleScreen;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 客户端主客户端退出屏障。
 * <p>
 * Mixin 只负责询问“是否取消本次原版调用”；完整流程在后台执行，渲染线程保持原版
 * 进度界面可响应。只有 EXIT_COMMIT 获得 committed=true 后才旁路执行真正的 stop。
 */
public final class ClientExitCoordinator {

    private static final Logger LOGGER =
            LoggerFactory.getLogger("player_to_player/client-exit");

    private static final AtomicBoolean ACTIVE = new AtomicBoolean();
    private static final AtomicBoolean BYPASS_DISCONNECT = new AtomicBoolean();
    private static final AtomicBoolean BYPASS_STOP = new AtomicBoolean();
    private static final AtomicBoolean BYPASS_PAUSE_DISCONNECT = new AtomicBoolean();
    private static final AtomicBoolean CLOSE_PROCESS_REQUESTED = new AtomicBoolean();

    private static volatile CompletableFuture<ExitBundle> bundleFuture;

    private ClientExitCoordinator() {
    }

    private enum ExitKind {
        DIRECT_DISCONNECT,
        PAUSE_DISCONNECT,
        STOP
    }

    /** @return true 表示 Mixin 必须取消本次原版 disconnect。 */
    public static boolean interceptDisconnect(Minecraft minecraft, Screen targetScreen) {
        if (BYPASS_DISCONNECT.getAndSet(false)) {
            return false;
        }
        if (ACTIVE.get()) {
            return true;
        }
        if (WorldSwitcher.isSwitching() || !isManagedWorld(minecraft)) {
            return false;
        }
        CLOSE_PROCESS_REQUESTED.set(false);
        start(minecraft, targetScreen, ExitKind.DIRECT_DISCONNECT);
        return true;
    }

    /** 暂停菜单必须在 ClientLevel.disconnect() 之前拦截。 */
    public static boolean interceptPauseDisconnect(PauseScreen pauseScreen) {
        if (BYPASS_PAUSE_DISCONNECT.getAndSet(false)) {
            return false;
        }
        Minecraft minecraft = Minecraft.getInstance();
        if (ACTIVE.get()) {
            return true;
        }
        if (WorldSwitcher.isSwitching() || !isManagedWorld(minecraft)) {
            return false;
        }
        CLOSE_PROCESS_REQUESTED.set(false);
        start(minecraft, pauseScreen, ExitKind.PAUSE_DISCONNECT);
        return true;
    }

    /** @return true 表示 Mixin 必须取消本次原版 stop。 */
    public static boolean interceptStop(Minecraft minecraft) {
        if (BYPASS_STOP.getAndSet(false)) {
            return false;
        }
        if (ACTIVE.get()) {
            CLOSE_PROCESS_REQUESTED.set(true);
            return true;
        }
        if (!isManagedWorld(minecraft)) {
            return false;
        }
        CLOSE_PROCESS_REQUESTED.set(true);
        start(minecraft, new ProgressScreen(true), ExitKind.STOP);
        return true;
    }

    public static boolean isActive() {
        return ACTIVE.get();
    }

    /** 子进程在 SERVER_STOPPED 后完成最终本机尾部文件，父进程从IO线程通知。 */
    public static void onExitBundle(ExitBundle bundle) {
        CompletableFuture<ExitBundle> future = bundleFuture;
        if (future == null || !future.complete(bundle)) {
            throw new IllegalStateException("退出协调器尚未接受最终 bundle");
        }
    }

    private static boolean isManagedWorld(Minecraft minecraft) {
        return minecraft != null
                && WorkerProcess.isManagedWorld();
    }

    private static void start(Minecraft minecraft, Screen sourceScreen,
                              ExitKind kind) {
        if (!ACTIVE.compareAndSet(false, true)) {
            return; // 已有退出流程：后续退出/关闭请求继续被拦截
        }
        ProgressScreen progress = new ProgressScreen(true);
        minecraft.setScreen(progress);
        CompletableFuture<ExitBundle> finalBundle = new CompletableFuture<>();
        bundleFuture = finalBundle;
        long workflowStartedNanos = System.nanoTime();

        // 退出流程内是 "无限重试直到服务端接受"（EXIT_BEGIN/EXIT_COMMIT），
        // 且同一条线程上还要等区块批次 ACK 上行完成 —— 属长驻阻塞任务，必须走专用线程：
        // 若它占住有界 io 池的名额，等待的批次上传任务反而排不上队，自我死锁。
        ThreadPools.startBlocking("client-exit", () -> runWorkflow(
                minecraft, sourceScreen, progress, kind, finalBundle,
                workflowStartedNanos));
    }

    private static void runWorkflow(Minecraft minecraft, Screen sourceScreen,
                                    ProgressScreen progress, ExitKind kind,
                                    CompletableFuture<ExitBundle> finalBundle,
                                    long workflowStartedNanos) {
        try {
            UUID shutdownId = UUID.randomUUID();
            long phaseStarted = System.nanoTime();
            ExitManifest local = beginLocalUntilReady(shutdownId);
            long prepareMillis = elapsedMillis(phaseStarted);
            ExitTransactionClient transaction = WorldSession.createExitTransactionClient();
            phaseStarted = System.nanoTime();
            ExitTransactionClient.BeginOutcome begin =
                    transaction.beginUntilAccepted(local);
            long beginMillis = elapsedMillis(phaseStarted);
            WorkerProcess.adoptExitManifest(begin.manifest());
            LOGGER.info("物理服务端已确认退出保护意图: shutdownId={} ownedChunks={} players={} "
                            + "exit.prepare_ms={} exit.begin_ms={}",
                    shutdownId, begin.manifest().chunks().size(),
                    begin.manifest().playerIds().size(), prepareMillis, beginMillis);

            CompletableFuture<Void> disconnected = new CompletableFuture<>();
            minecraft.execute(() -> {
                try {
                    if (kind == ExitKind.PAUSE_DISCONNECT) {
                        BYPASS_PAUSE_DISCONNECT.set(true);
                        BYPASS_DISCONNECT.set(true);
                        ((PauseScreenAccessor) sourceScreen)
                                .player_to_player$invokeOnDisconnect();
                        // 原版方法会立即切标题页；最终提交前继续显示保存进度。
                        minecraft.setScreen(progress);
                    } else {
                        BYPASS_DISCONNECT.set(true);
                        minecraft.disconnect(progress);
                    }
                    disconnected.complete(null);
                } catch (Throwable t) {
                    disconnected.completeExceptionally(t);
                } finally {
                    // 正常路径会在对应 Mixin 的 HEAD 消费一次性旗标；若重放调用
                    // 在进入目标方法前异常，也必须撤销许可，避免下一次关闭绕过屏障。
                    BYPASS_PAUSE_DISCONNECT.set(false);
                    BYPASS_DISCONNECT.set(false);
                }
            });
            disconnected.join();

            // 游戏连接断开不再隐式停止集成服：显式通知独立计算进程保存并停服。
            // finalBundle只有子服SERVER_STOPPED之后的耐久本机尾部就绪事件才能完成。
            WorkerProcess.stopForExit().join();

            phaseStarted = System.nanoTime();
            ExitBundle bundle = finalBundle.join();
            long saveMillis = elapsedMillis(phaseStarted);
            LOGGER.info("主客户端退出最终数据已收集: shutdownId={} ownedChunks={} "
                            + "writtenChunks={} compressedBytes={} exit.save_ms={}",
                    shutdownId, bundle.manifest().chunks().size(),
                    bundle.writtenChunks().size(), bundle.compressedBytes(), saveMillis);
            phaseStarted = System.nanoTime();
            transaction.commitUntilAccepted(bundle);
            long commitMillis = elapsedMillis(phaseStarted);
            long durableTotalMillis = elapsedMillis(workflowStartedNanos);
            LOGGER.info("主客户端退出事务已提交: shutdownId={} exit.commit_ms={} "
                            + "exit.total_to_durable_ms={}",
                    shutdownId, commitMillis, durableTotalMillis);
            phaseStarted = System.nanoTime();
            WorldSession.teardownSession();
            LOGGER.info("主客户端退出会话清理完成: shutdownId={} exit.teardown_ms={}",
                    shutdownId, elapsedMillis(phaseStarted));

            minecraft.execute(() -> {
                bundleFuture = null;
                ACTIVE.set(false);
                if (CLOSE_PROCESS_REQUESTED.getAndSet(false)) {
                    BYPASS_STOP.set(true);
                    try {
                        minecraft.stop();
                    } finally {
                        BYPASS_STOP.set(false);
                    }
                } else if (kind == ExitKind.PAUSE_DISCONNECT) {
                    minecraft.setScreen(new TitleScreen());
                } else {
                    minecraft.setScreen(sourceScreen);
                }
            });
        } catch (Throwable t) {
            // 本地编排异常不能绕过数据屏障；保留进度界面与 ACTIVE，方便日志诊断。
            LOGGER.error("主客户端耐久退出编排失败，关闭继续被阻止", t);
        }
    }

    private static ExitManifest beginLocalUntilReady(UUID shutdownId) {
        int attempt = 0;
        while (true) {
            attempt++;
            try {
                return WorkerProcess.beginExitDrain(shutdownId).get();
            } catch (Exception e) {
                LOGGER.warn("固定本地退出清单失败（第 {} 次），继续重试: {}",
                        attempt, e.toString());
                sleepUninterruptibly(1, TimeUnit.SECONDS);
            }
        }
    }

    private static void sleepUninterruptibly(long duration, TimeUnit unit) {
        long deadline = System.nanoTime() + unit.toNanos(duration);
        while (true) {
            long remaining = deadline - System.nanoTime();
            if (remaining <= 0) {
                return;
            }
            try {
                TimeUnit.NANOSECONDS.sleep(remaining);
            } catch (InterruptedException ignored) {
                // 用户明确要求退出事务不可取消。
            }
        }
    }

    private static long elapsedMillis(long startedNanos) {
        return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedNanos);
    }
}
