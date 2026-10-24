package imsng.player_to_player.env.restart;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static imsng.player_to_player.env.restart.RestartTask.*;

/**
 * 游戏侧的两阶段握手。prepare 只把内存参数交给小 JVM；commit 必须由客户端重新核验
 * 连接意图后调用。只有收到 COMMITTED 才能执行正常退出。所有阻塞操作由后台线程调用。
 */
public final class RestartHelperProcess {
    public static final Duration HANDSHAKE_TIMEOUT = Duration.ofSeconds(10);
    private final Process process;
    private final RestartTaskStore store;
    private final RestartTask task;
    private final DataInputStream input;
    private final DataOutputStream output;
    private final AtomicBoolean aborted = new AtomicBoolean();
    private final AtomicBoolean committed = new AtomicBoolean();

    private RestartHelperProcess(Process process, RestartTaskStore store, RestartTask task) {
        this.process = process;
        this.store = store;
        this.task = task;
        input = new DataInputStream(process.getInputStream());
        output = new DataOutputStream(process.getOutputStream());
    }

    public static RestartHelperProcess prepare(RestartTaskStore store, RestartTask task, List<String> command)
            throws IOException {
        return prepare(store, task, command, helperCommand(), HANDSHAKE_TIMEOUT);
    }

    static RestartHelperProcess prepare(RestartTaskStore store, RestartTask task, List<String> command,
                                        List<String> helperCommand, Duration timeout) throws IOException {
        RestartHelperProcess helper = null;
        try {
            ProcessBuilder builder = new ProcessBuilder(helperCommand).redirectError(ProcessBuilder.Redirect.DISCARD);
            clearJavaEnvironment(builder);
            helper = new RestartHelperProcess(builder.start(), store, task);
            RestartHelperProcess channel = helper;
            channel.exchange(() -> RestartWire.write(channel.output,
                    new RestartWire.Request(store.dataRoot(), task.id(), command)), RestartWire.READY, timeout);
            return channel;
        } catch (Exception failure) {
            if (helper != null) helper.abort();
            store.update(task.id(), old -> old.stage(Stage.FAILED, Failure.HANDSHAKE_FAILED));
            // 不附加原异常/命令，第三方 Java agent 的异常消息可能包含认证参数。
            throw new IOException("重启辅助进程未在限定时间内就绪，原客户端已保留。");
        }
    }

    public void commit() throws IOException {
        if (aborted.get() || !committed.compareAndSet(false, true)) throw new IOException("重启任务已取消或已提交");
        try {
            exchange(() -> { output.writeInt(RestartWire.COMMIT); output.flush(); },
                    RestartWire.COMMITTED, HANDSHAKE_TIMEOUT);
            input.close();
            output.close();
        } catch (IOException failure) {
            abort();
            store.update(task.id(), old -> old.stage(Stage.FAILED, Failure.HANDSHAKE_FAILED));
            throw new IOException("重启提交未确认，原客户端已保留，可手动重试。");
        }
    }

    /** 只结束本次小助手，永远不强杀旧游戏/新游戏。确保助手结束后才能向玩家开放重试。 */
    public void abort() {
        if (!aborted.compareAndSet(false, true)) return;
        process.destroy();
        boolean interrupted = false;
        while (process.isAlive()) {
            try {
                if (!process.waitFor(2, TimeUnit.SECONDS)) process.destroyForcibly();
            } catch (InterruptedException ignored) { interrupted = true; process.destroyForcibly(); }
        }
        try { input.close(); } catch (IOException ignored) { }
        try { output.close(); } catch (IOException ignored) { }
        try {
            store.update(task.id(), old -> old.stage() == Stage.PREPARING || old.stage() == Stage.READY
                    ? old.stage(Stage.CANCELLED, Failure.NONE) : old);
        } catch (IOException ignored) { }
        if (interrupted) Thread.currentThread().interrupt();
    }

    private interface WriteRequest { void write() throws IOException; }
    private void exchange(WriteRequest write, int expected, Duration timeout) throws IOException {
        CompletableFuture<Void> result = new CompletableFuture<>();
        Thread transfer = new Thread(() -> {
            try {
                write.write();
                if (input.readInt() != expected) throw new IOException("重启确认无效");
                result.complete(null);
            } catch (Exception failure) { result.completeExceptionally(new IOException("重启管道已关闭")); }
        }, "p2p-restart-handshake");
        transfer.setDaemon(true);
        transfer.start();
        try { result.get(timeout.toMillis(), TimeUnit.MILLISECONDS); }
        catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IOException("重启握手被中断");
        } catch (Exception failure) { throw new IOException("重启握手失败或超时"); }
    }

    static List<String> helperCommand() throws IOException {
        try {
            boolean windows = System.getProperty("os.name", "").toLowerCase(Locale.ROOT).contains("win");
            Path java = Path.of(System.getProperty("java.home"), "bin", windows ? "javaw.exe" : "java");
            if (!Files.isRegularFile(java)) throw new IOException("找不到本机 Java");
            Path classes = Path.of(RestartHelper.class.getProtectionDomain().getCodeSource().getLocation().toURI());
            return List.of(java.toString(), "-Xms16m", "-Xmx64m", "-XX:+UseSerialGC", "-cp",
                    classes.toString(), RestartHelper.class.getName());
        } catch (Exception failure) { throw new IOException("无法确定本机辅助进程的启动方式"); }
    }

    /** 有效游戏 JVM 参数已从 MXBean 重建；环境注入不能再覆盖助手堆大小或重复加载认证 agent。 */
    static void clearJavaEnvironment(ProcessBuilder builder) {
        for (String key : List.of("JAVA_TOOL_OPTIONS", "_JAVA_OPTIONS", "JDK_JAVA_OPTIONS", "CLASSPATH"))
            builder.environment().remove(key);
    }
}
