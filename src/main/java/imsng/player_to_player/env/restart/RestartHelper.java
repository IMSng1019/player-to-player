package imsng.player_to_player.env.restart;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.nio.file.Files;
import java.time.Duration;
import java.util.ArrayList;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static imsng.player_to_player.env.restart.RestartTask.*;

/**
 * 独立 JDK 入口，不初始化游戏/Loader/日志框架。参数只读匿名 stdin，stdout 只发确认码。
 * 旧 JVM 的退出等待没有截止时间；存档屏障失败时旧游戏可无限期留在退出界面。
 */
public final class RestartHelper {
    public static final Duration STARTUP_TIMEOUT = Duration.ofMinutes(5);
    private RestartHelper() { }

    public static void main(String[] ignored) {
        // 仅影响这个专用助手：Windows 参数已按 Java 启动器的 argv 规则编码，
        // 防止 ProcessBuilder 的兼容模式吞掉 JSON/认证参数里的字面双引号。
        System.setProperty("jdk.lang.Process.allowAmbiguousCommands", "true");
        RestartTaskStore store = null;
        UUID id = null;
        Process child = null;
        try {
            DataInputStream input = new DataInputStream(System.in);
            DataOutputStream output = new DataOutputStream(System.out);
            RestartWire.Request payload = RestartWire.read(input);
            store = new RestartTaskStore(payload.dataRoot());
            id = payload.taskId();
            RestartTask task = store.read(id);
            if (task.stage() != Stage.PREPARING) return;
            ProcessIdentity identity = ProcessIdentity.of(ProcessHandle.current());
            store.update(id, old -> old.helper(identity).stage(Stage.READY, Failure.NONE));
            output.writeInt(RestartWire.READY);
            output.flush();
            // 父进程崩溃/取消/关闭管道时 readInt 抛 EOF；没有 COMMIT 绝不启动。
            if (input.readInt() != RestartWire.COMMIT) return;
            store.update(id, old -> old.stage(Stage.COMMITTED, Failure.NONE));
            output.writeInt(RestartWire.COMMITTED);
            output.flush();

            waitForSource(task.source(), ProcessHandle.of(task.source().pid()));
            if (!Files.isDirectory(task.directory())) throw new IllegalStateException("安装目录不存在");
            store.update(id, old -> old.stage(Stage.LAUNCHING, Failure.NONE));
            var command = new ArrayList<>(payload.command());
            command.removeIf(arg -> arg.startsWith("-D" + TASK_PROPERTY + "=") || arg.startsWith("-Duser.dir="));
            command.add(1, "-D" + TASK_PROPERTY + "=" + id);
            command.add(1, "-Duser.dir=" + task.directory());
            ProcessBuilder builder = new ProcessBuilder(nativeCommand(command)).directory(task.directory().toFile())
                    .redirectErrorStream(true).redirectOutput(task.directory().resolve("client-launch.log").toFile());
            RestartHelperProcess.clearJavaEnvironment(builder);
            child = builder.start();
            child.getOutputStream().close();
            // 新进程可能在这里之前发出 CLIENT_STARTED；只补 PID，不倒退其状态。
            if (child.isAlive()) {
                var startedAt = child.info().startInstant();
                if (startedAt.isPresent()) {
                    ProcessIdentity started = new ProcessIdentity(child.pid(), startedAt.get().toEpochMilli());
                    store.update(id, old -> old.child(started));
                } else if (child.isAlive()) {
                    throw new IllegalStateException("无法确认新进程身份");
                }
                // 若刚在 isAlive 与 info 之间退出，让下方监视器准确记录 CHILD_EXITED。
            }
            awaitStartup(store, id, child, STARTUP_TIMEOUT);
        } catch (Exception failure) {
            if (store != null && id != null) {
                try {
                    boolean launched = child != null;
                    boolean exited = launched && !child.isAlive();
                    store.update(id, old -> {
                        if (old.stage() == Stage.CLIENT_STARTED || old.stage() == Stage.CANCELLED) return old;
                        if (exited) return old.stage(Stage.FAILED, Failure.CHILD_EXITED);
                        if (launched) return old.stage(Stage.INITIALIZATION_UNCONFIRMED, Failure.INITIALIZATION_UNCONFIRMED);
                        return old.stage(Stage.FAILED, old.stage() == Stage.LAUNCHING ? Failure.LAUNCH_FAILED : Failure.HELPER_FAILED);
                    });
                } catch (Exception ignoredFailure) { }
            }
            // 若启动成功后状态盘写失败，助手保持存活直到游戏结束，避免恢复页启动第二份游戏。
            if (child != null) {
                try { child.waitFor(); } catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); }
            }
        }
    }

    /**
     * Windows 没有 argv 系统调用。每个参数单独加引号，并加倍双引号前和参数尾的反斜线；
     * JDK 兼容模式保留已引用参数。这里直接调用 java(w).exe，绝不交给 cmd/PowerShell。
     * Unix ProcessBuilder 原生传 argv，无需此编码。
     */
    private static java.util.List<String> nativeCommand(java.util.List<String> command) {
        if (!System.getProperty("os.name", "").toLowerCase(java.util.Locale.ROOT).contains("win")) return command;
        var encoded = new ArrayList<String>();
        encoded.add(command.get(0));
        for (String argument : command.subList(1, command.size())) {
            StringBuilder value = new StringBuilder("\"");
            int slashes = 0;
            for (int i = 0; i < argument.length(); i++) {
                char c = argument.charAt(i);
                if (c == '\\') { slashes++; continue; }
                value.append("\\".repeat(c == '"' ? slashes * 2 + 1 : slashes));
                value.append(c);
                slashes = 0;
            }
            encoded.add(value.append("\\".repeat(slashes * 2)).append('"').toString());
        }
        return encoded;
    }

    static void waitForSource(ProcessIdentity source, java.util.Optional<ProcessHandle> handle) throws Exception {
        if (handle.isEmpty() || !handle.get().isAlive()) return;
        var startedAt = handle.get().info().startInstant();
        if (startedAt.isEmpty()) {
            // 正常退出会立即撤销进程元数据；不能把这段窗口误判为“交接失败”。
            if (!handle.get().isAlive()) return;
            throw new IllegalStateException("无法确认旧进程身份");
        }
        if (startedAt.get().toEpochMilli() == source.startedAt()) handle.get().onExit().get();
    }

    static void awaitStartup(RestartTaskStore store, UUID id, Process child, Duration timeout) throws Exception {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (true) {
            if (store.read(id).stage() == Stage.CLIENT_STARTED) return;
            if (!child.isAlive()) { store.finishUnconfirmed(id, Failure.CHILD_EXITED); return; }
            if (System.nanoTime() >= deadline) {
                store.finishUnconfirmed(id, Failure.INITIALIZATION_UNCONFIRMED);
                return; // 不终止仍在加载的游戏，也不重复启动。
            }
            child.waitFor(200, TimeUnit.MILLISECONDS);
        }
    }
}
