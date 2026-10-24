package imsng.player_to_player.env.restart;

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.UnaryOperator;

import static imsng.player_to_player.env.restart.RestartTask.*;

/**
 * 共享数据目录下的一任务一文件日志。临时文件 fsync 后原子替换；跨 JVM 文件锁保护
 * “读－判定－写”，避免辅助进程超时与 CLIENT_STARTED 回报互相覆盖。读取只看到完整旧版或新版。
 */
public final class RestartTaskStore {
    private static final ConcurrentHashMap<Path, Object> LOCKS = new ConcurrentHashMap<>();
    private final Path dataRoot;
    private final Path root;

    public RestartTaskStore(Path dataRoot) {
        this.dataRoot = dataRoot.toAbsolutePath().normalize();
        this.root = this.dataRoot.resolve("client-restarts");
    }
    public Path dataRoot() { return dataRoot; }
    public Path taskFile(UUID id) { return root.resolve(id + ".properties"); }

    public RestartTask create(UUID chainId, String address, String hash, Path directory) throws IOException {
        var task = new RestartTask(UUID.randomUUID(), chainId, address, hash, directory, 1,
                ProcessIdentity.of(ProcessHandle.current()), null, null,
                Stage.PREPARING, Failure.NONE, System.currentTimeMillis());
        writeAtomic(task);
        return task;
    }

    public RestartTask read(UUID id) throws IOException {
        // Windows 的普通文件读句柄不共享 DELETE；读取也需持锁，避免 rename 被读者拒绝。
        Files.createDirectories(root);
        Path lock = root.resolve(id + ".lock");
        synchronized (LOCKS.computeIfAbsent(lock, ignored -> new Object())) {
            try (FileChannel channel = FileChannel.open(lock, StandardOpenOption.CREATE, StandardOpenOption.WRITE);
                 var held = channel.lock()) {
                return readUnlocked(id);
            }
        }
    }

    private RestartTask readUnlocked(UUID id) throws IOException {
        try {
            Path file = taskFile(id);
            if (Files.size(file) > 64 * 1024) throw new IOException("重启记录过大");
            Properties p = new Properties();
            p.load(new StringReader(Files.readString(file, StandardCharsets.UTF_8)));
            if (!"1".equals(p.getProperty("version")) || !id.toString().equals(p.getProperty("id")))
                throw new IOException("重启记录版本或标识无效");
            return new RestartTask(id, UUID.fromString(p.getProperty("chainId")), p.getProperty("address"),
                    p.getProperty("runtimeHash"), Path.of(p.getProperty("directory")),
                    Integer.parseInt(p.getProperty("automaticRestarts")), identity(p, "source"),
                    identity(p, "helper"), identity(p, "child"), Stage.valueOf(p.getProperty("stage")),
                    Failure.valueOf(p.getProperty("failure")), Long.parseLong(p.getProperty("updatedAt")));
        } catch (RuntimeException invalid) {
            throw new IOException("重启记录无效");
        }
    }

    public RestartTask update(UUID id, UnaryOperator<RestartTask> change) throws IOException {
        Files.createDirectories(root);
        Path lock = root.resolve(id + ".lock");
        synchronized (LOCKS.computeIfAbsent(lock, ignored -> new Object())) {
            try (FileChannel channel = FileChannel.open(lock, StandardOpenOption.CREATE, StandardOpenOption.WRITE);
                 var held = channel.lock()) {
                RestartTask old = readUnlocked(id);
                RestartTask next = change.apply(old);
                if (!old.equals(next)) writeAtomic(next);
                return next;
            }
        }
    }

    /** 启动回报绑定安装目录、运行版和新 JVM，不能拿别的实例的回报结束本任务。 */
    public boolean clientStarted(UUID id, Path directory, String hash) throws IOException {
        ProcessIdentity process = ProcessIdentity.of(ProcessHandle.current());
        RestartTask value = update(id, old -> {
            if (!old.directory().equals(directory.toAbsolutePath().normalize()) || !old.runtimeHash().equals(hash)
                    || (old.child() != null && !old.child().equals(process))) return old;
            if (old.stage() != Stage.LAUNCHING && old.stage() != Stage.INITIALIZATION_UNCONFIRMED) return old;
            return old.child(process).stage(Stage.CLIENT_STARTED, Failure.NONE);
        });
        return value.stage() == Stage.CLIENT_STARTED && process.equals(value.child())
                && value.directory().equals(directory.toAbsolutePath().normalize()) && value.runtimeHash().equals(hash);
    }

    public void finishUnconfirmed(UUID id, Failure reason) throws IOException {
        update(id, old -> old.stage() != Stage.LAUNCHING ? old
                : old.stage(reason == Failure.INITIALIZATION_UNCONFIRMED
                ? Stage.INITIALIZATION_UNCONFIRMED : Stage.FAILED, reason));
    }

    /** 只提供已经没有相关存活进程的未完成任务；仍在慢速加载的游戏绝不提供重复启动入口。 */
    public List<RestartTask> recoverable() throws IOException {
        if (!Files.isDirectory(root)) return List.of();
        List<RestartTask> result = new ArrayList<>();
        try (var files = Files.newDirectoryStream(root, "*.properties")) {
            for (Path file : files) {
                try {
                    String name = file.getFileName().toString();
                    RestartTask task = read(UUID.fromString(name.substring(0, name.length() - 11)));
                    if (task.stage() != Stage.CLIENT_STARTED && task.stage() != Stage.CANCELLED && !task.hasLiveProcess())
                        result.add(task);
                } catch (IOException | IllegalArgumentException corrupt) {
                    // 单个损坏文件不妨碍其他任务的恢复，更不能据此执行未知启动命令。
                }
            }
        }
        result.sort(Comparator.comparingLong(RestartTask::updatedAt).reversed());
        return List.copyOf(result);
    }

    private void writeAtomic(RestartTask task) throws IOException {
        Files.createDirectories(root);
        Properties p = new Properties();
        p.setProperty("version", "1");
        p.setProperty("id", task.id().toString());
        p.setProperty("chainId", task.chainId().toString());
        p.setProperty("address", task.address());
        p.setProperty("runtimeHash", task.runtimeHash());
        p.setProperty("directory", task.directory().toString());
        p.setProperty("automaticRestarts", Integer.toString(task.automaticRestarts()));
        identity(p, "source", task.source());
        identity(p, "helper", task.helper());
        identity(p, "child", task.child());
        p.setProperty("stage", task.stage().name());
        p.setProperty("failure", task.failure().name());
        p.setProperty("updatedAt", Long.toString(task.updatedAt()));
        StringWriter text = new StringWriter();
        p.store(text, "P2P local restart task; no launch arguments");
        Path temporary = Files.createTempFile(root, task.id() + "-", ".tmp");
        try {
            try (FileChannel file = FileChannel.open(temporary, StandardOpenOption.WRITE)) {
                ByteBuffer bytes = StandardCharsets.UTF_8.encode(text.toString());
                while (bytes.hasRemaining()) file.write(bytes);
                file.force(true);
            }
            // 不支持原子重命名的文件系统禁用自动重启，保留原游戏和手动启动说明。
            Files.move(temporary, taskFile(task.id()), StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } finally { Files.deleteIfExists(temporary); }
    }
    private static ProcessIdentity identity(Properties p, String key) {
        String pid = p.getProperty(key + "Pid");
        return pid == null ? null : new ProcessIdentity(Long.parseLong(pid), Long.parseLong(p.getProperty(key + "StartedAt")));
    }
    private static void identity(Properties p, String key, ProcessIdentity value) {
        if (value == null) return;
        p.setProperty(key + "Pid", Long.toString(value.pid()));
        p.setProperty(key + "StartedAt", Long.toString(value.startedAt()));
    }
}
