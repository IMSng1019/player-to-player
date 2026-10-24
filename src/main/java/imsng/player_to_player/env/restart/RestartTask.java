package imsng.player_to_player.env.restart;

import java.nio.file.Path;
import java.util.Objects;
import java.util.UUID;

/**
 * 本机重启的耐久记录。刻意只保留恢复所需的元数据：命令、令牌、玩家身份和认证代理
 * 参数绝不进入此类型。辅助进程只依赖 JDK，因此此包不能引用 Minecraft、Fabric 或 Gson。
 */
public record RestartTask(UUID id, UUID chainId, String address, String runtimeHash, Path directory,
                          int automaticRestarts, ProcessIdentity source, ProcessIdentity helper,
                          ProcessIdentity child, Stage stage, Failure failure, long updatedAt) {
    public static final String TASK_PROPERTY = "player_to_player.restartTask";

    public enum Stage {
        PREPARING, READY, COMMITTED, LAUNCHING, CLIENT_STARTED,
        FAILED, INITIALIZATION_UNCONFIRMED, CANCELLED
    }
    public enum Failure {
        NONE, HANDSHAKE_FAILED, HELPER_FAILED, LAUNCH_FAILED, CHILD_EXITED,
        INITIALIZATION_UNCONFIRMED, INTERRUPTED
    }

    /** 同时比较 PID 和启动时间，不能把系统重用的 PID 当作仍在退出的旧游戏。 */
    public record ProcessIdentity(long pid, long startedAt) {
        public static ProcessIdentity of(ProcessHandle process) {
            return new ProcessIdentity(process.pid(), process.info().startInstant().orElseThrow(
                    () -> new IllegalStateException("无法确定进程启动时间")).toEpochMilli());
        }
        public boolean alive() {
            return ProcessHandle.of(pid).filter(ProcessHandle::isAlive)
                    .filter(p -> p.info().startInstant().map(t -> t.toEpochMilli() == startedAt).orElse(true))
                    .isPresent();
        }
    }

    public RestartTask {
        Objects.requireNonNull(id);
        Objects.requireNonNull(chainId);
        Objects.requireNonNull(address);
        Objects.requireNonNull(source);
        Objects.requireNonNull(stage);
        Objects.requireNonNull(failure);
        if (!runtimeHash.matches("[0-9a-f]{64}") || automaticRestarts != 1)
            throw new IllegalArgumentException("重启记录版本无效");
        directory = directory.toAbsolutePath().normalize();
    }

    public RestartTask stage(Stage next, Failure reason) {
        return new RestartTask(id, chainId, address, runtimeHash, directory, automaticRestarts,
                source, helper, child, next, reason, System.currentTimeMillis());
    }
    public RestartTask helper(ProcessIdentity value) {
        return new RestartTask(id, chainId, address, runtimeHash, directory, automaticRestarts,
                source, value, child, stage, failure, System.currentTimeMillis());
    }
    public RestartTask child(ProcessIdentity value) {
        return new RestartTask(id, chainId, address, runtimeHash, directory, automaticRestarts,
                source, helper, value, stage, failure, System.currentTimeMillis());
    }
    public boolean hasLiveProcess() {
        return source.alive() || (helper != null && helper.alive()) || (child != null && child.alive());
    }
}
