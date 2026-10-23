package imsng.player_to_player.group;

import net.minecraft.server.MinecraftServer;

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;

/**
 * 为预同步等必要控制操作提供服务器实例专属的主线程通道。
 * 原版同步 getChunk 的 managedBlock 只泵区块执行器，不推进普通服务器任务；如果快照
 * 仍排入 server.execute，就会形成“区块等待合并、合并等待快照”的循环等待。
 * 此队列同时由普通服务器轮询和区块等待轮询驱动，但绝不泵任意游戏指令或网络任务，
 * 也不改变区块所有权门控。任务必须自行检查所属事务的取消状态，且不得等待世界 tick。
 */
public final class GroupControlTasks {
    private static final ConcurrentHashMap<MinecraftServer, GroupControlTaskQueue> QUEUES =
            new ConcurrentHashMap<>();

    private GroupControlTasks() {
    }

    /** 后台可取得/使用执行器；实例停止或切换后，旧执行器会拒绝新任务。 */
    public static Executor executor(MinecraftServer server) {
        Objects.requireNonNull(server, "服务器实例");
        if (!isCurrent(server)) {
            discard(server);
            throw new RejectedExecutionException("当前服务器不是运行中的受管组世界");
        }
        return QUEUES.computeIfAbsent(server, ignored -> new GroupControlTaskQueue(
                server.getRunningThread(), () -> isCurrent(server)));
    }

    /** 仅在服务器主线程的安全轮询点执行至多一项专用任务，未接管的世界直接放行。 */
    public static boolean poll(MinecraftServer server) {
        GroupControlTaskQueue queue = QUEUES.get(server);
        if (queue == null) {
            return false;
        }
        if (!isCurrent(server)) {
            discard(server);
            return false;
        }
        return queue.poll();
    }

    /** detach 时清理实例队列；既有 Executor 引用同样被永久关闭。 */
    public static void discard(MinecraftServer server) {
        if (server == null) {
            return;
        }
        GroupControlTaskQueue queue = QUEUES.remove(server);
        if (queue != null) {
            queue.close();
        }
    }

    private static boolean isCurrent(MinecraftServer server) {
        // 原版在调用 stopServer/saveAll 之前就置 stopped=true；保存仍需要泵预卸载
        // 的最终快照与释放依赖。必须等 SERVER_STOPPED→detach 才退休这条通道。
        return GroupRuntime.isManagedServer(server);
    }
}
