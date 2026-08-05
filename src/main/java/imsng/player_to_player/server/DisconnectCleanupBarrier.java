package imsng.player_to_player.server;

import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 同一客户端旧连接清理与新会话操作之间的生命周期屏障。
 *
 * <p>断连处理在 HELLO 会话锁内发布 future；EXIT_BEGIN/ROLE_REQUEST 在读取组表和
 * ownership 前调用 {@link #await(UUID)}。重叠清理使用 allOf 合并，后发布的 future
 * 不能覆盖仍在释放 ownership 的旧任务。</p>
 */
final class DisconnectCleanupBarrier {

    private final ConcurrentHashMap<UUID, CompletableFuture<Void>> pending =
            new ConcurrentHashMap<>();

    /** 发布一次清理；返回值会在全部先行清理和本次清理完成后自动移除。 */
    CompletableFuture<Void> publish(UUID clientId, CompletableFuture<Void> cleanup) {
        Objects.requireNonNull(clientId, "clientId");
        Objects.requireNonNull(cleanup, "cleanup");
        CompletableFuture<Void> tracked = pending.compute(clientId,
                (ignored, previous) -> previous == null
                        ? cleanup : CompletableFuture.allOf(previous, cleanup));
        tracked.whenComplete((ignored, failure) -> pending.remove(clientId, tracked));
        return tracked;
    }

    /** 等待调用时已经发布以及等待期间串接进来的全部清理。 */
    void await(UUID clientId) {
        if (clientId == null) {
            return;
        }
        while (true) {
            CompletableFuture<Void> cleanup = pending.get(clientId);
            if (cleanup == null) {
                return;
            }
            cleanup.join();
            pending.remove(clientId, cleanup);
        }
    }

    boolean hasPending(UUID clientId) {
        return clientId != null && pending.containsKey(clientId);
    }
}
