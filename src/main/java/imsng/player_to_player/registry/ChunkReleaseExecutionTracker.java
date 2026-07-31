package imsng.player_to_player.registry;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * 服务端一次逻辑区块释放的在途请求计数器。
 * <p>
 * 客户端超时后会用相同 {@code releaseId} 重试，而 cached IO 池不保证 A、B 两个任务
 * 按入队顺序执行。客户端收到先完成的 B 的 ACK 后可以确认回收 receipt，但更早入队的 A
 * 仍可能尚未运行；若此时删 receipt，A 会误把同组后来重新取得的 ownership 当成旧 claim
 * 释放。本类要求 handler 在把请求交给 IO 池<b>之前</b>调用 {@link #begin}，并在任务所有
 * 路径结束时调用 {@link #finish}。确认只记录意图，直至该 ID 的所有已接收请求排空后才删
 * receipt，因此不依赖线程池顺序或任意延时猜测。
 */
final class ChunkReleaseExecutionTracker {

    private static final class State {
        private final UUID groupId;
        private int inFlight;
        private boolean confirmed;

        private State(UUID groupId) {
            this.groupId = groupId;
            this.inFlight = 1;
        }
    }

    private final Map<UUID, State> states = new HashMap<>();

    /**
     * 在请求进入异步队列前登记。相同 ID 只能属于同一组；冲突返回 false。
     */
    synchronized boolean begin(UUID releaseId, UUID groupId) {
        State state = states.get(releaseId);
        if (state == null) {
            states.put(releaseId, new State(groupId));
            return true;
        }
        if (!state.groupId.equals(groupId)) {
            return false;
        }
        state.inFlight++;
        return true;
    }

    /**
     * 请求任务结束（成功、拒绝、异常均调用）。最后一个任务离开时执行此前延迟的确认；
     * 尚未收到确认则直接丢弃计数状态，之后 confirmation 可直接删除 receipt。
     */
    synchronized void finish(UUID releaseId, UUID groupId,
                             ChunkRegistry registry) {
        State state = states.get(releaseId);
        if (state == null || !state.groupId.equals(groupId)) {
            return;
        }
        state.inFlight--;
        if (state.inFlight > 0) {
            return;
        }
        states.remove(releaseId);
        if (state.confirmed) {
            registry.confirmReleaseReceipt(releaseId, groupId);
        }
    }

    /**
     * 接受客户端的 receipt confirmation。仍有在途任务时必须先确认成功 receipt 已存在，
     * 再延迟删除；没有在途任务时直接走注册表的幂等回收。
     */
    synchronized boolean confirm(UUID releaseId, UUID groupId,
                                 ChunkRegistry registry) {
        State state = states.get(releaseId);
        if (state == null) {
            return registry.confirmReleaseReceipt(releaseId, groupId);
        }
        if (!state.groupId.equals(groupId)) {
            return false;
        }
        if (!registry.hasReleaseReceiptForGroup(releaseId, groupId)) {
            // 合法客户端只能在收到正释放 ACK 后确认；此时 receipt 必须已经建立。
            return false;
        }
        state.confirmed = true;
        return true;
    }
}
