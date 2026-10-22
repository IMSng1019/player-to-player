package imsng.player_to_player.group;

import com.google.gson.JsonObject;
import imsng.player_to_player.netproto.ControlConnection;
import imsng.player_to_player.netproto.ControlMessage;
import imsng.player_to_player.netproto.MessageType;
import imsng.player_to_player.netproto.Protocol;
import imsng.player_to_player.util.JsonUtil;
import net.minecraft.nbt.CompoundTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * 组内玩家完整 NBT 的实时确认上传器。
 * 同一玩家只保留最新状态；与最后确认内容相同则跳过，失败状态留在 pending 中。
 */
public final class PlayerStateUploadService {

    private static final Logger LOGGER =
            LoggerFactory.getLogger("player_to_player/player-upload");

    /**
     * 协议v7的上行序号跨集成服实例延续。同一控制连接可能经历多次主→副→主，
     * 若每次新建上行器从1编号，服务端会把新宿主合法更新视为旧帧。进程重启会建立
     * 新控制连接，服务端按连接对象隔离序号空间，因此不需要把该计数写入磁盘。
     */
    private static final AtomicLong UPLOAD_SEQUENCE = new AtomicLong();

    private final ControlConnection connection;
    private final Executor executor;
    private final Consumer<Runnable> retryScheduler;
    private record QueuedState(CompoundTag tag, long version) { }
    private record Confirmation(long version, CompletableFuture<Void> future) { }
    private final Map<UUID, QueuedState> pending = new LinkedHashMap<>();
    private final Map<UUID, byte[]> confirmed = new java.util.HashMap<>();
    /**
     * 已发出更新但尚未取得可信ACK的玩家。旧confirmed仍可供退出快照兜底，但不能再
     * 用于去重：A已确认→B超时→又回到A时，必须发送A的新序号屏障，阻止迟到B覆盖。
     */
    private final Set<UUID> uncertainWrites = new HashSet<>();
    private final Map<UUID, List<Confirmation>> confirmations = new java.util.HashMap<>();

    private boolean workerRunning;
    private volatile boolean shutdown;

    public PlayerStateUploadService(ControlConnection connection, Executor executor,
                                    Consumer<Runnable> retryScheduler) {
        this.connection = connection;
        this.executor = executor;
        this.retryScheduler = retryScheduler;
    }

    public void offer(UUID playerId, CompoundTag state) {
        if (shutdown || playerId == null || state == null) {
            return;
        }
        synchronized (pending) {
            if (shutdown) return;
            pending.put(playerId, new QueuedState(state.copy(), UPLOAD_SEQUENCE.incrementAndGet()));
            startWorkerLocked();
        }
    }

    /**
     * 分离最终玩家状态走与实时上行相同的串行 worker。仅在本快照（或其后的更新）
     * 收到 durable ACK 后完成，防止“直接另发最终包”被较早的实时包反向覆盖。
     */
    public CompletableFuture<Void> offerConfirmed(UUID playerId, CompoundTag state) {
        if (playerId == null || state == null) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("玩家最终快照缺失"));
        }
        synchronized (pending) {
            if (shutdown) {
                return CompletableFuture.failedFuture(new IllegalStateException("玩家上行器已停止"));
            }
            long version = UPLOAD_SEQUENCE.incrementAndGet();
            CompletableFuture<Void> future = new CompletableFuture<>();
            confirmations.computeIfAbsent(playerId, ignored -> new ArrayList<>())
                    .add(new Confirmation(version, future));
            pending.put(playerId, new QueuedState(state.copy(), version));
            startWorkerLocked();
            return future;
        }
    }

    private void startWorkerLocked() {
        if (!workerRunning && !pending.isEmpty() && !shutdown) {
            workerRunning = true;
            executor.execute(this::drainLoop);
        }
    }

    private void drainLoop() {
        while (!shutdown) {
            UUID playerId;
            QueuedState state;
            synchronized (pending) {
                Map.Entry<UUID, QueuedState> entry = pending.entrySet().stream()
                        .findFirst().orElse(null);
                if (entry == null) {
                    workerRunning = false;
                    return;
                }
                playerId = entry.getKey();
                state = entry.getValue();
            }
            try {
                byte[] gzip = PlayerStateNbt.encode(state.tag());
                boolean duplicate;
                CompletableFuture<ControlMessage> responseFuture = null;
                synchronized (pending) {
                    // 编码在锁外；真正发包前与shutdown使用同一锁，避免权限移交后再启动旧上行。
                    if (shutdown) return;
                    if (pending.get(playerId) != state) continue;
                    duplicate = !uncertainWrites.contains(playerId)
                            && Arrays.equals(confirmed.get(playerId), gzip);
                    if (!duplicate) {
                        JsonObject json = new JsonObject();
                        json.addProperty("playerUuid", playerId.toString());
                        // 重试复用QueuedState，因此序号和内容稳定；新快照才取新序号。
                        json.addProperty("uploadSequence", state.version());
                        // 在真实request前标记：即使request抛错/超时，服务端也可能已经排队。
                        // 只有后续完整ACK才能恢复去重，普通实时offer与最终offerConfirmed共用。
                        uncertainWrites.add(playerId);
                        responseFuture = connection.request(ControlMessage.of(
                                MessageType.PLAYER_DATA_UPLOAD, json, gzip));
                    }
                }
                if (duplicate) {
                    confirm(playerId, state, gzip);
                    continue;
                }
                ControlMessage response = responseFuture.get(Protocol.REQUEST_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
                if (response.type() != MessageType.PLAYER_DATA
                        || !JsonUtil.getBoolean(response.json(), "written", false)) {
                    throw new IllegalStateException("玩家实时上行未获写盘确认");
                }
                confirm(playerId, state, gzip);
            } catch (Exception e) {
                LOGGER.warn("玩家实时状态上行失败，保留最新状态等待重试: {}",
                        playerId, e);
                synchronized (pending) {
                    workerRunning = false;
                }
                retryScheduler.accept(() -> {
                    synchronized (pending) {
                        startWorkerLocked();
                    }
                });
                return;
            }
        }
    }

    /** ACK只确认不晚于该快照的等待者，较新的最终快照仍须等待自己的写盘。 */
    private void confirm(UUID playerId, QueuedState state, byte[] gzip) {
        List<CompletableFuture<Void>> completed = new ArrayList<>();
        synchronized (pending) {
            if (shutdown) return;
            confirmed.put(playerId, gzip.clone());
            uncertainWrites.remove(playerId);
            if (pending.get(playerId) == state) {
                pending.remove(playerId);
            }
            List<Confirmation> waiting = confirmations.get(playerId);
            if (waiting != null) {
                waiting.removeIf(confirmation -> {
                    if (confirmation.version() > state.version()) return false;
                    completed.add(confirmation.future());
                    return true;
                });
                if (waiting.isEmpty()) confirmations.remove(playerId);
            }
        }
        completed.forEach(future -> future.complete(null));
    }

    /** 退出构建最终数据时取得最新待传状态；没有 pending 则返回最后确认版本。 */
    public byte[] latestEncoded(UUID playerId) {
        synchronized (pending) {
            QueuedState state = pending.get(playerId);
            if (state != null) {
                try {
                    return PlayerStateNbt.encode(state.tag());
                } catch (Exception e) {
                    LOGGER.warn("编码退出玩家状态失败: {}", playerId, e);
                }
            }
            byte[] value = confirmed.get(playerId);
            return value != null ? value.clone() : null;
        }
    }

    public void shutdown() {
        List<CompletableFuture<Void>> abandoned = new ArrayList<>();
        synchronized (pending) {
            shutdown = true;
            confirmations.values().forEach(list -> list.forEach(c -> abandoned.add(c.future())));
            confirmations.clear();
            pending.clear();
        }
        abandoned.forEach(future -> future.completeExceptionally(
                new IllegalStateException("玩家上行器在最终状态确认前停止")));
    }
}
