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
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * 组内玩家完整 NBT 的实时确认上传器。
 * 同一玩家只保留最新状态；与最后确认内容相同则跳过，失败状态留在 pending 中。
 */
public final class PlayerStateUploadService {

    private static final Logger LOGGER =
            LoggerFactory.getLogger("player_to_player/player-upload");

    private final ControlConnection connection;
    private final Executor executor;
    private final Consumer<Runnable> retryScheduler;
    private final Map<UUID, CompoundTag> pending = new LinkedHashMap<>();
    private final Map<UUID, byte[]> confirmed = new java.util.HashMap<>();

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
            pending.put(playerId, state.copy());
            startWorkerLocked();
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
            CompoundTag state;
            synchronized (pending) {
                Map.Entry<UUID, CompoundTag> entry = pending.entrySet().stream()
                        .findFirst().orElse(null);
                if (entry == null) {
                    workerRunning = false;
                    return;
                }
                playerId = entry.getKey();
                state = entry.getValue();
            }
            try {
                byte[] gzip = PlayerStateNbt.encode(state);
                boolean duplicate;
                synchronized (pending) {
                    duplicate = Arrays.equals(confirmed.get(playerId), gzip);
                }
                if (duplicate) {
                    removeIfCurrent(playerId, state);
                    continue;
                }
                JsonObject json = new JsonObject();
                json.addProperty("playerUuid", playerId.toString());
                ControlMessage response = connection.request(ControlMessage.of(
                                MessageType.PLAYER_DATA_UPLOAD, json, gzip))
                        .get(Protocol.REQUEST_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
                if (response.type() != MessageType.PLAYER_DATA
                        || !JsonUtil.getBoolean(response.json(), "written", false)) {
                    throw new IllegalStateException("玩家实时上行未获写盘确认");
                }
                synchronized (pending) {
                    confirmed.put(playerId, gzip.clone());
                    if (pending.get(playerId) == state) {
                        pending.remove(playerId);
                    }
                }
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

    private void removeIfCurrent(UUID playerId, CompoundTag state) {
        synchronized (pending) {
            if (pending.get(playerId) == state) {
                pending.remove(playerId);
            }
        }
    }

    /** 退出构建最终数据时取得最新待传状态；没有 pending 则返回最后确认版本。 */
    public byte[] latestEncoded(UUID playerId) {
        synchronized (pending) {
            CompoundTag state = pending.get(playerId);
            if (state != null) {
                try {
                    return PlayerStateNbt.encode(state);
                } catch (Exception e) {
                    LOGGER.warn("编码退出玩家状态失败: {}", playerId, e);
                }
            }
            byte[] value = confirmed.get(playerId);
            return value != null ? value.clone() : null;
        }
    }

    public void shutdown() {
        shutdown = true;
    }
}
