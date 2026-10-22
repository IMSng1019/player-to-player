package imsng.player_to_player.server;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import imsng.player_to_player.netproto.ControlConnection;
import imsng.player_to_player.netproto.ControlMessage;
import imsng.player_to_player.netproto.MessageType;
import imsng.player_to_player.util.ThreadPools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * 权威归属变更提示的合并广播器。连续卸载数百个区块时，每 100ms 最多广播一帧，
 * 帧内按旧阻塞组去重；客户端只据此重新 claim，不能把提示当作所有权授予或耐久 ACK。
 * 广播目标在发送时重新读取，避免迁移期间向已经退位的旧主连接固定发送通知。
 */
final class OwnershipChangeBroadcaster implements AutoCloseable {
    private static final Logger LOGGER = LoggerFactory.getLogger("player_to_player/registry");

    private final Supplier<? extends Collection<ControlConnection>> recipients;
    private final Consumer<Runnable> scheduler;
    private final Set<UUID> pending = new HashSet<>();
    private boolean scheduled;
    private boolean closed;

    OwnershipChangeBroadcaster(GroupTable groups) {
        this(() -> groups.primariesSnapshot().values().stream().distinct()
                        .map(HelloHandler::connectionOf).filter(Objects::nonNull).toList(),
                task -> ThreadPools.scheduler().schedule(task, 100, TimeUnit.MILLISECONDS));
    }

    /** 测试只替换时钟和在线会话来源，真实发送与批次去重逻辑完全共用。 */
    OwnershipChangeBroadcaster(Supplier<? extends Collection<ControlConnection>> recipients,
                               Consumer<Runnable> scheduler) {
        this.recipients = recipients;
        this.scheduler = scheduler;
    }

    void changed(UUID previousGroup) {
        synchronized (this) {
            if (closed || previousGroup == null) {
                return;
            }
            pending.add(previousGroup);
            if (scheduled) {
                return;
            }
            scheduled = true;
        }
        try {
            scheduler.accept(this::flush);
        } catch (RuntimeException failure) {
            synchronized (this) {
                scheduled = false;
            }
            // 提示丢失可由客户端10秒定时重试恢复，不允许影响已经成功的耐久释放。
            LOGGER.debug("归属变更通知无法调度，保留定时申请兜底", failure);
        }
    }

    private void flush() {
        Set<UUID> changed;
        synchronized (this) {
            if (closed) {
                return;
            }
            changed = Set.copyOf(pending);
            pending.clear();
            scheduled = false;
        }
        if (changed.isEmpty()) {
            return;
        }
        JsonArray groups = new JsonArray();
        changed.forEach(group -> groups.add(group.toString()));
        JsonObject json = new JsonObject();
        json.add("changedGroups", groups);
        for (ControlConnection connection : recipients.get()) {
            synchronized (this) {
                if (closed) {
                    return;
                }
            }
            try {
                if (connection != null && connection.isOpen()) {
                    connection.send(ControlMessage.of(MessageType.CHUNK_OWNERSHIP_CHANGED, json));
                }
            } catch (RuntimeException failure) {
                LOGGER.debug("向主客户端发送归属变更提示失败，其他主继续接收", failure);
            }
        }
    }

    @Override
    public synchronized void close() {
        closed = true;
        pending.clear();
    }
}
