package imsng.player_to_player.server;

import imsng.player_to_player.group.ExitManifest;
import imsng.player_to_player.registry.ChunkKey;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * 退出事务的线程安全幂等状态机。
 * <p>
 * pending/committed 元数据持久化；本轮连接已经收到的玩家/区块集合仅驻内存。
 * 服务端重启后集合清空，客户端从 EXIT_BEGIN 起重放全部内容即可，避免每收到一个
 * 区块都同步重写事务文件。
 */
public final class ExitTransactionLedger {

    /** EXIT_BEGIN 结果：accepted=false 表示事务 ID 元数据冲突或身份不符。 */
    public record BeginResult(boolean accepted, boolean committed) {
    }

    private static final class RuntimeTransaction {
        private final ExitManifest manifest;
        private final Set<UUID> receivedPlayers = new HashSet<>();
        private final Set<ChunkKey> receivedChunks = new HashSet<>();
        private boolean committed;
        private boolean cleanupComplete;
        private boolean abandoned;

        private RuntimeTransaction(ExitTransactionRecord record) {
            manifest = record.manifest();
            committed = record.committed();
            cleanupComplete = record.cleanupComplete();
            abandoned = record.abandoned();
        }
    }

    private final ExitTransactionPersistence persistence;
    private final Map<UUID, RuntimeTransaction> transactions = new HashMap<>();
    private final Object lock = new Object();

    public ExitTransactionLedger(ExitTransactionPersistence persistence) {
        this.persistence = persistence;
        for (ExitTransactionRecord record : persistence.load().values()) {
            transactions.put(record.manifest().shutdownId(),
                    new RuntimeTransaction(record));
        }
    }

    public BeginResult begin(UUID peerId, ExitManifest manifest) {
        if (peerId == null || manifest == null || !peerId.equals(manifest.clientId())) {
            return new BeginResult(false, false);
        }
        synchronized (lock) {
            RuntimeTransaction existing = transactions.get(manifest.shutdownId());
            if (existing != null) {
                boolean same = existing.manifest.equals(manifest);
                if (same && !existing.committed) {
                    // 上次写入可能在原子替换后、force 完成前抛错。客户端重放
                    // BEGIN 时必须再次持久化，不能只依赖内存中的保护状态。
                    persistThenPublish(new ExitTransactionRecord(
                            existing.manifest, false), () -> { });
                }
                return new BeginResult(same, same && existing.committed);
            }
            for (RuntimeTransaction transaction : transactions.values()) {
                if (!transaction.cleanupComplete
                        && transaction.manifest.clientId().equals(manifest.clientId())) {
                    return new BeginResult(false, false);
                }
            }
            ExitTransactionRecord record = new ExitTransactionRecord(manifest, false);
            RuntimeTransaction created = new RuntimeTransaction(record);
            persistThenPublish(record, () -> transactions.put(
                    manifest.shutdownId(), created));
            return new BeginResult(true, false);
        }
    }

    public boolean markPlayerReceived(UUID peerId, UUID shutdownId, UUID playerId) {
        synchronized (lock) {
            RuntimeTransaction transaction = authorizedPending(peerId, shutdownId);
            if (transaction == null
                    || !transaction.manifest.playerIds().contains(playerId)) {
                return false;
            }
            transaction.receivedPlayers.add(playerId);
            return true;
        }
    }

    public boolean markChunkReceived(UUID peerId, UUID shutdownId, ChunkKey chunk) {
        synchronized (lock) {
            RuntimeTransaction transaction = authorizedPending(peerId, shutdownId);
            if (transaction == null || !transaction.manifest.chunks().contains(chunk)) {
                return false;
            }
            transaction.receivedChunks.add(chunk);
            return true;
        }
    }

    /**
     * 数据齐全时把事务标为 committed 并先落盘。重复提交已经 committed 的事务
     * 同样返回 true，用于最终 ACK 丢失后的重连恢复。
     */
    public boolean commit(UUID peerId, UUID shutdownId) {
        synchronized (lock) {
            RuntimeTransaction transaction = authorized(peerId, shutdownId);
            if (transaction == null) {
                return false;
            }
            if (transaction.committed) {
                return true;
            }
            if (!transaction.receivedPlayers.containsAll(transaction.manifest.playerIds())
                    || !transaction.receivedChunks.containsAll(transaction.manifest.chunks())) {
                return false;
            }
            ExitTransactionRecord committed =
                    new ExitTransactionRecord(transaction.manifest, true,
                            false, false);
            persistThenPublish(committed, () -> transaction.committed = true);
            return true;
        }
    }

    /** 强杀后的正常重进回退：以最后实时确认数据结束 pending，不伪造最终数据。 */
    public boolean abandon(UUID peerId, UUID shutdownId) {
        synchronized (lock) {
            RuntimeTransaction transaction = authorized(peerId, shutdownId);
            if (transaction == null) {
                return false;
            }
            if (transaction.committed) {
                return transaction.abandoned;
            }
            ExitTransactionRecord abandoned = new ExitTransactionRecord(
                    transaction.manifest, true, false, true);
            persistThenPublish(abandoned, () -> {
                transaction.committed = true;
                transaction.abandoned = true;
            });
            return true;
        }
    }

    /** 清理区块注册表与组表成功后再耐久标记，防旧事务在后续重启重复清理新会话。 */
    public boolean markCleanupComplete(UUID peerId, UUID shutdownId) {
        synchronized (lock) {
            RuntimeTransaction transaction = authorized(peerId, shutdownId);
            if (transaction == null || !transaction.committed) {
                return false;
            }
            if (transaction.cleanupComplete) {
                return true;
            }
            ExitTransactionRecord cleaned = new ExitTransactionRecord(
                    transaction.manifest, true, true, transaction.abandoned);
            persistThenPublish(cleaned,
                    () -> transaction.cleanupComplete = true);
            return true;
        }
    }

    public boolean isReadyToCommit(UUID peerId, UUID shutdownId) {
        synchronized (lock) {
            RuntimeTransaction transaction = authorizedPending(peerId, shutdownId);
            return transaction != null
                    && transaction.receivedPlayers.containsAll(
                    transaction.manifest.playerIds())
                    && transaction.receivedChunks.containsAll(
                    transaction.manifest.chunks());
        }
    }

    public boolean isCommitted(UUID peerId, UUID shutdownId) {
        synchronized (lock) {
            RuntimeTransaction transaction = authorized(peerId, shutdownId);
            return transaction != null && transaction.committed;
        }
    }

    public boolean isAbandoned(UUID peerId, UUID shutdownId) {
        synchronized (lock) {
            RuntimeTransaction transaction = authorized(peerId, shutdownId);
            return transaction != null && transaction.abandoned;
        }
    }

    public boolean isCleanupComplete(UUID peerId, UUID shutdownId) {
        synchronized (lock) {
            RuntimeTransaction transaction = authorized(peerId, shutdownId);
            return transaction != null && transaction.cleanupComplete;
        }
    }

    /** pending 事务保护其主客户端，断线清理不得释放组占用。 */
    public boolean isProtected(UUID clientId) {
        if (clientId == null) {
            return false;
        }
        synchronized (lock) {
            for (RuntimeTransaction transaction : transactions.values()) {
                if (!transaction.cleanupComplete
                        && clientId.equals(transaction.manifest.clientId())) {
                    return true;
                }
            }
            return false;
        }
    }

    public ExitManifest manifest(UUID peerId, UUID shutdownId) {
        synchronized (lock) {
            RuntimeTransaction transaction = authorized(peerId, shutdownId);
            return transaction != null ? transaction.manifest : null;
        }
    }

    public Set<ExitManifest> pendingManifests() {
        synchronized (lock) {
            Set<ExitManifest> result = new HashSet<>();
            for (RuntimeTransaction transaction : transactions.values()) {
                if (!transaction.committed) {
                    result.add(transaction.manifest);
                }
            }
            return Set.copyOf(result);
        }
    }

    public Set<ExitManifest> pendingManifestsForClient(UUID clientId) {
        synchronized (lock) {
            Set<ExitManifest> result = new HashSet<>();
            for (RuntimeTransaction transaction : transactions.values()) {
                if (!transaction.committed
                        && transaction.manifest.clientId().equals(clientId)) {
                    result.add(transaction.manifest);
                }
            }
            return Set.copyOf(result);
        }
    }

    public Set<ExitManifest> unfinishedManifestsForClient(UUID clientId) {
        synchronized (lock) {
            Set<ExitManifest> result = new HashSet<>();
            for (RuntimeTransaction transaction : transactions.values()) {
                if (!transaction.cleanupComplete
                        && transaction.manifest.clientId().equals(clientId)) {
                    result.add(transaction.manifest);
                }
            }
            return Set.copyOf(result);
        }
    }

    public Set<ExitManifest> committedManifestsNeedingCleanup() {
        synchronized (lock) {
            Set<ExitManifest> result = new HashSet<>();
            for (RuntimeTransaction transaction : transactions.values()) {
                if (transaction.committed && !transaction.cleanupComplete) {
                    result.add(transaction.manifest);
                }
            }
            return Set.copyOf(result);
        }
    }

    private RuntimeTransaction authorizedPending(UUID peerId, UUID shutdownId) {
        RuntimeTransaction transaction = authorized(peerId, shutdownId);
        return transaction != null && !transaction.committed ? transaction : null;
    }

    /**
     * 成功返回前先完成持久化，再发布对应内存状态。原子替换已经发生、随后 force
     * 抛错时，目标文件可能已经可见；此时回读到完全相同的记录就保守发布内存状态，
     * 但仍抛出原异常，确保本轮协议不会错误返回耐久 ACK，客户端会继续重试。
     */
    private void persistThenPublish(ExitTransactionRecord record,
                                    Runnable publish) {
        try {
            persistence.save(record);
        } catch (RuntimeException saveFailure) {
            try {
                ExitTransactionRecord visible = persistence.load().get(
                        record.manifest().shutdownId());
                if (record.equals(visible)) {
                    publish.run();
                }
            } catch (RuntimeException loadFailure) {
                saveFailure.addSuppressed(loadFailure);
            }
            throw saveFailure;
        }
        publish.run();
    }

    private RuntimeTransaction authorized(UUID peerId, UUID shutdownId) {
        RuntimeTransaction transaction = transactions.get(shutdownId);
        return transaction != null && transaction.manifest.clientId().equals(peerId)
                ? transaction : null;
    }
}
