package imsng.player_to_player.server;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import imsng.player_to_player.group.ChunkDelta;
import imsng.player_to_player.group.ChunkDeltaBatch;
import imsng.player_to_player.group.ExitManifest;
import imsng.player_to_player.netproto.ControlConnection;
import imsng.player_to_player.netproto.ControlMessage;
import imsng.player_to_player.netproto.HandlerRegistry;
import imsng.player_to_player.netproto.MessageType;
import imsng.player_to_player.registry.ChunkKey;
import imsng.player_to_player.util.JsonUtil;
import imsng.player_to_player.util.Sha256;
import imsng.player_to_player.util.ThreadPools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.BooleanSupplier;

/**
 * 主客户端耐久退出协议处理器。
 * <p>
 * Minecraft 存储操作通过小接口注入，使协议状态机可用同步假实现测试；生产环境把执行器
 * 指向 IO 池。所有成功应答都发生在对应写入完成之后，不能把“已进入线程池”误当成落盘。
 */
public final class ExitTransactionHandlers {

    private static final Logger LOGGER =
            LoggerFactory.getLogger("player_to_player/exit-handler");

    @FunctionalInterface
    public interface BeginAuthorizer {
        /** 返回服务端冻结后的权威清单；拒绝时返回 null。 */
        ExitManifest authorize(UUID peerId, ExitManifest requested);
    }

    @FunctionalInterface
    public interface PlayerSink {
        boolean write(UUID playerId, byte[] gzipNbt);
    }

    @FunctionalInterface
    public interface PendingProtection {
        void protect(ExitManifest manifest);

        default void cancelUnpersisted(ExitManifest manifest) {
        }
    }

    public interface ChunkSink {
        default boolean validate(UUID groupId, ChunkKey key, byte[] gzipNbt,
                                 boolean preserveExisting) {
            return true;
        }

        boolean write(UUID groupId, ChunkKey key, byte[] gzipNbt,
                      boolean preserveExisting);

        void flush();
    }

    public interface PayloadStore {
        boolean stagePlayer(ExitManifest manifest, UUID playerId, byte[] gzipNbt);

        boolean stageChunk(ExitManifest manifest, ChunkKey key, byte[] gzipNbt,
                           boolean preserveExisting);

        /**
         * v5 退出尾部批次的默认兼容实现。生产 {@link ExitPayloadStore} 会覆盖此方法，
         * 把整批二进制写成一个可重建文件，避免退化为逐区块临时文件写入。
         */
        default boolean stageChunkBatch(ExitManifest manifest,
                                        ChunkDeltaBatch batch) {
            for (ChunkDelta delta : batch.deltas()) {
                if (!stageChunk(manifest, delta.key(), delta.payload(), false)) {
                    return false;
                }
            }
            return true;
        }

        /**
         * 从可重建暂存区组装最终复合 WAL 载荷。只有启用 v5 世界 WAL 的生产路径调用；
         * 旧测试/兼容实现可继续只实现 stage/apply。
         */
        default ExitCommitPayload prepareCommit(
                ExitManifest manifest,
                ExitTransactionLedger.ChunkDisposition disposition) {
            throw new UnsupportedOperationException("未配置退出复合提交组装器");
        }

        void apply(ExitManifest manifest,
                   ExitTransactionLedger.ChunkDisposition disposition);

        void discard(ExitManifest manifest);
    }

    @FunctionalInterface
    public interface SessionGuard {
        boolean run(ControlConnection connection, BooleanSupplier action);
    }

    @FunctionalInterface
    public interface CommitCleanup {
        void cleanup(ExitManifest manifest);
    }

    private final ExitTransactionLedger ledger;
    private final BeginAuthorizer authorizer;
    private final PendingProtection pendingProtection;
    private final PayloadStore payloadStore;
    private final Runnable flushRealtime;
    private final SessionGuard sessionGuard;
    private final CommitCleanup cleanup;
    private final Executor executor;
    /** 非空表示启用 v5 复合世界 WAL，最终 ACK 不再等待正式物化。 */
    private final DurableChunkJournal durableJournal;
    private final ConcurrentHashMap<UUID, CompletableFuture<Void>> beginBarriers =
            new ConcurrentHashMap<>();
    private final ConcurrentHashMap<UUID, Boolean> cleanupTasks =
            new ConcurrentHashMap<>();
    /**
     * 同一退出事务的正式数据应用与 ownership/组表清理必须严格串行。
     *
     * <p>后台 ACK 后清理、服务端启动恢复以及 ROLE_REQUEST 重进恢复都可能同时
     * 到达这里。只依赖 ledger 的分步状态检查会形成 check-then-act 竞态，使旧退出
     * 快照重复覆盖玩家文件，并让较慢的旧清理误删已经重建的新组。</p>
     */
    private final ConcurrentHashMap<UUID, CompletableFuture<Void>> cleanupBarriers =
            new ConcurrentHashMap<>();
    private final ExitPayloadAssembler payloadAssembler = new ExitPayloadAssembler();

    public ExitTransactionHandlers(ExitTransactionLedger ledger,
                                   BeginAuthorizer authorizer,
                                   PendingProtection pendingProtection,
                                   PlayerSink playerSink,
                                   ChunkSink chunkSink,
                                   CommitCleanup cleanup,
                                   Executor executor) {
        this(ledger, authorizer, pendingProtection,
                new VolatilePayloadStore(playerSink, chunkSink),
                chunkSink::flush, cleanup, executor,
                (connection, action) -> action.getAsBoolean());
    }

    public ExitTransactionHandlers(ExitTransactionLedger ledger,
                                   BeginAuthorizer authorizer,
                                   PendingProtection pendingProtection,
                                   PayloadStore payloadStore,
                                   Runnable flushRealtime,
                                   CommitCleanup cleanup,
                                   Executor executor) {
        this(ledger, authorizer, pendingProtection, payloadStore,
                flushRealtime, cleanup, executor,
                (connection, action) -> action.getAsBoolean(), null);
    }

    public ExitTransactionHandlers(ExitTransactionLedger ledger,
                                   BeginAuthorizer authorizer,
                                   PendingProtection pendingProtection,
                                   PayloadStore payloadStore,
                                   Runnable flushRealtime,
                                   CommitCleanup cleanup,
                                   Executor executor,
                                   SessionGuard sessionGuard) {
        this.ledger = ledger;
        this.authorizer = authorizer;
        this.pendingProtection = pendingProtection;
        this.payloadStore = payloadStore;
        this.flushRealtime = flushRealtime;
        this.cleanup = cleanup;
        this.executor = executor;
        this.sessionGuard = sessionGuard;
        this.durableJournal = null;
    }

    public ExitTransactionHandlers(ExitTransactionLedger ledger,
                                   BeginAuthorizer authorizer,
                                   PendingProtection pendingProtection,
                                   PayloadStore payloadStore,
                                   Runnable flushRealtime,
                                   CommitCleanup cleanup,
                                   Executor executor,
                                   SessionGuard sessionGuard,
                                   DurableChunkJournal durableJournal) {
        this.ledger = ledger;
        this.authorizer = authorizer;
        this.pendingProtection = pendingProtection;
        this.payloadStore = payloadStore;
        this.flushRealtime = flushRealtime;
        this.cleanup = cleanup;
        this.executor = executor;
        this.sessionGuard = sessionGuard;
        this.durableJournal = durableJournal;
    }

    public void register(HandlerRegistry registry) {
        registry.on(MessageType.EXIT_BEGIN, this::handleBegin);
        registry.on(MessageType.EXIT_PLAYER_DATA, this::handlePlayer);
        registry.on(MessageType.EXIT_CHUNK_DATA, this::handleChunk);
        registry.on(MessageType.EXIT_CHUNK_BATCH, this::handleChunkBatch);
        registry.on(MessageType.EXIT_COMMIT, this::handleCommit);
        registry.on(MessageType.EXIT_STATUS, this::handleStatus);
    }

    public boolean isProtectedAfterBegins(UUID clientId) {
        awaitPendingBegins(clientId);
        return isProtectedNow(clientId);
    }

    /**
     * 等待当前已经发布的 BEGIN 屏障全部结束。调用方不得在 HELLO 会话锁内等待，
     * 因为 BEGIN 任务本身需要进入同一把锁校验连接是否仍为当前会话。
     */
    public void awaitPendingBegins(UUID clientId) {
        if (clientId == null) {
            return;
        }
        while (true) {
            CompletableFuture<Void> barrier = beginBarriers.get(clientId);
            if (barrier == null) {
                return;
            }
            barrier.handle((ignored, failure) -> null).join();
            // BEGIN 任务先完成屏障再从映射摘除；等待方可协助条件删除，随后继续
            // 检查是否有更新的屏障替换进来。
            beginBarriers.remove(clientId, barrier);
        }
    }

    /** 只读取当前耐久事务保护状态；不会等待 BEGIN 屏障。 */
    public boolean isProtectedNow(UUID clientId) {
        return ledger.isProtected(clientId);
    }

    /** 当前连接、BEGIN 屏障和 ledger 未保护检查共同包住一次实时权威写。 */
    public boolean writeRealtimeIfAllowed(ControlConnection connection,
                                          BooleanSupplier writer) {
        UUID clientId = connection != null ? connection.peerId() : null;
        if (clientId == null || writer == null) {
            return false;
        }
        CompletableFuture<Void> barrier = beginBarriers.get(clientId);
        if (barrier != null) {
            barrier.handle((ignored, failure) -> null).join();
        }
        return sessionGuard.run(connection, () -> {
            // 生产 SessionGuard 对同一客户端串行 HELLO、BEGIN 与本动作，因此这里
            // 只需在耗时 writer 前读取保护状态，不必占用 ledger 全局锁做磁盘 IO。
            if (ledger.isProtected(clientId)) {
                return false;
            }
            return writer.getAsBoolean();
        });
    }

    /** 服务端启动时先恢复 pending 冻结，再补做 committed 但尚未完成的清理。 */
    public void recoverDurableState() {
        for (ExitManifest manifest : ledger.pendingManifests()) {
            pendingProtection.protect(manifest);
        }
        for (ExitManifest manifest : ledger.committedManifestsNeedingCleanup()) {
            ensureCleanupComplete(manifest);
        }
        for (ExitManifest manifest : ledger.cleanupCompleteManifests()) {
            discardCompletedBestEffort(manifest);
        }
    }

    /** 新进程正常申请角色代表旧退出进程已被强杀：保留最后实时数据并释放旧占用。 */
    public int abandonPendingForRejoin(UUID clientId) {
        awaitPendingBegins(clientId);
        return abandonPendingForRejoinNow(clientId);
    }

    /**
     * 不等待 BEGIN 屏障的重进恢复阶段。调用方必须先在 HELLO 会话锁外调用
     * {@link #awaitPendingBegins(UUID)}，再在当前会话锁内调用本方法。
     */
    public int abandonPendingForRejoinNow(UUID clientId) {
        int abandoned = 0;
        for (ExitManifest manifest : ledger.unfinishedManifestsForClient(clientId)) {
            if (!ledger.isCommitted(clientId, manifest.shutdownId())) {
                flushRealtime.run();
                if (!ledger.abandon(clientId, manifest.shutdownId())) {
                    throw new IllegalStateException("无法终结强杀遗留退出事务");
                }
                abandoned++;
            }
            ensureCleanupComplete(manifest);
        }
        return abandoned;
    }

    private void handleBegin(ControlConnection connection, ControlMessage message) {
        UUID peerId = connection.peerId();
        CompletableFuture<Void> barrier = new CompletableFuture<>();
        java.util.concurrent.atomic.AtomicReference<CompletableFuture<Void>> previousRef =
                new java.util.concurrent.atomic.AtomicReference<>();
        if (peerId != null) {
            beginBarriers.compute(peerId, (ignored, previous) -> {
                previousRef.set(previous);
                return barrier;
            });
        }
        Runnable task = () -> {
            CompletableFuture<Void> previous = previousRef.get();
            if (previous != null) {
                previous.handle((ignored, failure) -> null).join();
            }
            JsonObject out = new JsonObject();
            java.util.concurrent.atomic.AtomicReference<ExitManifest> cleanupAfterReply =
                    new java.util.concurrent.atomic.AtomicReference<>();
            boolean current = sessionGuard.run(connection, () -> {
                try {
                UUID currentPeer = connection.peerId();
                ExitManifest requested = ExitManifest.fromJson(message.json());
                ExitManifest effective = ledger.manifest(
                        currentPeer, requested.shutdownId());
                boolean newlyAuthorized = effective == null;
                if (effective == null) {
                    effective = authorizer.authorize(currentPeer, requested);
                }
                if (effective == null) {
                    out.addProperty("accepted", false);
                    out.addProperty("committed", false);
                } else {
                    ExitTransactionLedger.BeginResult result;
                    try {
                        result = ledger.begin(currentPeer, effective);
                    } catch (RuntimeException e) {
                        if (newlyAuthorized && !ledger.isProtected(currentPeer)) {
                            pendingProtection.cancelUnpersisted(effective);
                        }
                        throw e;
                    }
                    if (result.accepted() && result.committed()) {
                        if (durableJournal == null) {
                            ensureCleanupComplete(effective);
                        } else {
                            cleanupAfterReply.set(effective);
                        }
                    } else if (result.accepted()) {
                        // ledger pending 已耐久后再把注册表校准并持久化；失败不回 ACK，
                        // 客户端重放 BEGIN 时会重复保护，不能在半保护状态下断开。
                        pendingProtection.protect(effective);
                    } else if (newlyAuthorized && !ledger.isProtected(currentPeer)) {
                        pendingProtection.cancelUnpersisted(effective);
                    }
                    out.addProperty("accepted", result.accepted());
                    out.addProperty("committed", result.committed());
                    if (result.accepted()) {
                        out.add("manifest", effective.toJson());
                    }
                }
                } catch (Exception e) {
                    LOGGER.warn("EXIT_BEGIN 处理失败", e);
                    out.addProperty("accepted", false);
                    out.addProperty("committed", false);
                }
                return true;
            });
            if (!current) {
                out.addProperty("accepted", false);
                out.addProperty("committed", false);
            }
            try {
                connection.send(message.reply(MessageType.EXIT_BEGIN, out, null));
                ExitManifest cleanupManifest = cleanupAfterReply.get();
                if (cleanupManifest != null) {
                    scheduleCleanup(cleanupManifest);
                }
            } finally {
                barrier.complete(null);
                if (peerId != null) {
                    beginBarriers.remove(peerId, barrier);
                }
            }
        };
        try {
            executor.execute(task);
        } catch (RuntimeException e) {
            barrier.completeExceptionally(e);
            if (peerId != null) {
                beginBarriers.remove(peerId, barrier);
            }
            throw e;
        }
    }

    private void handlePlayer(ControlConnection connection, ControlMessage message) {
        executor.execute(() -> {
            boolean[] result = new boolean[2];
            sessionGuard.run(connection, () -> {
                try {
                UUID peerId = connection.peerId();
                UUID shutdownId = parseUuid(message.json(), "shutdownId");
                UUID playerId = parseUuid(message.json(), "playerUuid");
                ExitManifest manifest = ledger.manifest(peerId, shutdownId);
                if (manifest != null && manifest.playerIds().contains(playerId)) {
                    ExitPayloadAssembler.Result assembled = assemble(
                            new ExitPayloadAssembler.Key(
                                    shutdownId, "player", playerId.toString()),
                            message);
                    result[0] = assembled.accepted();
                    if (assembled.complete()) {
                        byte[] payload = assembled.payload();
                        result[1] = ledger.receivePlayer(
                                peerId, shutdownId, playerId,
                                () -> payloadStore.stagePlayer(
                                        manifest, playerId, payload));
                        result[0] = result[1];
                    }
                }
                } catch (Exception e) {
                    LOGGER.warn("EXIT_PLAYER_DATA 处理失败", e);
                }
                return true;
            });
            JsonObject out = new JsonObject();
            out.addProperty("accepted", result[0]);
            out.addProperty("written", result[1]);
            connection.send(message.reply(MessageType.EXIT_PLAYER_DATA, out, null));
        });
    }

    private void handleChunk(ControlConnection connection, ControlMessage message) {
        executor.execute(() -> {
            boolean[] result = new boolean[2];
            sessionGuard.run(connection, () -> {
                try {
                UUID peerId = connection.peerId();
                UUID shutdownId = parseUuid(message.json(), "shutdownId");
                ChunkKey key = parseChunk(message.json());
                boolean preserve = JsonUtil.getBoolean(
                        message.json(), "preserveExisting", false);
                ExitManifest manifest = ledger.manifest(peerId, shutdownId);
                if (manifest != null && manifest.chunks().contains(key)) {
                    if (preserve) {
                        result[1] = ledger.receiveChunk(
                                peerId, shutdownId, key,
                                () -> payloadStore.stageChunk(
                                        manifest, key, null, true));
                        result[0] = result[1];
                    } else {
                        ExitPayloadAssembler.Result assembled = assemble(
                                new ExitPayloadAssembler.Key(
                                        shutdownId, "chunk", key.asString()),
                                message);
                        result[0] = assembled.accepted();
                        if (assembled.complete()) {
                            byte[] payload = assembled.payload();
                            result[1] = ledger.receiveChunk(
                                    peerId, shutdownId, key,
                                    () -> payloadStore.stageChunk(
                                            manifest, key, payload, false));
                            result[0] = result[1];
                        }
                    }
                }
                } catch (Exception e) {
                    LOGGER.warn("EXIT_CHUNK_DATA 处理失败", e);
                }
                return true;
            });
            JsonObject out = new JsonObject();
            out.addProperty("accepted", result[0]);
            out.addProperty("written", result[1]);
            connection.send(message.reply(MessageType.EXIT_CHUNK_DATA, out, null));
        });
    }

    /**
     * v5 退出尾部批次处理：先完整解码/校验批次，再由 PayloadStore 一次性暂存，
     * 最后原子登记整批区块已收到。重复 batchId 会由存储层幂等接受。
     */
    private void handleChunkBatch(ControlConnection connection, ControlMessage message) {
        executor.execute(() -> {
            boolean accepted = false;
            boolean written = false;
            try {
                UUID peerId = connection.peerId();
                UUID shutdownId = parseUuid(message.json(), "shutdownId");
                ChunkDeltaBatch batch = ChunkDeltaBatch.decode(message.binary());
                String declaredBatch = JsonUtil.getString(message.json(), "batchId", "");
                int declaredSequence = JsonUtil.getInt(message.json(), "sequence", -1);
                String declaredSha256 = JsonUtil.getString(
                        message.json(), "sha256", "");
                if (!batch.batchId().toString().equals(declaredBatch)
                        || batch.sequence() != declaredSequence
                        || !Sha256.hex(message.binary()).equals(declaredSha256)) {
                    throw new IllegalArgumentException("退出批次身份不一致");
                }
                ExitManifest manifest = ledger.manifest(peerId, shutdownId);
                if (manifest == null || batch.deltas().isEmpty()) {
                    throw new IllegalStateException("退出事务不存在或批次为空");
                }
                Set<ChunkKey> keys = new HashSet<>();
                for (ChunkDelta delta : batch.deltas()) {
                    if (!keys.add(delta.key()) || !manifest.chunks().contains(delta.key())) {
                        throw new IllegalArgumentException("退出批次区块不在 ownership 清单");
                    }
                }
                boolean[] stored = new boolean[1];
                boolean current = sessionGuard.run(connection, () -> {
                    stored[0] = ledger.receiveChunks(peerId, shutdownId, keys,
                            () -> payloadStore.stageChunkBatch(manifest, batch));
                    return true;
                });
                written = current && stored[0];
                accepted = written;
            } catch (Exception e) {
                LOGGER.warn("EXIT_CHUNK_BATCH 处理失败", e);
            }
            JsonObject out = new JsonObject();
            out.addProperty("accepted", accepted);
            out.addProperty("written", written);
            connection.send(message.reply(MessageType.EXIT_CHUNK_BATCH, out, null));
        });
    }

    private void handleCommit(ControlConnection connection, ControlMessage message) {
        executor.execute(() -> {
            long started = System.nanoTime();
            boolean[] committed = new boolean[1];
            java.util.concurrent.atomic.AtomicReference<ExitManifest> cleanupAfterReply =
                    new java.util.concurrent.atomic.AtomicReference<>();
            sessionGuard.run(connection, () -> {
                try {
                UUID peerId = connection.peerId();
                UUID shutdownId = parseUuid(message.json(), "shutdownId");
                Set<ChunkKey> writtenChunks = parseWrittenChunks(message.json());
                ExitManifest manifest = ledger.manifest(peerId, shutdownId);
                if (manifest != null) {
                    if (durableJournal == null) {
                        if (ledger.commit(peerId, shutdownId, writtenChunks)) {
                            ensureCleanupComplete(manifest);
                            committed[0] = true;
                        }
                    } else {
                        ExitCommitPayload prepared = null;
                        if (!ledger.isCommitted(peerId, shutdownId)) {
                            if (!ledger.isReadyToCommit(
                                    peerId, shutdownId, writtenChunks)) {
                                return true;
                            }
                            prepared = payloadStore.prepareCommit(manifest,
                                    new ExitTransactionLedger.ChunkDisposition(
                                            writtenChunks, true));
                        }
                        ExitCommitPayload finalPrepared = prepared;
                        if (ledger.commitAfterDurable(
                                peerId, shutdownId, writtenChunks, () -> {
                                    if (finalPrepared == null) {
                                        return true;
                                    }
                                    durableJournal.appendExitCommit(finalPrepared);
                                    return true;
                                })) {
                            // force 成功已经证明最终数据与退出意图可恢复；正式文件、MCA、
                            // 注册表 checkpoint 和组表清理全部移到 ACK 后重试。
                            committed[0] = true;
                            cleanupAfterReply.set(manifest);
                        }
                    }
                }
                } catch (Exception e) {
                    LOGGER.warn("EXIT_COMMIT 处理失败", e);
                }
                return true;
            });
            JsonObject out = new JsonObject();
            out.addProperty("committed", committed[0]);
            if (committed[0]) {
                LOGGER.info("退出事务耐久提交完成: shutdownId={} exit.commit_ms={}",
                        JsonUtil.getString(message.json(), "shutdownId", ""),
                        java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(
                                System.nanoTime() - started));
            }
            connection.send(message.reply(MessageType.EXIT_COMMIT, out, null));
            ExitManifest cleanupManifest = cleanupAfterReply.get();
            if (cleanupManifest != null) {
                scheduleCleanup(cleanupManifest);
            }
        });
    }

    /** 断线重连的只读状态查询；不会改变 pending/committed 状态。 */
    private void handleStatus(ControlConnection connection, ControlMessage message) {
        executor.execute(() -> {
            JsonObject out = new JsonObject();
            ExitManifest cleanupManifest = null;
            try {
                UUID peerId = connection.peerId();
                UUID shutdownId = parseUuid(message.json(), "shutdownId");
                ExitManifest manifest = ledger.manifest(peerId, shutdownId);
                out.addProperty("accepted", manifest != null);
                out.addProperty("committed",
                        manifest != null && ledger.isCommitted(peerId, shutdownId));
                out.addProperty("cleanupComplete",
                        manifest != null && ledger.isCleanupComplete(peerId, shutdownId));
                if (manifest != null) {
                    out.add("manifest", manifest.toJson());
                    if (durableJournal != null
                            && ledger.isCommitted(peerId, shutdownId)
                            && !ledger.isCleanupComplete(peerId, shutdownId)) {
                        cleanupManifest = manifest;
                    }
                }
            } catch (Exception e) {
                out.addProperty("accepted", false);
                out.addProperty("committed", false);
                out.addProperty("cleanupComplete", false);
            }
            connection.send(message.reply(MessageType.EXIT_STATUS, out, null));
            if (cleanupManifest != null) {
                scheduleCleanup(cleanupManifest);
            }
        });
    }

    /** 同一 shutdownId 只允许一个后台物化/清理重试链，避免重复 COMMIT 制造并发写。 */
    private void scheduleCleanup(ExitManifest manifest) {
        UUID shutdownId = manifest.shutdownId();
        if (cleanupTasks.putIfAbsent(shutdownId, Boolean.TRUE) != null) {
            return;
        }
        try {
            executor.execute(() -> runCleanupAttempt(manifest));
        } catch (RuntimeException e) {
            cleanupTasks.remove(shutdownId);
            LOGGER.warn("退出后台清理任务提交失败，将由重连/重启恢复: {}",
                    shutdownId, e);
        }
    }

    private void runCleanupAttempt(ExitManifest manifest) {
        UUID shutdownId = manifest.shutdownId();
        try {
            ensureCleanupComplete(manifest);
            cleanupTasks.remove(shutdownId);
        } catch (RuntimeException e) {
            LOGGER.warn("退出事务后台物化/清理失败，1 秒后继续重试: {}",
                    shutdownId, e);
            try {
                ThreadPools.scheduler().schedule(() -> {
                    try {
                        executor.execute(() -> runCleanupAttempt(manifest));
                    } catch (RuntimeException submitFailure) {
                        cleanupTasks.remove(shutdownId);
                        LOGGER.warn("退出后台清理重试提交失败，将由重连/重启恢复: {}",
                                shutdownId, submitFailure);
                    }
                }, 1, TimeUnit.SECONDS);
            } catch (RuntimeException scheduleFailure) {
                cleanupTasks.remove(shutdownId);
                LOGGER.warn("退出后台清理重试调度失败，将由重连/重启恢复: {}",
                        shutdownId, scheduleFailure);
            }
        }
    }

    private void ensureCleanupComplete(ExitManifest manifest) {
        UUID shutdownId = manifest.shutdownId();
        while (true) {
            CompletableFuture<Void> existing = cleanupBarriers.get(shutdownId);
            if (existing != null) {
                existing.join();
                return;
            }
            CompletableFuture<Void> owned = new CompletableFuture<>();
            if (cleanupBarriers.putIfAbsent(shutdownId, owned) != null) {
                continue;
            }
            try {
                ensureCleanupCompleteOnce(manifest);
                owned.complete(null);
                return;
            } catch (RuntimeException | Error failure) {
                owned.completeExceptionally(failure);
                throw failure;
            } finally {
                cleanupBarriers.remove(shutdownId, owned);
            }
        }
    }

    /** 调用方已经取得 shutdownId 专属屏障。 */
    private void ensureCleanupCompleteOnce(ExitManifest manifest) {
        if (ledger.isCleanupComplete(manifest.clientId(), manifest.shutdownId())) {
            discardCompletedBestEffort(manifest);
            return;
        }
        if (ledger.isAbandoned(manifest.clientId(), manifest.shutdownId())) {
            payloadStore.discard(manifest);
        } else if (!ledger.isDataApplied(
                manifest.clientId(), manifest.shutdownId())) {
            ExitTransactionLedger.ChunkDisposition disposition =
                    ledger.chunkDisposition(
                            manifest.clientId(), manifest.shutdownId());
            if (disposition == null) {
                throw new IllegalStateException("退出事务缺少 committed 区块处置计划");
            }
            payloadStore.apply(manifest, disposition);
            if (!ledger.markDataApplied(
                    manifest.clientId(), manifest.shutdownId())) {
                throw new IllegalStateException("退出正式数据应用状态落盘失败");
            }
        }
        cleanup.cleanup(manifest);
        if (!ledger.markCleanupComplete(
                manifest.clientId(), manifest.shutdownId())) {
            throw new IllegalStateException("退出事务清理状态落盘失败");
        }
        discardCompletedBestEffort(manifest);
    }

    private void discardCompletedBestEffort(ExitManifest manifest) {
        try {
            payloadStore.discard(manifest);
        } catch (RuntimeException e) {
            LOGGER.warn("退出事务已完成，但暂存目录清理失败: {}",
                    manifest.shutdownId(), e);
        }
    }

    private ExitPayloadAssembler.Result assemble(ExitPayloadAssembler.Key key,
                                                  ControlMessage message) {
        JsonObject json = message.json();
        byte[] binary = message.binary();
        int index = JsonUtil.getInt(json, "fragmentIndex", 0);
        int count = JsonUtil.getInt(json, "fragmentCount", 1);
        int total = JsonUtil.getInt(json, "totalBytes", binary.length);
        String sha256 = JsonUtil.getString(
                json, "sha256", Sha256.hex(binary));
        return payloadAssembler.offer(
                key, index, count, total, sha256, binary);
    }

    private static UUID parseUuid(JsonObject json, String name) {
        return UUID.fromString(JsonUtil.getString(json, name, ""));
    }

    private static ChunkKey parseChunk(JsonObject json) {
        String dimension = JsonUtil.getString(json, "dimension", "");
        if (dimension.isBlank() || !json.has("x") || !json.has("z")) {
            throw new IllegalArgumentException("退出区块坐标缺失");
        }
        return new ChunkKey(dimension, json.get("x").getAsInt(),
                json.get("z").getAsInt());
    }

    private static Set<ChunkKey> parseWrittenChunks(JsonObject json) {
        JsonElement element = json.get("writtenChunks");
        if (element == null || !element.isJsonArray()) {
            throw new IllegalArgumentException("EXIT_COMMIT 缺少 writtenChunks 数组");
        }
        Set<ChunkKey> writtenChunks = new HashSet<>();
        for (JsonElement value : element.getAsJsonArray()) {
            ChunkKey key = ChunkKey.parse(value.getAsString());
            if (!writtenChunks.add(key)) {
                throw new IllegalArgumentException(
                        "EXIT_COMMIT writtenChunks 含重复区块: " + key.asString());
            }
        }
        return Set.copyOf(writtenChunks);
    }

    /** 仅供纯状态机单测使用；生产环境必须注入 {@link ExitPayloadStore}。 */
    private static final class VolatilePayloadStore implements PayloadStore {
        private record ChunkPayload(byte[] data, boolean preserve) {
        }

        private final PlayerSink players;
        private final ChunkSink chunks;
        private final Map<UUID, Map<UUID, byte[]>> stagedPlayers = new HashMap<>();
        private final Map<UUID, Map<ChunkKey, ChunkPayload>> stagedChunks = new HashMap<>();

        private VolatilePayloadStore(PlayerSink players, ChunkSink chunks) {
            this.players = players;
            this.chunks = chunks;
        }

        @Override
        public synchronized boolean stagePlayer(ExitManifest manifest, UUID playerId,
                                                byte[] gzipNbt) {
            stagedPlayers.computeIfAbsent(manifest.shutdownId(), ignored -> new HashMap<>())
                    .put(playerId, gzipNbt.clone());
            return true;
        }

        @Override
        public synchronized boolean stageChunk(ExitManifest manifest, ChunkKey key,
                                               byte[] gzipNbt,
                                               boolean preserveExisting) {
            if (preserveExisting) {
                return false;
            }
            if (!chunks.validate(manifest.groupId(), key, gzipNbt,
                    preserveExisting)) {
                return false;
            }
            stagedChunks.computeIfAbsent(manifest.shutdownId(), ignored -> new HashMap<>())
                    .put(key, new ChunkPayload(
                            gzipNbt != null ? gzipNbt.clone() : null,
                            preserveExisting));
            return true;
        }

        @Override
        public synchronized boolean stageChunkBatch(ExitManifest manifest,
                                                     ChunkDeltaBatch batch) {
            for (ChunkDelta delta : batch.deltas()) {
                if (!stageChunk(manifest, delta.key(), delta.payload(), false)) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public synchronized void apply(
                ExitManifest manifest,
                ExitTransactionLedger.ChunkDisposition disposition) {
            Map<UUID, byte[]> playerData = stagedPlayers.getOrDefault(
                    manifest.shutdownId(), Map.of());
            for (UUID playerId : manifest.playerIds()) {
                byte[] data = playerData.get(playerId);
                if (data == null || !players.write(playerId, data)) {
                    throw new IllegalStateException("测试暂存玩家数据缺失或写入失败");
                }
            }
            Map<ChunkKey, ChunkPayload> chunkData = stagedChunks.getOrDefault(
                    manifest.shutdownId(), Map.of());
            Set<ChunkKey> requiredChunks = disposition.explicit()
                    ? disposition.writtenChunks() : manifest.chunks();
            for (ChunkKey key : requiredChunks) {
                ChunkPayload payload = chunkData.get(key);
                if (payload == null || !chunks.write(
                        manifest.groupId(), key, payload.data(), payload.preserve())) {
                    throw new IllegalStateException("测试暂存区块数据缺失或写入失败");
                }
            }
            chunks.flush();
        }

        @Override
        public synchronized void discard(ExitManifest manifest) {
            stagedPlayers.remove(manifest.shutdownId());
            stagedChunks.remove(manifest.shutdownId());
        }
    }
}
