package imsng.player_to_player.server;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import imsng.player_to_player.group.ExitManifest;
import imsng.player_to_player.netproto.ControlConnection;
import imsng.player_to_player.netproto.ControlMessage;
import imsng.player_to_player.netproto.HandlerRegistry;
import imsng.player_to_player.netproto.MessageType;
import imsng.player_to_player.registry.ChunkKey;
import imsng.player_to_player.util.JsonUtil;
import imsng.player_to_player.util.Sha256;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
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
    private final ConcurrentHashMap<UUID, CompletableFuture<Void>> beginBarriers =
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
                (connection, action) -> action.getAsBoolean());
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
    }

    public void register(HandlerRegistry registry) {
        registry.on(MessageType.EXIT_BEGIN, this::handleBegin);
        registry.on(MessageType.EXIT_PLAYER_DATA, this::handlePlayer);
        registry.on(MessageType.EXIT_CHUNK_DATA, this::handleChunk);
        registry.on(MessageType.EXIT_COMMIT, this::handleCommit);
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
                        ensureCleanupComplete(effective);
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

    private void handleCommit(ControlConnection connection, ControlMessage message) {
        executor.execute(() -> {
            boolean[] committed = new boolean[1];
            sessionGuard.run(connection, () -> {
                try {
                UUID peerId = connection.peerId();
                UUID shutdownId = parseUuid(message.json(), "shutdownId");
                Set<ChunkKey> writtenChunks = parseWrittenChunks(message.json());
                ExitManifest manifest = ledger.manifest(peerId, shutdownId);
                if (manifest != null) {
                    // 暂存对象已经逐个 force；先耐久写 committed 意图，再应用正式数据。
                    if (ledger.commit(peerId, shutdownId, writtenChunks)) {
                        ensureCleanupComplete(manifest);
                        // 只有正式数据和清理元数据全部耐久完成后才允许客户端关闭。
                        committed[0] = true;
                    }
                }
                } catch (Exception e) {
                    LOGGER.warn("EXIT_COMMIT 处理失败", e);
                }
                return true;
            });
            JsonObject out = new JsonObject();
            out.addProperty("committed", committed[0]);
            connection.send(message.reply(MessageType.EXIT_COMMIT, out, null));
        });
    }

    private void ensureCleanupComplete(ExitManifest manifest) {
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
