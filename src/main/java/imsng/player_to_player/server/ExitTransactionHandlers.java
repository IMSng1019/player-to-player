package imsng.player_to_player.server;

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
        boolean write(UUID groupId, ChunkKey key, byte[] gzipNbt,
                      boolean preserveExisting);

        void flush();
    }

    @FunctionalInterface
    public interface CommitCleanup {
        void cleanup(ExitManifest manifest);
    }

    private final ExitTransactionLedger ledger;
    private final BeginAuthorizer authorizer;
    private final PendingProtection pendingProtection;
    private final PlayerSink playerSink;
    private final ChunkSink chunkSink;
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
        this.ledger = ledger;
        this.authorizer = authorizer;
        this.pendingProtection = pendingProtection;
        this.playerSink = playerSink;
        this.chunkSink = chunkSink;
        this.cleanup = cleanup;
        this.executor = executor;
    }

    public void register(HandlerRegistry registry) {
        registry.on(MessageType.EXIT_BEGIN, this::handleBegin);
        registry.on(MessageType.EXIT_PLAYER_DATA, this::handlePlayer);
        registry.on(MessageType.EXIT_CHUNK_DATA, this::handleChunk);
        registry.on(MessageType.EXIT_COMMIT, this::handleCommit);
    }

    public boolean isProtectedAfterBegins(UUID clientId) {
        CompletableFuture<Void> barrier = beginBarriers.get(clientId);
        if (barrier != null) {
            barrier.handle((ignored, failure) -> null).join();
        }
        return ledger.isProtected(clientId);
    }

    /** 服务端启动时先恢复 pending 冻结，再补做 committed 但尚未完成的清理。 */
    public void recoverDurableState() {
        for (ExitManifest manifest : ledger.pendingManifests()) {
            pendingProtection.protect(manifest);
        }
        for (ExitManifest manifest : ledger.committedManifestsNeedingCleanup()) {
            ensureCleanupComplete(manifest);
        }
    }

    /** 新进程正常申请角色代表旧退出进程已被强杀：保留最后实时数据并释放旧占用。 */
    public int abandonPendingForRejoin(UUID clientId) {
        CompletableFuture<Void> barrier = beginBarriers.get(clientId);
        if (barrier != null) {
            barrier.handle((ignored, failure) -> null).join();
        }
        int abandoned = 0;
        for (ExitManifest manifest : ledger.unfinishedManifestsForClient(clientId)) {
            if (!ledger.isCommitted(clientId, manifest.shutdownId())) {
                chunkSink.flush();
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
            boolean accepted = false;
            boolean written = false;
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
                    accepted = assembled.accepted();
                    if (assembled.complete()
                            && playerSink.write(playerId, assembled.payload())) {
                        written = ledger.markPlayerReceived(
                                peerId, shutdownId, playerId);
                        accepted = written;
                    }
                }
            } catch (Exception e) {
                LOGGER.warn("EXIT_PLAYER_DATA 处理失败", e);
            }
            JsonObject out = new JsonObject();
            out.addProperty("accepted", accepted);
            out.addProperty("written", written);
            connection.send(message.reply(MessageType.EXIT_PLAYER_DATA, out, null));
        });
    }

    private void handleChunk(ControlConnection connection, ControlMessage message) {
        executor.execute(() -> {
            boolean accepted = false;
            boolean written = false;
            try {
                UUID peerId = connection.peerId();
                UUID shutdownId = parseUuid(message.json(), "shutdownId");
                ChunkKey key = parseChunk(message.json());
                boolean preserve = JsonUtil.getBoolean(
                        message.json(), "preserveExisting", false);
                ExitManifest manifest = ledger.manifest(peerId, shutdownId);
                if (manifest != null && manifest.chunks().contains(key)) {
                    if (preserve) {
                        written = chunkSink.write(
                                manifest.groupId(), key, null, true)
                                && ledger.markChunkReceived(
                                peerId, shutdownId, key);
                        accepted = written;
                    } else {
                        ExitPayloadAssembler.Result assembled = assemble(
                                new ExitPayloadAssembler.Key(
                                        shutdownId, "chunk", key.asString()),
                                message);
                        accepted = assembled.accepted();
                        if (assembled.complete()
                                && chunkSink.write(manifest.groupId(), key,
                                assembled.payload(), false)) {
                            written = ledger.markChunkReceived(
                                    peerId, shutdownId, key);
                            accepted = written;
                        }
                    }
                }
            } catch (Exception e) {
                LOGGER.warn("EXIT_CHUNK_DATA 处理失败", e);
            }
            JsonObject out = new JsonObject();
            out.addProperty("accepted", accepted);
            out.addProperty("written", written);
            connection.send(message.reply(MessageType.EXIT_CHUNK_DATA, out, null));
        });
    }

    private void handleCommit(ControlConnection connection, ControlMessage message) {
        executor.execute(() -> {
            boolean committed = false;
            try {
                UUID peerId = connection.peerId();
                UUID shutdownId = parseUuid(message.json(), "shutdownId");
                ExitManifest manifest = ledger.manifest(peerId, shutdownId);
                if (manifest != null && ledger.isCommitted(peerId, shutdownId)) {
                    ensureCleanupComplete(manifest);
                    committed = true;
                } else if (manifest != null
                        && ledger.isReadyToCommit(peerId, shutdownId)) {
                    // 先刷新全部已接受区块，确认数据耐久后再写 committed 标记。
                    chunkSink.flush();
                    committed = ledger.commit(peerId, shutdownId);
                    if (committed) {
                        ensureCleanupComplete(manifest);
                    }
                }
            } catch (Exception e) {
                LOGGER.warn("EXIT_COMMIT 处理失败", e);
            }
            JsonObject out = new JsonObject();
            out.addProperty("committed", committed);
            connection.send(message.reply(MessageType.EXIT_COMMIT, out, null));
        });
    }

    private void ensureCleanupComplete(ExitManifest manifest) {
        if (ledger.isCleanupComplete(manifest.clientId(), manifest.shutdownId())) {
            return;
        }
        cleanup.cleanup(manifest);
        if (!ledger.markCleanupComplete(
                manifest.clientId(), manifest.shutdownId())) {
            throw new IllegalStateException("退出事务清理状态落盘失败");
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
}
