package imsng.player_to_player.server;

import com.google.gson.JsonObject;
import imsng.player_to_player.group.ExitManifest;
import imsng.player_to_player.netproto.ControlConnection;
import imsng.player_to_player.netproto.ControlMessage;
import imsng.player_to_player.netproto.HandlerRegistry;
import imsng.player_to_player.netproto.MessageHandler;
import imsng.player_to_player.netproto.MessageType;
import imsng.player_to_player.registry.ChunkKey;
import org.junit.jupiter.api.Test;

import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ExitTransactionHandlersTest {

    private static final UUID SHUTDOWN_ID =
            UUID.fromString("c768829c-9160-43c7-a4fd-ecc2290936f4");
    private static final UUID CLIENT_ID =
            UUID.fromString("9207ce8d-2b04-4fd8-a9f7-a437ce1211a0");
    private static final ChunkKey CHUNK =
            new ChunkKey("minecraft:overworld", -7, 0);

    @Test
    void finalPayloadsDoNotTouchOfficialSinksBeforeCommit() throws Exception {
        ExitTransactionLedger ledger = new ExitTransactionLedger(
                new InMemoryPersistence());
        List<String> officialWrites = new ArrayList<>();
        ExitTransactionHandlers handlers = new ExitTransactionHandlers(
                ledger,
                (peer, requested) -> requested,
                ignored -> {
                },
                (player, data) -> {
                    officialWrites.add("player");
                    return true;
                },
                new ExitTransactionHandlers.ChunkSink() {
                    @Override
                    public boolean write(UUID groupId, ChunkKey key, byte[] data,
                                         boolean preserveExisting) {
                        officialWrites.add("chunk");
                        return true;
                    }

                    @Override
                    public void flush() {
                        officialWrites.add("flush");
                    }
                },
                ignored -> officialWrites.add("cleanup"),
                Runnable::run);
        FakeRegistry registry = new FakeRegistry();
        handlers.register(registry);
        FakeConnection connection = new FakeConnection(CLIENT_ID);

        registry.handle(connection, ControlMessage.of(
                MessageType.EXIT_BEGIN, manifest().toJson()));
        sendPlayer(registry, connection, new byte[]{1, 2, 3});
        sendChunk(registry, connection, new byte[]{4, 5});

        assertTrue(officialWrites.isEmpty());
    }

    @Test
    void abandoningPartialFinalUploadKeepsOfficialDataUntouched() throws Exception {
        ExitTransactionLedger ledger = new ExitTransactionLedger(
                new InMemoryPersistence());
        List<String> officialWrites = new ArrayList<>();
        ExitTransactionHandlers handlers = new ExitTransactionHandlers(
                ledger,
                (peer, requested) -> requested,
                ignored -> {
                },
                (player, data) -> {
                    officialWrites.add("player");
                    return true;
                },
                new NoopChunkSink(),
                ignored -> officialWrites.add("cleanup"),
                Runnable::run);
        FakeRegistry registry = new FakeRegistry();
        handlers.register(registry);
        FakeConnection connection = new FakeConnection(CLIENT_ID);

        registry.handle(connection, ControlMessage.of(
                MessageType.EXIT_BEGIN, manifest().toJson()));
        sendPlayer(registry, connection, new byte[]{1, 2, 3});
        assertEquals(1, handlers.abandonPendingForRejoin(CLIENT_ID));

        assertEquals(List.of("cleanup"), officialWrites);
    }

    @Test
    void lateFinalPayloadAfterCommitDoesNotStageAgain() throws Exception {
        ExitTransactionLedger ledger = new ExitTransactionLedger(
                new InMemoryPersistence());
        RecordingPayloadStore payloads = new RecordingPayloadStore();
        ExitTransactionHandlers handlers = new ExitTransactionHandlers(
                ledger,
                (peer, requested) -> requested,
                ignored -> {
                },
                payloads,
                () -> {
                },
                ignored -> {
                },
                Runnable::run);
        FakeRegistry registry = new FakeRegistry();
        handlers.register(registry);
        FakeConnection connection = new FakeConnection(CLIENT_ID);
        registry.handle(connection, ControlMessage.of(
                MessageType.EXIT_BEGIN, manifest().toJson()));
        sendPlayer(registry, connection, new byte[]{1});
        sendChunk(registry, connection, new byte[]{2});
        registry.handle(connection, ControlMessage.of(
                MessageType.EXIT_COMMIT, idJson()));
        int stagesBeforeLateFrame = payloads.playerStages;

        JsonObject late = idJson();
        late.addProperty("playerUuid", CLIENT_ID.toString());
        registry.handle(connection, ControlMessage.of(
                MessageType.EXIT_PLAYER_DATA, late, new byte[]{3}));

        assertFalse(connection.last().json().get("written").getAsBoolean());
        assertEquals(stagesBeforeLateFrame, payloads.playerStages);
    }

    @Test
    void cleanupMetadataRetryDoesNotReapplyCommittedPayloads() throws Exception {
        CleanupMarkerFailingPersistence persistence =
                new CleanupMarkerFailingPersistence();
        ExitTransactionLedger ledger = new ExitTransactionLedger(persistence);
        RecordingPayloadStore payloads = new RecordingPayloadStore();
        AtomicInteger cleanups = new AtomicInteger();
        ExitTransactionHandlers handlers = new ExitTransactionHandlers(
                ledger,
                (peer, requested) -> requested,
                ignored -> {
                },
                payloads,
                () -> {
                },
                ignored -> cleanups.incrementAndGet(),
                Runnable::run);
        FakeRegistry registry = new FakeRegistry();
        handlers.register(registry);
        FakeConnection connection = new FakeConnection(CLIENT_ID);
        registry.handle(connection, ControlMessage.of(
                MessageType.EXIT_BEGIN, manifest().toJson()));
        sendPlayer(registry, connection, new byte[]{1});
        sendChunk(registry, connection, new byte[]{2});

        registry.handle(connection, ControlMessage.of(
                MessageType.EXIT_COMMIT, idJson()));
        assertFalse(connection.last().json().get("committed").getAsBoolean());
        registry.handle(connection, ControlMessage.of(
                MessageType.EXIT_COMMIT, idJson()));

        assertTrue(connection.last().json().get("committed").getAsBoolean());
        assertEquals(1, payloads.applies);
        assertEquals(2, cleanups.get());
    }

    @Test
    void completeTransactionWritesFlushesCommitsAndCleansUpInOrder() throws Exception {
        InMemoryPersistence persistence = new InMemoryPersistence();
        ExitTransactionLedger ledger = new ExitTransactionLedger(persistence);
        FakeRegistry registry = new FakeRegistry();
        List<String> events = new ArrayList<>();
        ExitTransactionHandlers handlers = new ExitTransactionHandlers(
                ledger,
                (peer, manifest) -> peer.equals(CLIENT_ID) ? manifest : null,
                manifest -> events.add("protect"),
                (playerId, data) -> {
                    events.add("player");
                    return data.length == 3;
                },
                new ExitTransactionHandlers.ChunkSink() {
                    @Override
                    public boolean write(UUID groupId, ChunkKey key, byte[] data,
                                         boolean preserveExisting) {
                        events.add("chunk");
                        return key.equals(CHUNK) && data.length == 2 && !preserveExisting;
                    }

                    @Override
                    public void flush() {
                        events.add("flush");
                    }
                },
                manifest -> events.add("cleanup"),
                Runnable::run);
        handlers.register(registry);
        FakeConnection connection = new FakeConnection(CLIENT_ID);
        ExitManifest manifest = manifest();

        registry.handle(connection, ControlMessage.of(
                MessageType.EXIT_BEGIN, manifest.toJson()));
        assertTrue(connection.last().json().get("accepted").getAsBoolean());
        assertFalse(connection.last().json().get("committed").getAsBoolean());
        assertEquals(manifest, ExitManifest.fromJson(
                connection.last().json().getAsJsonObject("manifest")));

        registry.handle(connection, ControlMessage.of(MessageType.EXIT_COMMIT,
                idJson()));
        assertFalse(connection.last().json().get("committed").getAsBoolean());

        JsonObject player = idJson();
        player.addProperty("playerUuid", CLIENT_ID.toString());
        registry.handle(connection, ControlMessage.of(
                MessageType.EXIT_PLAYER_DATA, player, new byte[]{1, 2, 3}));
        assertTrue(connection.last().json().get("written").getAsBoolean());

        JsonObject chunk = idJson();
        chunk.addProperty("dimension", CHUNK.dimension());
        chunk.addProperty("x", CHUNK.x());
        chunk.addProperty("z", CHUNK.z());
        chunk.addProperty("preserveExisting", false);
        registry.handle(connection, ControlMessage.of(
                MessageType.EXIT_CHUNK_DATA, chunk, new byte[]{4, 5}));
        assertTrue(connection.last().json().get("written").getAsBoolean());

        registry.handle(connection, ControlMessage.of(MessageType.EXIT_COMMIT,
                idJson()));
        assertTrue(connection.last().json().get("committed").getAsBoolean());
        assertEquals(List.of("protect", "player", "chunk", "flush", "cleanup"),
                events);
        assertFalse(ledger.isProtected(CLIENT_ID));
    }

    @Test
    void committedRecoveryCleansUpOnceAndDuplicateBeginDoesNotTouchNewState()
            throws Exception {
        InMemoryPersistence persistence = new InMemoryPersistence();
        RecordingPayloadStore payloads = new RecordingPayloadStore();
        ExitTransactionLedger first = new ExitTransactionLedger(persistence);
        first.begin(CLIENT_ID, manifest());
        assertTrue(payloads.stagePlayer(manifest(), CLIENT_ID, new byte[]{1}));
        assertTrue(payloads.stageChunk(manifest(), CHUNK, new byte[]{2}, false));
        first.markPlayerReceived(CLIENT_ID, SHUTDOWN_ID, CLIENT_ID);
        first.markChunkReceived(CLIENT_ID, SHUTDOWN_ID, CHUNK);
        first.commit(CLIENT_ID, SHUTDOWN_ID);

        List<ExitManifest> cleaned = new ArrayList<>();
        ExitTransactionHandlers recovered = new ExitTransactionHandlers(
                new ExitTransactionLedger(persistence),
                (peer, manifest) -> null,
                manifest -> {
                },
                payloads,
                () -> {
                },
                cleaned::add,
                Runnable::run);
        recovered.recoverDurableState();
        assertEquals(List.of(manifest()), cleaned);
        assertEquals(1, payloads.applies);
        cleaned.clear();
        FakeRegistry registry = new FakeRegistry();
        recovered.register(registry);
        FakeConnection connection = new FakeConnection(CLIENT_ID);

        registry.handle(connection, ControlMessage.of(
                MessageType.EXIT_BEGIN, manifest().toJson()));

        assertTrue(connection.last().json().get("accepted").getAsBoolean());
        assertTrue(connection.last().json().get("committed").getAsBoolean());
        assertTrue(cleaned.isEmpty());
    }

    @Test
    void pendingRecoveryReappliesProtectionBeforeAcceptingReconnect() {
        InMemoryPersistence persistence = new InMemoryPersistence();
        ExitTransactionLedger first = new ExitTransactionLedger(persistence);
        first.begin(CLIENT_ID, manifest());

        List<ExitManifest> protectedManifests = new ArrayList<>();
        ExitTransactionHandlers recovered = new ExitTransactionHandlers(
                new ExitTransactionLedger(persistence),
                (peer, requested) -> null,
                protectedManifests::add,
                (player, data) -> true,
                new NoopChunkSink(),
                ignored -> {
                },
                Runnable::run);

        recovered.recoverDurableState();

        assertEquals(List.of(manifest()), protectedManifests);
    }

    @Test
    void rejectedPreserveDoesNotCountChunkAsReceived() throws Exception {
        InMemoryPersistence persistence = new InMemoryPersistence();
        ExitTransactionLedger ledger = new ExitTransactionLedger(persistence);
        ExitTransactionHandlers handlers = new ExitTransactionHandlers(
                ledger,
                (peer, requested) -> requested,
                ignored -> {
                },
                (player, data) -> true,
                new ExitTransactionHandlers.ChunkSink() {
                    @Override
                    public boolean validate(UUID groupId, ChunkKey key, byte[] data,
                                            boolean preserveExisting) {
                        return !preserveExisting;
                    }

                    @Override
                    public boolean write(UUID groupId, ChunkKey key, byte[] data,
                                         boolean preserveExisting) {
                        return !preserveExisting;
                    }

                    @Override
                    public void flush() {
                    }
                },
                ignored -> {
                },
                Runnable::run);
        FakeRegistry registry = new FakeRegistry();
        handlers.register(registry);
        FakeConnection connection = new FakeConnection(CLIENT_ID);

        registry.handle(connection, ControlMessage.of(
                MessageType.EXIT_BEGIN, manifest().toJson()));
        JsonObject player = idJson();
        player.addProperty("playerUuid", CLIENT_ID.toString());
        registry.handle(connection, ControlMessage.of(
                MessageType.EXIT_PLAYER_DATA, player, new byte[]{1}));

        JsonObject chunk = idJson();
        chunk.addProperty("dimension", CHUNK.dimension());
        chunk.addProperty("x", CHUNK.x());
        chunk.addProperty("z", CHUNK.z());
        chunk.addProperty("preserveExisting", true);
        registry.handle(connection, ControlMessage.of(
                MessageType.EXIT_CHUNK_DATA, chunk));
        assertFalse(connection.last().json().get("written").getAsBoolean());

        registry.handle(connection, ControlMessage.of(
                MessageType.EXIT_COMMIT, idJson()));
        assertFalse(connection.last().json().get("committed").getAsBoolean());
    }

    @Test
    void queuedBeginIsAwaitedBeforeNoWaitRejoinRecovery() throws Exception {
        InMemoryPersistence persistence = new InMemoryPersistence();
        ExitTransactionLedger ledger = new ExitTransactionLedger(persistence);
        java.util.concurrent.ExecutorService executor =
                java.util.concurrent.Executors.newSingleThreadExecutor();
        CountDownLatch authorizerEntered = new CountDownLatch(1);
        CountDownLatch allowAuthorizer = new CountDownLatch(1);
        ExitTransactionHandlers handlers = new ExitTransactionHandlers(
                ledger,
                (peer, requested) -> {
                    authorizerEntered.countDown();
                    try {
                        allowAuthorizer.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return null;
                    }
                    return requested;
                },
                ignored -> {
                },
                (player, data) -> true,
                new NoopChunkSink(),
                ignored -> {
                },
                executor);
        FakeRegistry registry = new FakeRegistry();
        handlers.register(registry);
        FakeConnection connection = new FakeConnection(CLIENT_ID);

        registry.handle(connection, ControlMessage.of(
                MessageType.EXIT_BEGIN, manifest().toJson()));
        assertTrue(authorizerEntered.await(5, TimeUnit.SECONDS));
        CompletableFuture<Boolean> protectedAfterBegin = CompletableFuture.supplyAsync(
                () -> handlers.isProtectedAfterBegins(CLIENT_ID));
        assertFalse(protectedAfterBegin.isDone());

        allowAuthorizer.countDown();
        assertTrue(protectedAfterBegin.get(5, TimeUnit.SECONDS));
        assertEquals(1, handlers.abandonPendingForRejoinNow(CLIENT_ID));
        assertFalse(handlers.isProtectedNow(CLIENT_ID));
        executor.shutdownNow();
    }

    @Test
    void normalRejoinAbandonsPendingTransactionAfterFlushingLastRealtimeData() {
        InMemoryPersistence persistence = new InMemoryPersistence();
        ExitTransactionLedger ledger = new ExitTransactionLedger(persistence);
        ledger.begin(CLIENT_ID, manifest());
        List<String> events = new ArrayList<>();
        ExitTransactionHandlers handlers = new ExitTransactionHandlers(
                ledger,
                (peer, requested) -> null,
                ignored -> {
                },
                (player, data) -> true,
                new ExitTransactionHandlers.ChunkSink() {
                    @Override
                    public boolean write(UUID groupId, ChunkKey key, byte[] data,
                                         boolean preserveExisting) {
                        return true;
                    }

                    @Override
                    public void flush() {
                        events.add("flush");
                    }
                },
                ignored -> events.add("cleanup"),
                Runnable::run);

        assertEquals(1, handlers.abandonPendingForRejoin(CLIENT_ID));

        assertEquals(List.of("flush", "cleanup"), events);
        assertTrue(ledger.isAbandoned(CLIENT_ID, SHUTDOWN_ID));
        assertFalse(ledger.isProtected(CLIENT_ID));
    }

    private static ExitManifest manifest() {
        return new ExitManifest(SHUTDOWN_ID, CLIENT_ID, CLIENT_ID,
                Set.of(CLIENT_ID), Set.of(CHUNK));
    }

    private static JsonObject idJson() {
        JsonObject json = new JsonObject();
        json.addProperty("shutdownId", SHUTDOWN_ID.toString());
        return json;
    }

    private static void sendPlayer(FakeRegistry registry,
                                   FakeConnection connection, byte[] data)
            throws Exception {
        JsonObject player = idJson();
        player.addProperty("playerUuid", CLIENT_ID.toString());
        registry.handle(connection, ControlMessage.of(
                MessageType.EXIT_PLAYER_DATA, player, data));
        assertTrue(connection.last().json().get("written").getAsBoolean());
    }

    private static void sendChunk(FakeRegistry registry,
                                  FakeConnection connection, byte[] data)
            throws Exception {
        JsonObject chunk = idJson();
        chunk.addProperty("dimension", CHUNK.dimension());
        chunk.addProperty("x", CHUNK.x());
        chunk.addProperty("z", CHUNK.z());
        chunk.addProperty("preserveExisting", false);
        registry.handle(connection, ControlMessage.of(
                MessageType.EXIT_CHUNK_DATA, chunk, data));
        assertTrue(connection.last().json().get("written").getAsBoolean());
    }

    private static final class NoopChunkSink
            implements ExitTransactionHandlers.ChunkSink {
        @Override
        public boolean write(UUID groupId, ChunkKey key, byte[] data,
                             boolean preserveExisting) {
            return true;
        }

        @Override
        public void flush() {
        }
    }

    private static final class FakeRegistry implements HandlerRegistry {
        private final Map<MessageType, MessageHandler> handlers = new HashMap<>();

        @Override
        public void on(MessageType type, MessageHandler handler) {
            handlers.put(type, handler);
        }

        private void handle(ControlConnection connection, ControlMessage message)
                throws Exception {
            handlers.get(message.type()).handle(connection, message);
        }
    }

    private static final class FakeConnection implements ControlConnection {
        private final UUID peerId;
        private final List<ControlMessage> sent = new ArrayList<>();

        private FakeConnection(UUID peerId) {
            this.peerId = peerId;
        }

        private ControlMessage last() {
            return sent.get(sent.size() - 1);
        }

        @Override
        public void send(ControlMessage message) {
            sent.add(message);
        }

        @Override
        public CompletableFuture<ControlMessage> request(ControlMessage message) {
            throw new UnsupportedOperationException();
        }

        @Override
        public SocketAddress remoteAddress() {
            return null;
        }

        @Override
        public UUID peerId() {
            return peerId;
        }

        @Override
        public void setPeerId(UUID peerId) {
        }

        @Override
        public boolean isOpen() {
            return true;
        }

        @Override
        public void close() {
        }
    }

    private static final class InMemoryPersistence
            implements ExitTransactionPersistence {
        private final Map<UUID, ExitTransactionRecord> records = new HashMap<>();

        @Override
        public Map<UUID, ExitTransactionRecord> load() {
            return new HashMap<>(records);
        }

        @Override
        public void save(ExitTransactionRecord record) {
            records.put(record.manifest().shutdownId(), record);
        }
    }

    private static final class CleanupMarkerFailingPersistence
            implements ExitTransactionPersistence {
        private final Map<UUID, ExitTransactionRecord> records = new HashMap<>();
        private boolean failed;

        @Override
        public Map<UUID, ExitTransactionRecord> load() {
            return new HashMap<>(records);
        }

        @Override
        public void save(ExitTransactionRecord record) {
            if (record.cleanupComplete() && !failed) {
                failed = true;
                throw new IllegalStateException("cleanup marker disk failure");
            }
            records.put(record.manifest().shutdownId(), record);
        }
    }

    private static final class RecordingPayloadStore
            implements ExitTransactionHandlers.PayloadStore {
        private final Set<UUID> players = new java.util.HashSet<>();
        private final Set<ChunkKey> chunks = new java.util.HashSet<>();
        private int playerStages;
        private int applies;

        @Override
        public boolean stagePlayer(ExitManifest manifest, UUID playerId,
                                   byte[] gzipNbt) {
            players.add(playerId);
            playerStages++;
            return true;
        }

        @Override
        public boolean stageChunk(ExitManifest manifest, ChunkKey key,
                                  byte[] gzipNbt, boolean preserveExisting) {
            chunks.add(key);
            return true;
        }

        @Override
        public void apply(ExitManifest manifest) {
            if (!players.containsAll(manifest.playerIds())
                    || !chunks.containsAll(manifest.chunks())) {
                throw new IllegalStateException("测试暂存数据不完整");
            }
            applies++;
        }

        @Override
        public void discard(ExitManifest manifest) {
        }
    }
}
