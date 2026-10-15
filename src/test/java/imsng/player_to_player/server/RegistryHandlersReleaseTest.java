package imsng.player_to_player.server;

import com.google.gson.JsonObject;
import imsng.player_to_player.netproto.ControlConnection;
import imsng.player_to_player.netproto.ControlMessage;
import imsng.player_to_player.netproto.HandlerRegistry;
import imsng.player_to_player.netproto.MessageHandler;
import imsng.player_to_player.netproto.MessageType;
import imsng.player_to_player.registry.ChunkKey;
import imsng.player_to_player.registry.ChunkRegistry;
import imsng.player_to_player.registry.ChunkWriteback;
import imsng.player_to_player.registry.PlayerTable;
import imsng.player_to_player.server.GroupTable;
import imsng.player_to_player.registry.RegistryHandlers;
import imsng.player_to_player.registry.RegistryMutationJournal;
import imsng.player_to_player.registry.RegistryStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.SocketAddress;
import java.nio.file.Path;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RegistryHandlersReleaseTest {

    private static final UUID GROUP =
            UUID.fromString("9207ce8d-2b04-4fd8-a9f7-a437ce1211a0");
    private static final UUID RELEASE_ID =
            UUID.fromString("129c3954-0249-45da-b98f-739dc5dab118");
    private static final ChunkKey KEY =
            new ChunkKey("minecraft:overworld", -15, 4);

    @TempDir
    Path temporary;

    @Test
    void lostAckIsReplayedAfterRestartWithoutWritingFinalDataAgain()
            throws Exception {
        MemoryRegistryStore store = new MemoryRegistryStore();
        Path journalFile = temporary.resolve("mutations.jsonl");
        AtomicInteger finalWrites = new AtomicInteger();
        ChunkWriteback writeback = (key, uploader, gzipNbt, transaction) -> {
            ChunkWriteback.ReleaseResult replayed = transaction.replay();
            if (replayed != null) {
                return replayed;
            }
            finalWrites.incrementAndGet();
            return transaction.commit(true);
        };

        ChunkRegistry first = new ChunkRegistry(
                store, null, new RegistryMutationJournal(journalFile));
        assertTrue(first.tryClaim(KEY, GROUP).granted());
        ControlMessage request = releaseRequest();
        ControlMessage firstAck = invokeRelease(first, writeback, request);
        assertTrue(firstAck.json().get("released").getAsBoolean());
        assertTrue(firstAck.json().get("finalDataWritten").getAsBoolean());
        // 模拟 ACK 已由服务端生成但在网络上丢失；周期 checkpoint 后再重启。
        first.persist();

        ChunkRegistry recovered = new ChunkRegistry(
                store, null, new RegistryMutationJournal(journalFile));
        recovered.load();
        ControlMessage replayedAck = invokeRelease(recovered, writeback, request);

        assertTrue(replayedAck.json().get("released").getAsBoolean());
        assertTrue(replayedAck.json().get("finalDataWritten").getAsBoolean());
        assertEquals(1, finalWrites.get(),
                "耐久回执命中后不得再次写入最终区块数据");

        ControlMessage confirmation = confirmationRequest();
        ControlMessage confirmationAck = invoke(
                recovered, writeback, confirmation,
                MessageType.CHUNK_RELEASE_RECEIPT_CONFIRM);
        assertTrue(confirmationAck.json().get("accepted").getAsBoolean());
        recovered.persist();

        ChunkRegistry afterConfirmation = new ChunkRegistry(
                store, null, new RegistryMutationJournal(journalFile));
        afterConfirmation.load();
        assertNull(afterConfirmation.findReleaseReceipt(
                RELEASE_ID, KEY, GROUP, 3,
                imsng.player_to_player.util.Sha256.hex(new byte[]{1, 2, 3})));
        ControlMessage duplicateConfirmation = invoke(
                afterConfirmation, writeback, confirmation,
                MessageType.CHUNK_RELEASE_RECEIPT_CONFIRM);
        assertTrue(duplicateConfirmation.json().get("accepted").getAsBoolean());
    }

    @Test
    void confirmationCannotDeleteReceiptUntilEveryAcceptedRetryFinishes()
            throws Exception {
        ChunkRegistry registry = new ChunkRegistry(
                new MemoryRegistryStore(), null);
        assertTrue(registry.tryClaim(KEY, GROUP).granted());

        CountDownLatch oldRequestEntered = new CountDownLatch(1);
        CountDownLatch allowOldRequest = new CountDownLatch(1);
        AtomicInteger executions = new AtomicInteger();
        ChunkWriteback writeback = (key, uploader, gzipNbt, transaction) -> {
            if (executions.incrementAndGet() == 1) {
                // A 已被 handler 接收并进入执行，但故意停在首次回执查询之前。
                oldRequestEntered.countDown();
                await(allowOldRequest);
            }
            ChunkWriteback.ReleaseResult replayed = transaction.replay();
            return replayed != null ? replayed : transaction.commit(true);
        };

        FakeHandlerRegistry handlers = new FakeHandlerRegistry();
        RegistryHandlers.register(handlers, registry, new PlayerTable(),
                writeback, exits(), new GroupTable());
        FakeConnection connection = new FakeConnection(GROUP);
        ControlMessage request = releaseRequest();

        // A 先到达，客户端超时后用相同 releaseId 发 B；cached IO 池允许 B 先完成。
        handlers.handle(connection, request);
        assertTrue(oldRequestEntered.await(5, TimeUnit.SECONDS));
        handlers.handle(connection, request);
        ControlMessage retryAck = connection.sent.poll(5, TimeUnit.SECONDS);
        assertNotNull(retryAck);
        assertEquals(MessageType.CHUNK_RELEASE_ACK, retryAck.type());
        assertTrue(retryAck.json().get("released").getAsBoolean());

        // B 的正 ACK 触发确认；A 尚未排空时 receipt 只能标记待回收，不能删除。
        handlers.handle(connection, confirmationRequest());
        ControlMessage confirmationAck = connection.sent.poll(
                5, TimeUnit.SECONDS);
        assertNotNull(confirmationAck);
        assertEquals(MessageType.CHUNK_RELEASE_RECEIPT_CONFIRM,
                confirmationAck.type());
        assertTrue(confirmationAck.json().get("accepted").getAsBoolean());

        // 客户端此时已获准卸载，同组可能立即重新取得相同坐标的新 ownership。
        assertTrue(registry.tryClaim(KEY, GROUP).granted());
        allowOldRequest.countDown();

        ControlMessage oldAck = connection.sent.poll(5, TimeUnit.SECONDS);
        assertNotNull(oldAck);
        assertEquals(MessageType.CHUNK_RELEASE_ACK, oldAck.type());
        assertEquals(GROUP, registry.ownerOf(KEY),
                "旧重试不得释放同组后来重新取得的新 ownership");

        awaitReceiptRemoval(registry);
    }

    private ControlMessage invokeRelease(ChunkRegistry registry,
                                         ChunkWriteback writeback,
                                         ControlMessage request)
            throws Exception {
        return invoke(registry, writeback, request,
                MessageType.CHUNK_RELEASE_ACK);
    }

    private ControlMessage invoke(ChunkRegistry registry,
                                  ChunkWriteback writeback,
                                  ControlMessage request,
                                  MessageType expectedResponse)
            throws Exception {
        FakeHandlerRegistry handlers = new FakeHandlerRegistry();
        RegistryHandlers.register(handlers, registry, new PlayerTable(),
                writeback, exits(), new GroupTable());
        FakeConnection connection = new FakeConnection(GROUP);

        handlers.handle(connection, request);

        ControlMessage response = connection.sent.poll(5, TimeUnit.SECONDS);
        assertNotNull(response, "释放处理器必须返回应答");
        assertEquals(expectedResponse, response.type());
        return response;
    }

    private static ControlMessage releaseRequest() {
        JsonObject json = new JsonObject();
        json.addProperty("dimension", KEY.dimension());
        json.addProperty("x", KEY.x());
        json.addProperty("z", KEY.z());
        json.addProperty("groupId", GROUP.toString());
        json.addProperty("releaseId", RELEASE_ID.toString());
        return ControlMessage.of(
                MessageType.CHUNK_RELEASE, json, new byte[]{1, 2, 3});
    }

    private static ControlMessage confirmationRequest() {
        JsonObject json = new JsonObject();
        json.addProperty("releaseId", RELEASE_ID.toString());
        json.addProperty("groupId", GROUP.toString());
        return ControlMessage.of(
                MessageType.CHUNK_RELEASE_RECEIPT_CONFIRM, json);
    }

    private static ExitTransactionHandlers exits() {
        ExitTransactionLedger ledger = new ExitTransactionLedger(
                new ExitTransactionPersistence() {
                    @Override
                    public Map<UUID, ExitTransactionRecord> load() {
                        return Map.of();
                    }

                    @Override
                    public void save(ExitTransactionRecord record) {
                    }
                });
        ExitTransactionHandlers.ChunkSink chunks =
                new ExitTransactionHandlers.ChunkSink() {
                    @Override
                    public boolean write(UUID groupId, ChunkKey key,
                                         byte[] gzipNbt,
                                         boolean preserveExisting) {
                        return true;
                    }

                    @Override
                    public void flush() {
                    }
                };
        return new ExitTransactionHandlers(
                ledger, (peer, manifest) -> manifest, manifest -> {
                }, (player, data) -> true, chunks, manifest -> {
                }, Runnable::run);
    }

    private static void await(CountDownLatch latch) {
        try {
            if (!latch.await(5, TimeUnit.SECONDS)) {
                throw new AssertionError("等待旧释放请求放行超时");
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new AssertionError("等待旧释放请求时被中断", interrupted);
        }
    }

    private static void awaitReceiptRemoval(ChunkRegistry registry)
            throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (System.nanoTime() < deadline) {
            if (registry.findReleaseReceipt(
                    RELEASE_ID, KEY, GROUP, 3,
                    imsng.player_to_player.util.Sha256.hex(
                            new byte[]{1, 2, 3})) == null) {
                return;
            }
            Thread.sleep(10);
        }
        assertNull(registry.findReleaseReceipt(
                RELEASE_ID, KEY, GROUP, 3,
                imsng.player_to_player.util.Sha256.hex(
                        new byte[]{1, 2, 3})),
                "所有已接收重试排空后应回收 receipt");
    }

    private static final class FakeHandlerRegistry implements HandlerRegistry {
        private final Map<MessageType, MessageHandler> handlers =
                new EnumMap<>(MessageType.class);

        @Override
        public void on(MessageType type, MessageHandler handler) {
            handlers.put(type, handler);
        }

        private void handle(ControlConnection connection,
                            ControlMessage message) {
            handlers.get(message.type()).handle(connection, message);
        }
    }

    private static final class FakeConnection implements ControlConnection {
        private final BlockingQueue<ControlMessage> sent =
                new LinkedBlockingQueue<>();
        private UUID peerId;

        private FakeConnection(UUID peerId) {
            this.peerId = peerId;
        }

        @Override
        public void send(ControlMessage message) {
            sent.add(message);
        }

        @Override
        public CompletableFuture<ControlMessage> request(
                ControlMessage message) {
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
            this.peerId = peerId;
        }

        @Override
        public boolean isOpen() {
            return true;
        }

        @Override
        public void close() {
        }
    }

    private static final class MemoryRegistryStore implements RegistryStore {
        private Map<ChunkKey, UUID> claims = new HashMap<>();

        @Override
        public Map<ChunkKey, UUID> load() {
            return new HashMap<>(claims);
        }

        @Override
        public void save(Map<ChunkKey, UUID> snapshot) {
            claims = new HashMap<>(snapshot);
        }

        @Override
        public String describe() {
            return "memory";
        }
    }
}
