package imsng.player_to_player.group;

import com.google.gson.JsonObject;
import imsng.player_to_player.netproto.ControlConnection;
import imsng.player_to_player.netproto.ControlMessage;
import imsng.player_to_player.netproto.MessageType;
import imsng.player_to_player.registry.ChunkKey;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.SocketAddress;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ExitTransactionClientTest {

    private static final UUID ID =
            UUID.fromString("9207ce8d-2b04-4fd8-a9f7-a437ce1211a0");
    private static final ChunkKey CHUNK =
            new ChunkKey("minecraft:overworld", -7, 0);

    @Test
    void cleanManifestChunksDoNotSendChunkPayloadMessages() {
        ChunkKey cleanA = new ChunkKey("minecraft:overworld", 12, -3);
        ChunkKey cleanB = new ChunkKey("minecraft:the_nether", 4, 9);
        ExitManifest manifest = manifest(Set.of(cleanA, cleanB));
        ExitBundle bundle = new ExitBundle(
                manifest, Map.of(ID, new byte[]{1}), Map.of());
        List<MessageType> sent = new ArrayList<>();
        List<String> committedWritten = new ArrayList<>();
        FakeFactory factory = new FakeFactory();
        factory.connections.add(new ScriptedConnection(message -> {
            sent.add(message.type());
            return switch (message.type()) {
                case EXIT_BEGIN -> CompletableFuture.completedFuture(
                        beginAck(manifest, false));
                case EXIT_PLAYER_DATA -> CompletableFuture.completedFuture(
                        booleanAck(MessageType.EXIT_PLAYER_DATA, "written", true));
                case EXIT_CHUNK_DATA -> CompletableFuture.completedFuture(
                        booleanAck(MessageType.EXIT_CHUNK_DATA, "written", true));
                case EXIT_COMMIT -> {
                    if (message.json().has("writtenChunks")) {
                        message.json().getAsJsonArray("writtenChunks")
                                .forEach(element -> committedWritten.add(element.getAsString()));
                    }
                    yield CompletableFuture.completedFuture(booleanAck(
                            MessageType.EXIT_COMMIT, "committed", true));
                }
                default -> throw new AssertionError(message.type());
            };
        }));

        new ExitTransactionClient(factory, ignored -> {
        }).commitUntilAccepted(bundle);

        assertEquals(List.of(MessageType.EXIT_BEGIN, MessageType.EXIT_PLAYER_DATA,
                MessageType.EXIT_COMMIT), sent);
        assertTrue(committedWritten.isEmpty());
    }

    @Test
    void commitDeclaresExactlyWrittenChunksInStableOrder() {
        ChunkKey later = new ChunkKey("minecraft:overworld", 8, 1);
        ChunkKey earlier = new ChunkKey("minecraft:overworld", -4, 2);
        ExitManifest manifest = manifest(Set.of(later, earlier));
        ExitBundle bundle = new ExitBundle(manifest,
                Map.of(ID, new byte[]{1}),
                Map.of(later, new byte[]{2}, earlier, new byte[]{3}));
        List<String> committedWritten = new ArrayList<>();
        FakeFactory factory = new FakeFactory();
        factory.connections.add(new ScriptedConnection(message -> {
            return switch (message.type()) {
                case EXIT_BEGIN -> CompletableFuture.completedFuture(
                        beginAck(manifest, false));
                case EXIT_PLAYER_DATA, EXIT_CHUNK_DATA -> CompletableFuture.completedFuture(
                        booleanAck(message.type(), "written", true));
                case EXIT_COMMIT -> {
                    if (message.json().has("writtenChunks")) {
                        message.json().getAsJsonArray("writtenChunks")
                                .forEach(element -> committedWritten.add(element.getAsString()));
                    }
                    yield CompletableFuture.completedFuture(booleanAck(
                            MessageType.EXIT_COMMIT, "committed", true));
                }
                default -> throw new AssertionError(message.type());
            };
        }));

        new ExitTransactionClient(factory, ignored -> {
        }).commitUntilAccepted(bundle);

        assertEquals(bundle.writtenChunks().stream()
                        .map(ChunkKey::asString).sorted().toList(),
                committedWritten);
    }

    @Test
    void largeExitUsesOneChunkBatchInsteadOfPerChunkRoundTrips() {
        ChunkKey a = new ChunkKey("minecraft:overworld", 1, 1);
        ChunkKey b = new ChunkKey("minecraft:overworld", 2, 1);
        ChunkKey c = new ChunkKey("minecraft:the_nether", 3, 1);
        ExitManifest manifest = manifest(Set.of(a, b, c));
        ExitBundle bundle = new ExitBundle(manifest,
                Map.of(ID, new byte[]{9}),
                Map.of(a, new byte[]{1}, b, new byte[]{2}, c, new byte[]{3}));
        List<MessageType> sent = new ArrayList<>();
        List<Integer> batchSizes = new ArrayList<>();
        FakeFactory factory = new FakeFactory();
        factory.connections.add(new ScriptedConnection(message -> {
            sent.add(message.type());
            return switch (message.type()) {
                case EXIT_BEGIN -> CompletableFuture.completedFuture(
                        beginAck(manifest, false));
                case EXIT_PLAYER_DATA -> CompletableFuture.completedFuture(
                        booleanAck(MessageType.EXIT_PLAYER_DATA, "written", true));
                case EXIT_CHUNK_BATCH -> {
                    batchSizes.add(ChunkDeltaBatch.decode(message.binary()).deltas().size());
                    JsonObject ack = new JsonObject();
                    ack.addProperty("accepted", true);
                    ack.addProperty("written", true);
                    yield CompletableFuture.completedFuture(
                            ControlMessage.of(MessageType.EXIT_CHUNK_BATCH, ack));
                }
                case EXIT_COMMIT -> CompletableFuture.completedFuture(
                        booleanAck(MessageType.EXIT_COMMIT, "committed", true));
                default -> throw new AssertionError(message.type());
            };
        }));

        new ExitTransactionClient(factory, ignored -> {
        }).commitUntilAccepted(bundle);

        assertEquals(List.of(MessageType.EXIT_BEGIN, MessageType.EXIT_PLAYER_DATA,
                MessageType.EXIT_CHUNK_BATCH, MessageType.EXIT_COMMIT), sent);
        assertEquals(List.of(3), batchSizes);
    }

    @Test
    void beginRetriesForeverPolicyUntilConnectionReturnsAcceptedManifest()
            throws Exception {
        ExitManifest requested = manifest(Set.of());
        ExitManifest canonical = manifest(Set.of(CHUNK));
        FakeFactory factory = new FakeFactory();
        factory.connections.add(new ScriptedConnection(message ->
                CompletableFuture.failedFuture(new IOException("offline"))));
        factory.connections.add(new ScriptedConnection(message ->
                CompletableFuture.completedFuture(beginAck(canonical, false))));
        List<Integer> waits = new ArrayList<>();
        ExitTransactionClient client = new ExitTransactionClient(factory, waits::add);

        ExitTransactionClient.BeginOutcome outcome =
                client.beginUntilAccepted(requested);

        assertEquals(canonical, outcome.manifest());
        assertFalse(outcome.committed());
        assertEquals(2, factory.connectCount);
        assertEquals(1, factory.invalidated);
        assertEquals(List.of(1), waits);
    }

    @Test
    void commitReconnectsAndReplaysWholeBundleAfterMidStreamFailure()
            throws Exception {
        ExitManifest manifest = manifest(Set.of(CHUNK));
        ExitBundle bundle = new ExitBundle(manifest,
                Map.of(ID, new byte[]{1}), Map.of(CHUNK, new byte[]{2}));
        List<MessageType> first = new ArrayList<>();
        List<MessageType> second = new ArrayList<>();
        FakeFactory factory = new FakeFactory();
        factory.connections.add(new ScriptedConnection(message -> {
            first.add(message.type());
            return switch (message.type()) {
                case EXIT_BEGIN -> CompletableFuture.completedFuture(beginAck(manifest, false));
                case EXIT_PLAYER_DATA -> CompletableFuture.completedFuture(
                        booleanAck(MessageType.EXIT_PLAYER_DATA, "written", true));
                case EXIT_CHUNK_DATA -> CompletableFuture.failedFuture(
                        new IOException("connection reset"));
                default -> throw new AssertionError(message.type());
            };
        }));
        factory.connections.add(new ScriptedConnection(message -> {
            second.add(message.type());
            return switch (message.type()) {
                case EXIT_BEGIN -> CompletableFuture.completedFuture(beginAck(manifest, false));
                case EXIT_PLAYER_DATA -> CompletableFuture.completedFuture(
                        booleanAck(MessageType.EXIT_PLAYER_DATA, "written", true));
                case EXIT_CHUNK_DATA -> CompletableFuture.completedFuture(
                        booleanAck(MessageType.EXIT_CHUNK_DATA, "written", true));
                case EXIT_COMMIT -> CompletableFuture.completedFuture(
                        booleanAck(MessageType.EXIT_COMMIT, "committed", true));
                default -> throw new AssertionError(message.type());
            };
        }));
        ExitTransactionClient client = new ExitTransactionClient(factory, attempt -> {
        });

        client.commitUntilAccepted(bundle);

        assertEquals(List.of(MessageType.EXIT_BEGIN, MessageType.EXIT_PLAYER_DATA,
                MessageType.EXIT_CHUNK_DATA), first);
        assertEquals(List.of(MessageType.EXIT_BEGIN, MessageType.EXIT_PLAYER_DATA,
                MessageType.EXIT_CHUNK_DATA, MessageType.EXIT_COMMIT), second);
        assertEquals(1, factory.invalidated);
    }

    @Test
    void committedBeginSkipsPayloadReplay() throws Exception {
        ExitManifest manifest = manifest(Set.of(CHUNK));
        ExitBundle bundle = new ExitBundle(manifest,
                Map.of(ID, new byte[]{1}), Map.of(CHUNK, new byte[]{2}));
        List<MessageType> sent = new ArrayList<>();
        FakeFactory factory = new FakeFactory();
        factory.connections.add(new ScriptedConnection(message -> {
            sent.add(message.type());
            return CompletableFuture.completedFuture(beginAck(manifest, true));
        }));

        new ExitTransactionClient(factory, attempt -> {
        }).commitUntilAccepted(bundle);

        assertEquals(List.of(MessageType.EXIT_BEGIN), sent);
        assertTrue(factory.connections.isEmpty());
    }

    @Test
    void oversizedPayloadIsSplitIntoBoundedVerifiedFragments() {
        ExitManifest manifest = manifest(Set.of());
        byte[] large = new byte[ExitPayloadProtocol.FRAGMENT_BYTES * 2 + 17];
        java.util.Arrays.fill(large, (byte) 7);
        ExitBundle bundle = new ExitBundle(
                manifest, Map.of(ID, large), Map.of());
        List<Integer> indexes = new ArrayList<>();
        List<Integer> sizes = new ArrayList<>();
        FakeFactory factory = new FakeFactory();
        factory.connections.add(new ScriptedConnection(message -> {
            if (message.type() == MessageType.EXIT_BEGIN) {
                return CompletableFuture.completedFuture(beginAck(manifest, false));
            }
            if (message.type() == MessageType.EXIT_PLAYER_DATA) {
                int index = message.json().get("fragmentIndex").getAsInt();
                int count = message.json().get("fragmentCount").getAsInt();
                indexes.add(index);
                sizes.add(message.binary().length);
                JsonObject ack = new JsonObject();
                ack.addProperty("accepted", true);
                ack.addProperty("written", index == count - 1);
                return CompletableFuture.completedFuture(
                        ControlMessage.of(MessageType.EXIT_PLAYER_DATA, ack));
            }
            if (message.type() == MessageType.EXIT_COMMIT) {
                return CompletableFuture.completedFuture(booleanAck(
                        MessageType.EXIT_COMMIT, "committed", true));
            }
            throw new AssertionError(message.type());
        }));

        new ExitTransactionClient(factory, ignored -> {
        }).commitUntilAccepted(bundle);

        assertEquals(List.of(0, 1, 2), indexes);
        assertTrue(sizes.stream().allMatch(
                size -> size <= ExitPayloadProtocol.FRAGMENT_BYTES));
    }

    private static ExitManifest manifest(Set<ChunkKey> chunks) {
        return new ExitManifest(
                UUID.fromString("c768829c-9160-43c7-a4fd-ecc2290936f4"),
                ID, ID, Set.of(ID), chunks);
    }

    private static ControlMessage beginAck(ExitManifest manifest, boolean committed) {
        JsonObject json = new JsonObject();
        json.addProperty("accepted", true);
        json.addProperty("committed", committed);
        json.add("manifest", manifest.toJson());
        return ControlMessage.of(MessageType.EXIT_BEGIN, json);
    }

    private static ControlMessage booleanAck(MessageType type, String key, boolean value) {
        JsonObject json = new JsonObject();
        json.addProperty(key, value);
        return ControlMessage.of(type, json);
    }

    private static final class FakeFactory
            implements ExitTransactionClient.ConnectionFactory {
        private final Queue<ControlConnection> connections = new ArrayDeque<>();
        private int connectCount;
        private int invalidated;

        @Override
        public ControlConnection connect() throws IOException {
            connectCount++;
            ControlConnection connection = connections.poll();
            if (connection == null) {
                throw new IOException("没有更多测试连接");
            }
            return connection;
        }

        @Override
        public void invalidate(ControlConnection connection) {
            invalidated++;
        }
    }

    private static final class ScriptedConnection implements ControlConnection {
        private final Function<ControlMessage, CompletableFuture<ControlMessage>> script;

        private ScriptedConnection(
                Function<ControlMessage, CompletableFuture<ControlMessage>> script) {
            this.script = script;
        }

        @Override
        public void send(ControlMessage message) {
        }

        @Override
        public CompletableFuture<ControlMessage> request(ControlMessage message) {
            return script.apply(message);
        }

        @Override
        public SocketAddress remoteAddress() {
            return null;
        }

        @Override
        public UUID peerId() {
            return null;
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
}
