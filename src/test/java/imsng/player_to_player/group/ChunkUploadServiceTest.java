package imsng.player_to_player.group;

import com.google.gson.JsonObject;
import imsng.player_to_player.netproto.ControlConnection;
import imsng.player_to_player.netproto.ControlMessage;
import imsng.player_to_player.netproto.MessageType;
import imsng.player_to_player.registry.ChunkKey;
import net.minecraft.nbt.CompoundTag;
import org.junit.jupiter.api.Test;

import java.net.SocketAddress;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ChunkUploadServiceTest {

    private static final UUID GROUP_ID =
            UUID.fromString("9207ce8d-2b04-4fd8-a9f7-a437ce1211a0");
    private static final ChunkKey KEY =
            new ChunkKey("minecraft:overworld", -7, 0);

    @Test
    void failedUploadKeepsLatestReplacementUntilAcknowledged() throws Exception {
        FakeConnection connection = new FakeConnection();
        connection.responses.add(CompletableFuture.failedFuture(
                new java.io.IOException("offline")));
        connection.responses.add(success());
        ChunkUploadService service = new ChunkUploadService(connection, GROUP_ID,
                Runnable::run, ignored -> {
        }, key -> true);

        service.enqueue(KEY, chunkAt(1));
        service.enqueue(KEY, chunkAt(9));

        CompoundTag uploaded = net.minecraft.nbt.NbtIo.readCompressed(
                new java.io.ByteArrayInputStream(connection.lastRequest.binary()),
                net.minecraft.nbt.NbtAccounter.unlimitedHeap());
        assertEquals(9, uploaded.getInt("value"));
        assertEquals(2, connection.requests);
        assertEquals(9, service.latestSnapshot(KEY).getInt("value"));
    }

    private static CompletableFuture<ControlMessage> success() {
        JsonObject json = new JsonObject();
        json.addProperty("written", true);
        return CompletableFuture.completedFuture(
                ControlMessage.of(MessageType.CHUNK_DATA_UPLOAD, json));
    }

    private static CompoundTag chunkAt(int value) {
        CompoundTag tag = new CompoundTag();
        tag.putInt("xPos", KEY.x());
        tag.putInt("zPos", KEY.z());
        tag.putInt("value", value);
        return tag;
    }

    private static final class FakeConnection implements ControlConnection {
        private final Queue<CompletableFuture<ControlMessage>> responses =
                new ArrayDeque<>();
        private int requests;
        private ControlMessage lastRequest;

        @Override
        public void send(ControlMessage message) {
        }

        @Override
        public CompletableFuture<ControlMessage> request(ControlMessage message) {
            requests++;
            lastRequest = message;
            return responses.remove();
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

