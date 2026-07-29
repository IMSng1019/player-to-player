package imsng.player_to_player.group;

import com.google.gson.JsonObject;
import imsng.player_to_player.netproto.ControlConnection;
import imsng.player_to_player.netproto.ControlMessage;
import imsng.player_to_player.netproto.MessageType;
import net.minecraft.nbt.CompoundTag;
import net.minecraft.nbt.DoubleTag;
import net.minecraft.nbt.ListTag;
import org.junit.jupiter.api.Test;

import java.net.SocketAddress;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;

class PlayerStateUploadServiceTest {

    private static final UUID PLAYER_ID =
            UUID.fromString("9207ce8d-2b04-4fd8-a9f7-a437ce1211a0");

    @Test
    void confirmedDuplicateIsSkippedAndChangedStateIsUploaded() {
        FakeConnection connection = new FakeConnection();
        connection.responses.add(success());
        connection.responses.add(success());
        PlayerStateUploadService service = new PlayerStateUploadService(
                connection, Runnable::run, ignored -> {
        });

        service.offer(PLAYER_ID, playerAt(1.0));
        service.offer(PLAYER_ID, playerAt(1.0));
        service.offer(PLAYER_ID, playerAt(2.0));

        assertEquals(2, connection.requests);
    }

    @Test
    void failedStateRemainsReplaceableByNewerState() throws Exception {
        FakeConnection connection = new FakeConnection();
        connection.responses.add(CompletableFuture.failedFuture(
                new java.io.IOException("offline")));
        connection.responses.add(success());
        PlayerStateUploadService service = new PlayerStateUploadService(
                connection, Runnable::run, ignored -> {
        });

        service.offer(PLAYER_ID, playerAt(1.0));
        service.offer(PLAYER_ID, playerAt(9.0));

        CompoundTag uploaded = PlayerStateNbt.decodeValidated(
                connection.lastRequest.binary());
        assertEquals(9.0, uploaded.getList("Pos", 6).getDouble(0), 0.0);
        assertEquals(2, connection.requests);
    }

    private static CompletableFuture<ControlMessage> success() {
        JsonObject json = new JsonObject();
        json.addProperty("written", true);
        return CompletableFuture.completedFuture(
                ControlMessage.of(MessageType.PLAYER_DATA, json));
    }

    private static CompoundTag playerAt(double x) {
        CompoundTag tag = new CompoundTag();
        tag.putString("Dimension", "minecraft:overworld");
        ListTag pos = new ListTag();
        pos.add(DoubleTag.valueOf(x));
        pos.add(DoubleTag.valueOf(80.0));
        pos.add(DoubleTag.valueOf(2.0));
        tag.put("Pos", pos);
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

