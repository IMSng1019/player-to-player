package imsng.player_to_player.group;

import com.google.gson.JsonObject;
import imsng.player_to_player.netproto.ControlConnection;
import imsng.player_to_player.netproto.ControlMessage;
import imsng.player_to_player.netproto.MessageType;
import net.minecraft.nbt.CompoundTag;
import net.minecraft.nbt.DoubleTag;
import net.minecraft.nbt.ListTag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class PlayerDataClientTest {

    private static final UUID PLAYER_ID =
            UUID.fromString("9207ce8d-2b04-4fd8-a9f7-a437ce1211a0");

    @Test
    void requestsAndValidatesAuthoritativePlayerData() throws Exception {
        JsonObject json = new JsonObject();
        json.addProperty("exists", true);
        FakeConnection connection = new FakeConnection(CompletableFuture.completedFuture(
                ControlMessage.of(MessageType.PLAYER_DATA, json,
                        PlayerStateNbt.encode(playerAt(-92.0, 119.0, 8.0)))));

        CompoundTag state = PlayerDataClient.requestAuthoritative(connection, PLAYER_ID);

        assertEquals(-92.0, state.getList("Pos", 6).getDouble(0), 0.0);
        assertEquals(MessageType.PLAYER_DATA_REQUEST, connection.request.type());
        assertEquals(PLAYER_ID.toString(),
                connection.request.json().get("playerUuid").getAsString());
    }

    @Test
    void rejectsMissingServerPlayerData() {
        JsonObject json = new JsonObject();
        json.addProperty("exists", false);
        FakeConnection connection = new FakeConnection(CompletableFuture.completedFuture(
                ControlMessage.of(MessageType.PLAYER_DATA, json)));

        assertThrows(IOException.class,
                () -> PlayerDataClient.requestAuthoritative(connection, PLAYER_ID));
    }

    @Test
    void rejectsErrorResponse() {
        FakeConnection connection = new FakeConnection(CompletableFuture.completedFuture(
                ControlMessage.of(MessageType.ERROR)));

        assertThrows(IOException.class,
                () -> PlayerDataClient.requestAuthoritative(connection, PLAYER_ID));
    }

    @Test
    void rejectsEmptyBinaryAttachment() {
        JsonObject json = new JsonObject();
        json.addProperty("exists", true);
        FakeConnection connection = new FakeConnection(CompletableFuture.completedFuture(
                ControlMessage.of(MessageType.PLAYER_DATA, json)));

        assertThrows(IOException.class,
                () -> PlayerDataClient.requestAuthoritative(connection, PLAYER_ID));
    }

    @Test
    void rejectsInvalidPlayerNbt() {
        JsonObject json = new JsonObject();
        json.addProperty("exists", true);
        FakeConnection connection = new FakeConnection(CompletableFuture.completedFuture(
                ControlMessage.of(MessageType.PLAYER_DATA, json, new byte[]{1, 2, 3})));

        assertThrows(IOException.class,
                () -> PlayerDataClient.requestAuthoritative(connection, PLAYER_ID));
    }

    @Test
    void rejectsTimedOutRequest() {
        FakeConnection connection = new FakeConnection(CompletableFuture.failedFuture(
                new TimeoutException("test timeout")));

        assertThrows(IOException.class,
                () -> PlayerDataClient.requestAuthoritative(connection, PLAYER_ID));
    }

    private static CompoundTag playerAt(double x, double y, double z) {
        CompoundTag tag = new CompoundTag();
        tag.putString("Dimension", "minecraft:overworld");
        ListTag pos = new ListTag();
        pos.add(DoubleTag.valueOf(x));
        pos.add(DoubleTag.valueOf(y));
        pos.add(DoubleTag.valueOf(z));
        tag.put("Pos", pos);
        return tag;
    }

    private static final class FakeConnection implements ControlConnection {
        private final CompletableFuture<ControlMessage> response;
        private ControlMessage request;

        private FakeConnection(CompletableFuture<ControlMessage> response) {
            this.response = response;
        }

        @Override
        public void send(ControlMessage message) {
        }

        @Override
        public CompletableFuture<ControlMessage> request(ControlMessage message) {
            request = message;
            return response;
        }

        @Override
        public SocketAddress remoteAddress() {
            return new InetSocketAddress("127.0.0.1", 0);
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
