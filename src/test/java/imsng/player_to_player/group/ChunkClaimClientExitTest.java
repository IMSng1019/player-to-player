package imsng.player_to_player.group;

import com.google.gson.JsonObject;
import imsng.player_to_player.netproto.ControlConnection;
import imsng.player_to_player.netproto.ControlMessage;
import imsng.player_to_player.netproto.MessageType;
import imsng.player_to_player.registry.ChunkKey;
import org.junit.jupiter.api.Test;

import java.net.SocketAddress;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ChunkClaimClientExitTest {

    private static final UUID GROUP_ID =
            UUID.fromString("9207ce8d-2b04-4fd8-a9f7-a437ce1211a0");
    private static final ChunkKey CLAIMED =
            new ChunkKey("minecraft:overworld", -7, 0);

    @Test
    void exitFreezeKeepsCurrentClaimsAndRejectsNewClaimsAndReleases() {
        FakeConnection connection = new FakeConnection();
        ChunkClaimClient client = new ChunkClaimClient(connection, GROUP_ID, 1);
        client.claimWithRetry(CLAIMED).join();
        int requestsBeforeFreeze = connection.requests;

        Set<ChunkKey> frozen = client.freezeForExit();
        client.releaseWithData(CLAIMED, new byte[]{1, 2});

        assertEquals(Set.of(CLAIMED), frozen);
        assertTrue(client.isClaimed(CLAIMED));
        assertEquals(requestsBeforeFreeze, connection.requests);
        assertThrows(Exception.class, () -> client.claimWithRetry(
                new ChunkKey("minecraft:overworld", 2, 3)).join());
    }

    private static final class FakeConnection implements ControlConnection {
        private int requests;

        @Override
        public void send(ControlMessage message) {
        }

        @Override
        public CompletableFuture<ControlMessage> request(ControlMessage message) {
            requests++;
            if (message.type() == MessageType.CHUNK_CLAIM_REQUEST) {
                JsonObject json = new JsonObject();
                json.addProperty("granted", true);
                json.addProperty("hasServerData", false);
                return CompletableFuture.supplyAsync(() -> ControlMessage.of(
                                MessageType.CHUNK_CLAIM_RESPONSE, json),
                        CompletableFuture.delayedExecutor(
                                10, java.util.concurrent.TimeUnit.MILLISECONDS));
            }
            throw new AssertionError("冻结后不应发送 " + message.type());
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
