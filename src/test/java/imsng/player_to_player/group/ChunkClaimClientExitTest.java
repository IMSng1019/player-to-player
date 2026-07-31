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
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;

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
        CompletableFuture<Void> rejectedRelease =
                client.releaseWithData(CLAIMED, new byte[]{1, 2});

        assertEquals(Set.of(CLAIMED), frozen);
        assertTrue(client.isClaimed(CLAIMED));
        assertEquals(requestsBeforeFreeze, connection.requests);
        assertTrue(rejectedRelease.isCompletedExceptionally(),
                "冻结后新 release 不能静默完成");
        assertThrows(Exception.class, () -> client.claimWithRetry(
                new ChunkKey("minecraft:overworld", 2, 3)).join());
    }

    @Test
    void exitPreparationStopsPendingClaimsButAllowsAlreadyStartedReleaseAfterFreeze()
            throws Exception {
        FakeConnection connection = new FakeConnection();
        ChunkClaimClient client = new ChunkClaimClient(connection, GROUP_ID, 1);
        client.claimWithRetry(CLAIMED).join();

        CompletableFuture<Void> release = client.releaseWhenConfirmed(CLAIMED, null);
        assertTrue(connection.awaitRequests(2));
        CompletableFuture<ChunkClaimClient.ClaimOutcome> pendingClaim =
                client.claimWithRetry(new ChunkKey("minecraft:overworld", 2, 3));
        assertTrue(connection.awaitRequests(3));

        client.beginExitPreparation();
        assertThrows(CompletionException.class, pendingClaim::join);
        Set<ChunkKey> frozen = client.freezeForExit();
        assertEquals(Set.of(CLAIMED), frozen);
        assertTrue(!release.isDone());

        connection.pendingRelease.complete(releaseAck(true, false).join());
        release.join();
        assertTrue(!client.isClaimed(CLAIMED));
    }

    @Test
    void exitPreparationStillAllowsAReleaseStartedByPreUnload() {
        FakeConnection connection = new FakeConnection();
        ChunkClaimClient client = new ChunkClaimClient(connection, GROUP_ID, 1);
        client.claimWithRetry(CLAIMED).join();

        client.beginExitPreparation();
        CompletableFuture<Void> release = client.releaseWhenConfirmed(CLAIMED, null);
        connection.pendingRelease.complete(releaseAck(true, false).join());
        release.join();

        assertTrue(!client.isClaimed(CLAIMED));
    }

    private static CompletableFuture<ControlMessage> releaseAck(
            boolean released, boolean finalDataWritten) {
        JsonObject json = new JsonObject();
        json.addProperty("released", released);
        json.addProperty("finalDataWritten", finalDataWritten);
        return CompletableFuture.completedFuture(
                ControlMessage.of(MessageType.CHUNK_RELEASE_ACK, json));
    }

    private static final class FakeConnection implements ControlConnection {
        private int requests;
        private final CompletableFuture<ControlMessage> pendingRelease =
                new CompletableFuture<>();
        private final CompletableFuture<ControlMessage> pendingClaim =
                new CompletableFuture<>();

        @Override
        public void send(ControlMessage message) {
        }

        @Override
        public CompletableFuture<ControlMessage> request(ControlMessage message) {
            synchronized (this) {
                requests++;
                notifyAll();
            }
            if (message.type() == MessageType.CHUNK_CLAIM_REQUEST) {
                JsonObject json = new JsonObject();
                json.addProperty("granted", true);
                json.addProperty("hasServerData", false);
                if (requests == 1) {
                    return CompletableFuture.supplyAsync(() -> ControlMessage.of(
                                    MessageType.CHUNK_CLAIM_RESPONSE, json),
                            CompletableFuture.delayedExecutor(
                                    10, TimeUnit.MILLISECONDS));
                }
                return pendingClaim;
            }
            if (message.type() == MessageType.CHUNK_RELEASE) {
                return pendingRelease;
            }
            throw new AssertionError("不应发送 " + message.type());
        }

        private synchronized boolean awaitRequests(int expected) throws InterruptedException {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (requests < expected) {
                long remaining = deadline - System.nanoTime();
                if (remaining <= 0) {
                    return false;
                }
                TimeUnit.NANOSECONDS.timedWait(this, remaining);
            }
            return true;
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
