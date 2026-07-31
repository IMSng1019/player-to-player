package imsng.player_to_player.group;

import com.google.gson.JsonObject;
import imsng.player_to_player.netproto.ControlConnection;
import imsng.player_to_player.netproto.ControlMessage;
import imsng.player_to_player.netproto.MessageType;
import imsng.player_to_player.registry.ChunkKey;
import org.junit.jupiter.api.Test;

import java.net.SocketAddress;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ChunkClaimClientReleaseTest {

    private static final UUID GROUP_ID =
            UUID.fromString("9207ce8d-2b04-4fd8-a9f7-a437ce1211a0");
    private static final ChunkKey KEY =
            new ChunkKey("minecraft:overworld", -7, 0);

    @Test
    void claimWithoutServerDataRequiresInitialWriteUntilServerDataIsKnown() {
        FakeConnection connection = new FakeConnection();
        connection.responses.add(claimResponse(false));
        ChunkClaimClient client = new ChunkClaimClient(connection, GROUP_ID, 1);

        client.claimWithRetry(KEY).join();

        assertTrue(client.requiresInitialWrite(KEY));
        client.markServerDataKnown(KEY);
        assertFalse(client.requiresInitialWrite(KEY));
    }

    @Test
    void claimWithServerDataDoesNotRequireInitialWrite() {
        FakeConnection connection = new FakeConnection();
        connection.responses.add(claimResponse(true));
        ChunkClaimClient client = new ChunkClaimClient(connection, GROUP_ID, 1);

        client.claimWithRetry(KEY).join();

        assertFalse(client.requiresInitialWrite(KEY));
    }

    @Test
    void lateServerDataAcknowledgementDoesNotRecreateForgottenState() {
        FakeConnection connection = new FakeConnection();
        connection.responses.add(claimResponse(false));
        ChunkClaimClient client = new ChunkClaimClient(connection, GROUP_ID, 1);
        client.claimWithRetry(KEY).join();

        client.forgetLocal(KEY);
        client.markServerDataKnown(KEY);

        assertFalse(client.requiresInitialWrite(KEY));
        assertFalse(client.isClaimed(KEY));
    }

    @Test
    void releaseRetriesEveryNonSuccessResponseAndKeepsClaimUntilPositiveAck()
            throws Exception {
        FakeConnection connection = new FakeConnection();
        connection.responses.add(claimResponse(false));
        connection.responses.add(CompletableFuture.failedFuture(
                new java.io.IOException("offline")));
        connection.responses.add(CompletableFuture.completedFuture(null));
        connection.responses.add(CompletableFuture.completedFuture(
                ControlMessage.of(MessageType.CHUNK_CLAIM_RESPONSE)));
        connection.responses.add(releaseAck(false, false));
        connection.responses.add(releaseAck(true, true));
        AtomicReference<Runnable> retry = new AtomicReference<>();
        ChunkClaimClient client = new ChunkClaimClient(
                connection, GROUP_ID, 1, retry::set);
        client.claimWithRetry(KEY).join();

        CompletableFuture<Void> released = client.releaseWhenConfirmed(KEY, new byte[]{1, 2});
        for (int failedAttempt = 0; failedAttempt < 4; failedAttempt++) {
            assertTrue(connection.awaitRequests(failedAttempt + 2));
            assertTrue(client.isClaimed(KEY), "正 ACK 前不能摘除 claimed");
            Runnable next = awaitRetry(retry);
            retry.set(null);
            next.run();
        }
        assertTrue(connection.awaitRequests(6));
        released.join();

        assertFalse(client.isClaimed(KEY));
        assertFalse(client.requiresInitialWrite(KEY));
        List<String> releaseIds = connection.requests.stream()
                .filter(request -> request.type() == MessageType.CHUNK_RELEASE)
                .map(request -> request.json().get("releaseId").getAsString())
                .toList();
        assertEquals(5, releaseIds.size());
        assertEquals(1, releaseIds.stream().distinct().count(),
                "同一释放操作的所有网络重试必须复用稳定 releaseId");
        UUID.fromString(releaseIds.get(0));
    }

    @Test
    void nonEmptyReleaseRequiresFinalDataWrittenInAdditionToReleased() throws Exception {
        FakeConnection connection = new FakeConnection();
        connection.responses.add(claimResponse(true));
        connection.responses.add(releaseAck(true, false));
        connection.responses.add(releaseAck(true, true));
        AtomicReference<Runnable> retry = new AtomicReference<>();
        ChunkClaimClient client = new ChunkClaimClient(
                connection, GROUP_ID, 1, retry::set);
        client.claimWithRetry(KEY).join();

        CompletableFuture<Void> released = client.releaseWhenConfirmed(KEY, new byte[]{7});
        assertTrue(connection.awaitRequests(2));
        assertTrue(client.isClaimed(KEY));
        awaitRetry(retry).run();
        assertTrue(connection.awaitRequests(3));
        released.join();
        assertFalse(client.isClaimed(KEY));
    }

    @Test
    void emptyReleaseOnlyRequiresReleasedFlag() throws Exception {
        FakeConnection connection = new FakeConnection();
        connection.responses.add(claimResponse(true));
        connection.responses.add(releaseAck(true, false));
        ChunkClaimClient client = new ChunkClaimClient(
                connection, GROUP_ID, 1, ignored -> {
                });
        client.claimWithRetry(KEY).join();

        client.releaseWhenConfirmed(KEY, null).join();

        assertFalse(client.isClaimed(KEY));
    }

    @Test
    void receiptConfirmationRetriesInBackgroundWithoutBlockingLocalUnload()
            throws Exception {
        FakeConnection connection = new FakeConnection();
        connection.responses.add(claimResponse(true));
        connection.responses.add(releaseAck(true, false));
        connection.responses.add(CompletableFuture.failedFuture(
                new java.io.IOException("confirmation lost")));
        connection.responses.add(receiptConfirmationAck(true));
        AtomicReference<Runnable> retry = new AtomicReference<>();
        ChunkClaimClient client = new ChunkClaimClient(
                connection, GROUP_ID, 1, retry::set);
        client.claimWithRetry(KEY).join();

        client.releaseWhenConfirmed(KEY, null).join();

        assertFalse(client.isClaimed(KEY),
                "正释放 ACK 已足够允许本地区块卸载，回执回收不得增加一个 RTT 屏障");
        assertTrue(connection.awaitRequests(3));
        ControlMessage release = connection.requests.get(1);
        ControlMessage firstConfirmation = connection.requests.get(2);
        assertEquals(MessageType.CHUNK_RELEASE_RECEIPT_CONFIRM,
                firstConfirmation.type());
        assertEquals(release.json().get("releaseId").getAsString(),
                firstConfirmation.json().get("releaseId").getAsString());

        Runnable next = awaitRetry(retry);
        retry.set(null);
        next.run();
        assertTrue(connection.awaitRequests(4));
        assertEquals(MessageType.CHUNK_RELEASE_RECEIPT_CONFIRM,
                connection.requests.get(3).type());
    }

    @Test
    void confirmationSchedulerFailureCannotReclosePositiveReleaseBarrier()
            throws Exception {
        FakeConnection connection = new FakeConnection();
        connection.responses.add(claimResponse(true));
        connection.responses.add(releaseAck(true, false));
        connection.throwOnConfirmationRequest = true;
        ChunkClaimClient client = new ChunkClaimClient(
                connection, GROUP_ID, 1,
                ignored -> {
                    throw new java.util.concurrent.RejectedExecutionException(
                            "scheduler stopped");
                });
        client.claimWithRetry(KEY).join();

        client.releaseWhenConfirmed(KEY, null).get(5, TimeUnit.SECONDS);

        assertFalse(client.isClaimed(KEY));
    }

    private static CompletableFuture<ControlMessage> releaseAck(
            boolean released, boolean finalDataWritten) {
        JsonObject json = new JsonObject();
        json.addProperty("released", released);
        json.addProperty("finalDataWritten", finalDataWritten);
        return CompletableFuture.completedFuture(
                ControlMessage.of(MessageType.CHUNK_RELEASE_ACK, json));
    }

    private static CompletableFuture<ControlMessage> receiptConfirmationAck(
            boolean accepted) {
        JsonObject json = new JsonObject();
        json.addProperty("accepted", accepted);
        return CompletableFuture.completedFuture(ControlMessage.of(
                MessageType.CHUNK_RELEASE_RECEIPT_CONFIRM, json));
    }

    private static Runnable awaitRetry(AtomicReference<Runnable> retry) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (retry.get() == null && System.nanoTime() < deadline) {
            Thread.sleep(5);
        }
        Runnable task = retry.get();
        assertNotNull(task, "失败释放必须安排下一次重试");
        return task;
    }

    private static CompletableFuture<ControlMessage> claimResponse(boolean hasServerData) {
        JsonObject json = new JsonObject();
        json.addProperty("granted", true);
        json.addProperty("hasServerData", hasServerData);
        return CompletableFuture.completedFuture(
                ControlMessage.of(MessageType.CHUNK_CLAIM_RESPONSE, json));
    }

    private static final class FakeConnection implements ControlConnection {
        private final Queue<CompletableFuture<ControlMessage>> responses =
                new ArrayDeque<>();
        private final List<ControlMessage> requests = new ArrayList<>();
        private boolean throwOnConfirmationRequest;

        @Override
        public void send(ControlMessage message) {
        }

        @Override
        public CompletableFuture<ControlMessage> request(ControlMessage message) {
            // ControlConnection 的真实实现总是在收到网络应答后异步完成。这里也强制异步，
            // 避免已完成 future 在 claimWithRetry 的 computeIfAbsent 内同步回调。
            synchronized (this) {
                requests.add(message);
                notifyAll();
            }
            if (message.type() == MessageType.CHUNK_RELEASE_RECEIPT_CONFIRM
                    && throwOnConfirmationRequest) {
                throw new IllegalStateException("confirmation send rejected");
            }
            if (message.type() == MessageType.CHUNK_RELEASE_RECEIPT_CONFIRM
                    && responses.isEmpty()) {
                return receiptConfirmationAck(true);
            }
            return responses.remove().thenApplyAsync(
                    response -> response,
                    CompletableFuture.delayedExecutor(10, TimeUnit.MILLISECONDS));
        }

        private synchronized boolean awaitRequests(int expected) throws InterruptedException {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (requests.size() < expected) {
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
