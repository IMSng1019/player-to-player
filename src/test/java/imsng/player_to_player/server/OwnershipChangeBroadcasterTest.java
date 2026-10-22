package imsng.player_to_player.server;

import imsng.player_to_player.netproto.ControlConnection;
import imsng.player_to_player.netproto.ControlMessage;
import imsng.player_to_player.netproto.MessageType;
import org.junit.jupiter.api.Test;

import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;

class OwnershipChangeBroadcasterTest {
    @Test
    void manyChunkChangesProduceOneBatchPerCurrentPrimary() {
        List<Runnable> timers = new ArrayList<>();
        FakeConnection first = new FakeConnection();
        FakeConnection second = new FakeConnection();
        List<ControlConnection> recipients = new ArrayList<>(List.of(first));
        OwnershipChangeBroadcaster broadcaster =
                new OwnershipChangeBroadcaster(() -> List.copyOf(recipients), timers::add);
        UUID group = UUID.randomUUID();
        for (int i = 0; i < 100; i++) broadcaster.changed(group);
        broadcaster.changed(UUID.randomUUID());
        assertEquals(1, timers.size());
        assertTrue(first.sent.isEmpty());
        recipients.add(second);
        timers.get(0).run();
        assertEquals(1, first.sent.size());
        assertEquals(1, second.sent.size(), "广播时重新取当前在线主列表");
        assertEquals(MessageType.CHUNK_OWNERSHIP_CHANGED, first.sent.get(0).type());
        assertEquals(2, first.sent.get(0).json().getAsJsonArray("changedGroups").size());
    }

    @Test
    void closeDropsQueuedNotificationAndFutureChanges() {
        List<Runnable> timers = new ArrayList<>();
        FakeConnection connection = new FakeConnection();
        OwnershipChangeBroadcaster broadcaster =
                new OwnershipChangeBroadcaster(() -> List.of(connection), timers::add);
        broadcaster.changed(UUID.randomUUID());
        broadcaster.close();
        timers.get(0).run();
        broadcaster.changed(UUID.randomUUID());
        assertTrue(connection.sent.isEmpty());
        assertEquals(1, timers.size());
    }

    private static final class FakeConnection implements ControlConnection {
        private final List<ControlMessage> sent = new ArrayList<>();
        @Override public void send(ControlMessage message) { sent.add(message); }
        @Override public CompletableFuture<ControlMessage> request(ControlMessage message) {
            throw new UnsupportedOperationException();
        }
        @Override public SocketAddress remoteAddress() { return null; }
        @Override public UUID peerId() { return null; }
        @Override public void setPeerId(UUID peerId) { }
        @Override public boolean isOpen() { return true; }
        @Override public void close() { }
    }
}
