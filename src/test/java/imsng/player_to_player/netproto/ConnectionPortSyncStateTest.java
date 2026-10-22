package imsng.player_to_player.netproto;

import org.junit.jupiter.api.Test;

import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/** 使用真实状态类复现 Netty 收包领先于 render JOIN，以及快速切服的迟到事件。 */
class ConnectionPortSyncStateTest {
    @Test
    void retainsPacketReceivedBetweenInitAndJoin() {
        ConnectionPortSyncState state = new ConnectionPortSyncState();
        Object connection = new Object();
        state.beginConnection(connection);
        assertTrue(state.receive(connection, 30123, 30124));

        ConnectionPortSyncState.Join join = state.beginJoin();

        assertEquals(new ConnectionPortSyncState.Ports(30123, 30124), state.await(join, 0, 0));
    }

    @Test
    void receivesPacketAfterJoin() {
        ConnectionPortSyncState state = new ConnectionPortSyncState();
        Object connection = new Object();
        state.beginConnection(connection);
        ConnectionPortSyncState.Join join = state.beginJoin();

        assertTrue(state.receive(connection, 31000, 0));

        assertEquals(new ConnectionPortSyncState.Ports(31000, 0), state.await(join, 0, 0));
    }

    @Test
    void rejectsOldPacketAndDisconnectAfterNewConnectionStarts() {
        ConnectionPortSyncState state = new ConnectionPortSyncState();
        Object oldConnection = new Object();
        Object newConnection = new Object();
        state.beginConnection(oldConnection);
        ConnectionPortSyncState.Join oldJoin = state.beginJoin();
        state.receive(oldConnection, 30001, 0);
        state.beginConnection(newConnection);
        ConnectionPortSyncState.Join newJoin = state.beginJoin();
        state.receive(newConnection, 30002, 0);

        assertFalse(state.receive(oldConnection, 30001, 0));
        state.advertise(oldConnection, false);
        state.disconnect(oldConnection);

        assertNull(state.await(oldJoin, 0, 0));
        assertEquals(new ConnectionPortSyncState.Ports(30002, 0), state.await(newJoin, 0, 0));
    }

    @Test
    void disconnectWakesWaitingJoinImmediately() throws Exception {
        ConnectionPortSyncState state = new ConnectionPortSyncState();
        Object connection = new Object();
        state.beginConnection(connection);
        ConnectionPortSyncState.Join join = state.beginJoin();
        FutureTask<ConnectionPortSyncState.Ports> waiting = waitInBackground(state, join);

        state.disconnect(connection);

        assertNull(waiting.get(1, TimeUnit.SECONDS));
        assertFalse(state.receive(connection, 30001, 0));
    }

    @Test
    void newConnectionCancelsOldJoinWithoutWaitingForTimeout() throws Exception {
        ConnectionPortSyncState state = new ConnectionPortSyncState();
        state.beginConnection(new Object());
        ConnectionPortSyncState.Join oldJoin = state.beginJoin();
        FutureTask<ConnectionPortSyncState.Ports> waiting = waitInBackground(state, oldJoin);

        state.beginConnection(new Object());

        assertNull(waiting.get(1, TimeUnit.SECONDS));
    }

    @Test
    void unsupportedServerFallsBackImmediately() {
        ConnectionPortSyncState state = new ConnectionPortSyncState();
        Object connection = new Object();
        state.beginConnection(connection);
        ConnectionPortSyncState.Join join = state.beginJoin();
        state.advertise(connection, false);

        assertTimeoutPreemptively(java.time.Duration.ofSeconds(1),
                () -> assertNull(state.await(join, 10_000, 10_000)));
    }

    @Test
    void invalidPortsNeverCompleteJoin() {
        ConnectionPortSyncState state = new ConnectionPortSyncState();
        Object connection = new Object();
        state.beginConnection(connection);
        ConnectionPortSyncState.Join join = state.beginJoin();

        assertFalse(state.receive(connection, 0, 0));
        assertFalse(state.receive(connection, 65536, 0));
        assertFalse(state.receive(connection, 30000, -1));
        assertFalse(state.receive(connection, 30000, 65536));
        assertNull(state.await(join, 0, 0));
    }

    private static FutureTask<ConnectionPortSyncState.Ports> waitInBackground(
            ConnectionPortSyncState state, ConnectionPortSyncState.Join join) {
        FutureTask<ConnectionPortSyncState.Ports> waiting =
                new FutureTask<>(() -> state.await(join, 10_000, 10_000));
        Thread worker = new Thread(waiting, "port-sync-regression");
        worker.setDaemon(true);
        worker.start();
        return waiting;
    }
}
