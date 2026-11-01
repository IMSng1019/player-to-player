package imsng.player_to_player.runtime;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import imsng.player_to_player.group.PlayerDirectory;
import imsng.player_to_player.netproto.*;
import org.junit.jupiter.api.Test;

import java.net.SocketAddress;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/** 旧控制连接的解析已开始但尚未提交时，换服不能被它回填旧成员身份。 */
class WorkerPermissionsSessionTest {
    @Test void oldPermissionPushCannotCommitAfterNewControlSessionReset() throws Exception {
        ControlClient oldClient = new ControlClient("127.0.0.1", 1);
        ControlClient newClient = new ControlClient("127.0.0.1", 1);
        var oldConnection = connection();
        WorkerProcess.registerForwarders(oldClient, oldConnection);
        var handlers = ControlClient.class.getDeclaredField("handlers"); handlers.setAccessible(true);
        @SuppressWarnings("unchecked") var handler = ((Map<MessageType, MessageHandler>) handlers.get(oldClient))
                .get(MessageType.PERMISSION_RELAY);
        var lockField = WorkerProcess.class.getDeclaredField("MEMBER_LOCK"); lockField.setAccessible(true);
        var cached = WorkerProcess.class.getDeclaredField("permissions"); cached.setAccessible(true);
        JsonObject frame = new JsonObject(); frame.addProperty("groupId", UUID.randomUUID().toString());
        JsonArray rows = new JsonArray(); JsonObject player = new JsonObject();
        player.addProperty("uuid", UUID.randomUUID().toString()); player.addProperty("name", "OldWorldPlayer");
        rows.add(player); frame.add("members", rows); frame.add("onlinePlayers", rows.deepCopy());
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread delivery = new Thread(() -> {
            try { handler.handle(oldConnection, ControlMessage.of(MessageType.PERMISSION_RELAY, frame)); }
            catch (Throwable error) { failure.set(error); }
        }, "old-permission-push");
        try {
            synchronized (lockField.get(null)) {
                delivery.start();
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                while (delivery.getState() != Thread.State.BLOCKED && delivery.isAlive()
                        && System.nanoTime() < deadline) Thread.sleep(1);
                assertEquals(Thread.State.BLOCKED, delivery.getState());
                WorkerProcess.registerForwarders(newClient, connection());
            }
            delivery.join(5000);
            assertFalse(delivery.isAlive());
            assertNull(failure.get());
            assertNull(cached.get(null), "旧帧不得覆盖新控制会话的空授权缓存");
            assertEquals(List.of(), PlayerDirectory.SESSION.namesIncluding(List.of()));
        } finally {
            delivery.join(5000);
            oldClient.close(); newClient.close();
            WorkerProcess.registerForwarders(new ControlClient("127.0.0.1", 1), connection());
        }
    }

    private static ControlConnection connection() {
        return new ControlConnection() {
            public void send(ControlMessage message) { }
            public CompletableFuture<ControlMessage> request(ControlMessage message) { return new CompletableFuture<>(); }
            public SocketAddress remoteAddress() { return null; }
            public UUID peerId() { return null; }
            public void setPeerId(UUID player) { }
            public boolean isOpen() { return true; }
            public void close() { }
        };
    }
}
