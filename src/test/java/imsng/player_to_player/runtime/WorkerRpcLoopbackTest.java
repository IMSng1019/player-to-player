package imsng.player_to_player.runtime;

import com.google.gson.JsonObject;
import imsng.player_to_player.netproto.*;
import org.junit.jupiter.api.Test;
import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.*;
import static org.junit.jupiter.api.Assertions.*;

/** 真实回环连接验证双向RPC序号、二进制数据和断连失败，而非仅验证模拟调用次数。 */
class WorkerRpcLoopbackTest {
    @Test void oppositeDirectionsKeepIndependentRequestsAndBinary() throws Exception {
        ControlServer server = new ControlServer("127.0.0.1", 0);
        WorkerRpc parent = new WorkerRpc(request -> CompletableFuture.completedFuture(
                ControlMessage.of(MessageType.WORKER_EVENT, request.json(), request.binary())), (c,m) -> { });
        parent.register(server);
        server.on(MessageType.HELLO, (connection, hello) -> {
            connection.setPeerId(UUID.randomUUID()); parent.attach(connection);
            connection.send(hello.reply(MessageType.HELLO_ACK, new JsonObject(), null));
        });
        server.onDisconnect(connection -> parent.disconnected());
        server.start();
        ControlClient client = new ControlClient("127.0.0.1", server.boundPort());
        WorkerRpc child = new WorkerRpc(request -> CompletableFuture.completedFuture(
                ControlMessage.of(MessageType.WORKER_EVENT, request.json(), request.binary())), (c,m) -> { });
        child.register(client); client.onDisconnect(connection -> child.disconnected());
        try {
            ControlConnection connection = client.connect(); child.attach(connection);
            connection.request(ControlMessage.of(MessageType.HELLO)).get(5, TimeUnit.SECONDS);
            JsonObject a = new JsonObject(); a.addProperty("op", "a");
            JsonObject b = new JsonObject(); b.addProperty("op", "b");
            var first = parent.request(ControlMessage.of(MessageType.WORKER_CONTROL, a, new byte[]{2,3}));
            var second = child.request(ControlMessage.of(MessageType.WORKER_CONTROL, b, new byte[]{5,7}));
            assertArrayEquals(new byte[]{2,3}, first.get(5, TimeUnit.SECONDS).binary());
            assertArrayEquals(new byte[]{5,7}, second.get(5, TimeUnit.SECONDS).binary());
        } finally { client.close(); server.stop(); }
    }

    @Test void lostWorkerCannotCompletePendingStopAsSuccess() throws Exception {
        WorkerRpc rpc = new WorkerRpc(request -> new CompletableFuture<>(), (c,m) -> { });
        rpc.attach(new ControlConnection() {
            public void send(ControlMessage message) { }
            public CompletableFuture<ControlMessage> request(ControlMessage message) { return new CompletableFuture<>(); }
            public java.net.SocketAddress remoteAddress() { return null; }
            public UUID peerId() { return null; }
            public void setPeerId(UUID peer) { }
            public boolean isOpen() { return true; }
            public void close() { }
        });
        var stop = rpc.request("stop", new JsonObject()); rpc.disconnected();
        ExecutionException error = assertThrows(ExecutionException.class, () -> stop.get(1, TimeUnit.SECONDS));
        assertInstanceOf(IOException.class, error.getCause());
    }
}
