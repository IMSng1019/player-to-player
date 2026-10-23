package imsng.player_to_player.runtime;

import com.google.gson.JsonObject;
import imsng.player_to_player.netproto.*;
import imsng.player_to_player.util.JsonUtil;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Function;

/**
 * 仅用于父子进程的异步 RPC。停服保存/预同步可能超过远端控制协议的 15 秒默认超时，
 * 因此使用独立序号；连接断开会统一异常完成所有等待，不把超时误当成 COMMIT/ABORT。
 */
public final class WorkerRpc {
    private final AtomicLong sequence = new AtomicLong();
    private final Map<Long, CompletableFuture<ControlMessage>> pending = new ConcurrentHashMap<>();
    private final Function<ControlMessage, CompletionStage<ControlMessage>> handler;
    private final BiConsumer<ControlConnection, ControlMessage> events;
    private volatile ControlConnection connection;

    public WorkerRpc(Function<ControlMessage, CompletionStage<ControlMessage>> handler,
                     BiConsumer<ControlConnection, ControlMessage> events) {
        this.handler = handler;
        this.events = events;
    }
    public void register(HandlerRegistry registry) {
        registry.on(MessageType.WORKER_CONTROL, this::receive);
        registry.on(MessageType.WORKER_EVENT, this::event);
    }
    public synchronized void attach(ControlConnection conn) { connection = conn; }
    public ControlConnection connection() { return connection; }
    public CompletableFuture<ControlMessage> request(String op, JsonObject json) {
        JsonObject payload = json == null ? new JsonObject() : json.deepCopy();
        payload.addProperty("op", op);
        return request(ControlMessage.of(MessageType.WORKER_CONTROL, payload));
    }
    public synchronized CompletableFuture<ControlMessage> request(ControlMessage message) {
        ControlConnection conn = connection;
        if (conn == null || !conn.isOpen()) return CompletableFuture.failedFuture(new IOException("计算进程IPC已断开"));
        long id = sequence.incrementAndGet();
        JsonObject json = message.json().deepCopy();
        json.addProperty("_workerRid", id);
        var future = new CompletableFuture<ControlMessage>();
        pending.put(id, future);
        try { conn.send(ControlMessage.of(MessageType.WORKER_CONTROL, json, message.binary())); }
        catch (RuntimeException error) { pending.remove(id); future.completeExceptionally(error); }
        return future;
    }
    public void emit(String op, JsonObject json, byte[] binary) {
        ControlConnection conn = connection;
        if (conn == null || !conn.isOpen()) throw new IllegalStateException("计算进程IPC已断开");
        JsonObject payload = json == null ? new JsonObject() : json.deepCopy();
        payload.addProperty("op", op);
        conn.send(ControlMessage.of(MessageType.WORKER_EVENT, payload, binary));
    }
    private void receive(ControlConnection conn, ControlMessage message) {
        if (conn != connection) { conn.close(); return; }
        long id = message.json().get("_workerRid").getAsLong();
        try {
            handler.apply(message).whenComplete((result, error) -> reply(conn, id, result, error));
        } catch (Throwable error) { reply(conn, id, null, error); }
    }
    private void reply(ControlConnection conn, long id, ControlMessage result, Throwable error) {
        JsonObject json = result != null ? result.json().deepCopy() : new JsonObject();
        json.addProperty("_workerReply", id);
        if (error != null) json.addProperty("workerError", error.toString());
        conn.send(ControlMessage.of(MessageType.WORKER_EVENT, json,
                result != null ? result.binary() : ControlMessage.NO_BINARY));
    }
    private void event(ControlConnection conn, ControlMessage message) {
        if (conn != connection) { conn.close(); return; }
        if (message.json().has("_workerReply")) {
            var future = pending.remove(message.json().get("_workerReply").getAsLong());
            if (future == null) return;
            String error = JsonUtil.getString(message.json(), "workerError", "");
            if (error.isEmpty()) future.complete(message);
            else future.completeExceptionally(new IOException(error));
        } else events.accept(conn, message);
    }
    public synchronized void disconnected() {
        connection = null;
        IOException error = new IOException("计算进程IPC连接已断开，未完成操作不能视为成功");
        pending.forEach((id, future) -> future.completeExceptionally(error));
        pending.clear();
    }
    public static ControlMessage ok() { return ControlMessage.of(MessageType.WORKER_EVENT); }
}
