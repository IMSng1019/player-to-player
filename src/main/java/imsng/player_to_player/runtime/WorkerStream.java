package imsng.player_to_player.runtime;

import com.google.gson.JsonObject;
import imsng.player_to_player.group.PresyncChannel;
import imsng.player_to_player.netproto.*;
import java.io.*;
import java.util.Arrays;

/**
 * 预同步在子服运行，可靠 P2P 通道仍由父进程管理。每个 read 精确限定长度，无后台读泵，
 * 所以 TAIL_DONE 后的 tunnel_prepare 字节不会被 IPC 偷读；写入以 64KiB 有界分片反压。
 */
final class WorkerStream implements PresyncChannel {
    private final WorkerRpc rpc;
    private final String id;
    private volatile boolean open = true;
    WorkerStream(WorkerRpc rpc, String id) { this.rpc = rpc; this.id = id; }
    private JsonObject header(String op) {
        JsonObject json = new JsonObject(); json.addProperty("op", op); json.addProperty("streamId", id); return json;
    }
    private ControlMessage call(JsonObject json, byte[] bytes) throws IOException {
        if (!isOpen()) throw new IOException("预同步IPC流已关闭");
        try { return rpc.request(ControlMessage.of(MessageType.WORKER_CONTROL, json, bytes)).get(); }
        catch (Exception error) { throw new IOException("预同步IPC流失败", error); }
    }
    private final InputStream input = new InputStream() {
        @Override public int read() throws IOException {
            byte[] one = new byte[1]; int n = read(one, 0, 1); return n < 0 ? -1 : one[0] & 255;
        }
        @Override public int read(byte[] bytes, int offset, int length) throws IOException {
            if (length == 0) return 0;
            JsonObject json = header("stream_read"); json.addProperty("length", Math.min(length, 65536));
            ControlMessage reply = call(json, ControlMessage.NO_BINARY);
            if (reply.json().has("eof") && reply.json().get("eof").getAsBoolean()) return -1;
            byte[] data = reply.binary();
            if (data.length == 0 || data.length > length) throw new IOException("非法IPC流读取长度");
            System.arraycopy(data, 0, bytes, offset, data.length); return data.length;
        }
    };
    private final OutputStream output = new BufferedOutputStream(new OutputStream() {
        @Override public void write(int value) throws IOException { write(new byte[]{(byte) value}); }
        @Override public void write(byte[] bytes, int offset, int length) throws IOException {
            while (length > 0) {
                int n = Math.min(length, 65536);
                call(header("stream_write"), Arrays.copyOfRange(bytes, offset, offset + n));
                offset += n; length -= n;
            }
        }
    }, 65536);
    @Override public InputStream inputStream() { return input; }
    @Override public OutputStream outputStream() { return output; }
    @Override public boolean isOpen() { return open && rpc.connection() != null && rpc.connection().isOpen(); }
    @Override public void abort() {
        open = false;
        rpc.request(ControlMessage.of(MessageType.WORKER_CONTROL, header("stream_abort")));
    }
}
