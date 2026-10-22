package imsng.player_to_player.p2p;

import com.google.gson.JsonObject;
import imsng.player_to_player.util.JsonUtil;
import imsng.player_to_player.util.ThreadPools;
import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ScheduledFuture;
import java.util.function.IntSupplier;

/**
 * 已经确认远端接待能力、但尚未接入 Minecraft 的隧道。准备、一次消费、关闭互斥；
 * 只在本机 MC 连入后创建远端 LAN socket，保留完整原版握手/模组登录生命周期。
 */
public final class PreparedTunnel implements AutoCloseable {
    public static final long LEASE_SECONDS = 360;
    private final ReliableChannel channel;
    private final String id;
    private final String label;
    private final ScheduledFuture<?> expiry;
    private boolean consumed;
    private boolean closed;
    private TcpTunnel.Listener listener;

    private PreparedTunnel(ReliableChannel channel, String id, String label) {
        this.channel = channel;
        this.id = id;
        this.label = label;
        expiry = ThreadPools.scheduler().schedule(this::close, LEASE_SECONDS, TimeUnit.SECONDS);
    }

    /** 专用阻塞线程调用；失败后必须关闭本通道。 */
    public static PreparedTunnel prepare(ReliableChannel channel, String transactionId) throws IOException {
        String id = transactionId != null ? transactionId : UUID.randomUUID().toString();
        try {
            TcpTunnel.writeHeader(channel, message("tunnel_prepare", id).toString());
            require(readBounded(channel, 15), "ready", id);
            return new PreparedTunnel(channel, id, "prepared:" + id);
        } catch (IOException | RuntimeException error) {
            channel.abort();
            throw error;
        }
    }

    /** 返回本机监听口，调用方随后在渲染线程走原版断开/连接。 */
    public synchronized int activate() throws IOException {
        if (closed || consumed || !channel.isOpen()) throw new IOException("预建隧道已失效或已消费");
        consumed = true;
        expiry.cancel(false);
        try {
            listener = TcpTunnel.listenPrepared(channel, label, () -> {
                TcpTunnel.writeHeader(channel, message("activate", id).toString());
                require(readBounded(channel, 15), "active", id);
            });
            return listener.port();
        } catch (IOException | RuntimeException error) {
            close();
            throw error;
        }
    }

    public synchronized boolean isOpen() { return !closed && channel.isOpen(); }

    @Override public synchronized void close() {
        if (closed) return;
        closed = true;
        expiry.cancel(false);
        if (listener != null) {
            try { listener.socket().close(); } catch (IOException ignored) { }
        }
        // 可靠 FIN 使远端等待 activate 的读线程立即退出并归还宿主名额。
        // 只关闭 UDP/中转本端资源不会通知对端；linger 放在专用线程避免阻塞渲染。
        ThreadPools.startBlocking("prepared-cancel", channel::close);
    }

    /** 宿主接待准备会话；调用方已保留名额，直至实际隧道关闭才释放。 */
    public static void accept(ReliableChannel channel, JsonObject header, IntSupplier port,
                              Runnable terminated) {
        boolean handedOff = false;
        try {
            String id = JsonUtil.getString(header, "transactionId", "");
            UUID.fromString(id);
            if (port.getAsInt() <= 0) throw new IOException("宿主 LAN 未就绪");
            TcpTunnel.writeHeader(channel, message("ready", id).toString());
            require(readBounded(channel, LEASE_SECONDS), "activate", id);
            int currentPort = port.getAsInt();
            if (currentPort <= 0) throw new IOException("宿主已停止接待");
            TcpTunnel.activateAndBridge(channel, currentPort, "prepared:" + id,
                    message("active", id).toString(), terminated);
            handedOff = true;
        } catch (Exception error) {
            channel.abort();
        } finally {
            if (!handedOff) TcpTunnel.Terminations.run(terminated);
        }
    }

    private static JsonObject message(String op, String id) {
        JsonObject message = new JsonObject();
        message.addProperty("op", op);
        message.addProperty("transactionId", id);
        return message;
    }

    private static void require(String raw, String op, String id) throws IOException {
        JsonObject message = JsonUtil.parseObject(raw);
        if (!op.equals(JsonUtil.getString(message, "op", ""))
                || !id.equals(JsonUtil.getString(message, "transactionId", "")))
            throw new IOException("预建隧道确认与事务不匹配");
    }

    private static String readBounded(ReliableChannel channel, long seconds) throws IOException {
        ScheduledFuture<?> timeout = ThreadPools.scheduler().schedule(channel::abort, seconds, TimeUnit.SECONDS);
        try { return TcpTunnel.readHeader(channel); }
        finally { timeout.cancel(false); }
    }
}
