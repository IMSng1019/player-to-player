package imsng.player_to_player.p2p;

import imsng.player_to_player.util.JsonUtil;
import org.junit.jupiter.api.Test;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.InetAddress;
import java.net.SocketTimeoutException;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import static org.junit.jupiter.api.Assertions.*;

class PreparedTunnelTest {
    @Test
    void opensRemoteLanOnlyAfterLocalMinecraftConnectsAndBridgesUnmodifiedBytes() throws Exception {
        try (Pair pair = new Pair(); ServerSocket lan = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
            AtomicInteger terminated = new AtomicInteger();
            CompletableFuture<Void> host = CompletableFuture.runAsync(() -> {
                try {
                    PreparedTunnel.accept(pair.host, JsonUtil.parseObject(TcpTunnel.readHeader(pair.host)),
                            lan::getLocalPort, terminated::incrementAndGet);
                } catch (Exception error) { throw new CompletionException(error); }
            });
            try (PreparedTunnel prepared = PreparedTunnel.prepare(pair.client, UUID.randomUUID().toString())) {
                lan.setSoTimeout(150);
                assertThrows(SocketTimeoutException.class, lan::accept, "准备不能提前占用MC登录连接");
                int port = prepared.activate();
                assertThrows(java.io.IOException.class, prepared::activate, "只能消费一次");
                assertThrows(SocketTimeoutException.class, lan::accept, "仅创建本机监听仍不连接LAN");
                try (Socket local = new Socket(InetAddress.getLoopbackAddress(), port)) {
                    lan.setSoTimeout(5000);
                    try (Socket remote = lan.accept()) {
                        local.setSoTimeout(5000);
                        remote.setSoTimeout(5000);
                        local.getOutputStream().write(new byte[]{1, 2, 3});
                        assertArrayEquals(new byte[]{1, 2, 3}, remote.getInputStream().readNBytes(3));
                        remote.getOutputStream().write(7);
                        assertEquals(7, local.getInputStream().read());
                    }
                }
            }
            host.get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void cancelledPreparationCannotActivate() throws Exception {
        try (Pair pair = new Pair()) {
            var terminated = new CompletableFuture<Void>();
            var host = CompletableFuture.runAsync(() -> {
                try {
                    var header = JsonUtil.parseObject(TcpTunnel.readHeader(pair.host));
                    PreparedTunnel.accept(pair.host, header, () -> 12345,
                            () -> terminated.complete(null));
                } catch (Exception error) { throw new CompletionException(error); }
            });
            PreparedTunnel prepared = PreparedTunnel.prepare(pair.client, UUID.randomUUID().toString());
            pair.a.dropFirstFin = true; // UDP 丢首个关闭帧时也须释放远端接待名额。
            prepared.close();
            assertFalse(prepared.isOpen());
            assertThrows(java.io.IOException.class, prepared::activate);
            terminated.get(5, TimeUnit.SECONDS);
            host.get(5, TimeUnit.SECONDS);
        }
    }

    /** 异步内存传输模拟真实双向接收，避免同线程递归 ACK 掩盖锁顺序错误。 */
    private static final class Pair implements AutoCloseable {
        final Wire a = new Wire(), b = new Wire();
        final ReliableChannel client, host;
        Pair() {
            a.peer = b; b.peer = a;
            client = new ReliableChannel(a, "test-client");
            host = new ReliableChannel(b, "test-host");
        }
        public void close() { client.abort(); host.abort(); a.worker.shutdownNow(); b.worker.shutdownNow(); }
    }
    private static final class Wire implements P2PTransport {
        final ExecutorService worker = Executors.newSingleThreadExecutor();
        volatile Consumer<byte[]> receiver;
        volatile boolean open = true;
        volatile boolean dropFirstFin;
        Wire peer;
        public void send(byte[] data) {
            if (dropFirstFin && data.length >= 5 && data[0] == 3) {
                dropFirstFin = false;
                return;
            }
            if (open && peer.open) peer.worker.execute(() -> {
                Consumer<byte[]> target = peer.receiver;
                if (target != null && peer.open) target.accept(data);
            });
        }
        public void setReceiver(Consumer<byte[]> receiver) { this.receiver = receiver; }
        public boolean isOpen() { return open; }
        public void close() { open = false; }
    }
}
