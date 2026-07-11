package imsng.player_to_player.p2p;

import imsng.player_to_player.util.ThreadPools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 可靠有序字节流（ARQ），运行在 {@link P2PTransport} 之上（Phase 2，DESIGN.md 第 7 节）。
 * <p>
 * 背景：副客户端经 P2P 隧道加入主客户端时，隧道里跑的是 MC 原版游戏协议 ——
 * 它假定 TCP 的字节流语义（可靠、有序、无重复）。而 {@link P2PChannel} 是加密 UDP
 * 数据报（可能丢包/乱序/重复），{@link RelayClient} 虽然底层 TCP 可靠，但为了让上层
 * 对"直连/中转"完全透明，两种传输统一套本层（TCP 路径上本层零重传，只多一份 ACK 开销）。
 *
 * <h2>线上帧格式（P2PTransport 载荷内）</h2>
 * <pre>
 *   byte  type   1=DATA 2=ACK 3=FIN
 *   DATA: int32 seq, byte[] payload（≤ {@value #CHUNK_BYTES} 字节）
 *   FIN : int32 seq（参与排序的流结束标记，无载荷）
 *   ACK : int32 cumAck（接收方下一个期望的 seq，累计确认）
 * </pre>
 * 帧尺寸取 {@value #CHUNK_BYTES}：低于常见路径 MTU（约 1500 减去 IP/UDP/加密开销），
 * 避免 IP 分片放大丢包率。
 *
 * <h2>协议要点</h2>
 * <ul>
 *   <li><b>发送窗口</b> {@value #SEND_WINDOW} 帧：窗口满时 {@link #outputStream()}
 *       的 write 阻塞（对隧道 pump 线程形成天然反压）；</li>
 *   <li><b>重传</b>：定时器检查最旧未确认帧，超 RTO 只重传该帧（接收方缓存乱序帧，
 *       累计 ACK 会在补洞后一次性前跳），RTO 指数退避、有进展即复位；</li>
 *   <li><b>接收侧流控</b>：已交付未被读走的字节超过 {@value #MAX_BUFFERED_BYTES}
 *       时暂停交付（也就不推进累计 ACK），发送方自然被窗口限速 ——
 *       消费慢不会导致内存无界增长；</li>
 *   <li><b>结束</b>：FIN 作为带序号的帧可靠传递，对端读尽数据后 read() 返回 -1；</li>
 *   <li><b>所有权</b>：本类接管 transport（设置 receiver、close 时关闭底层）。</li>
 * </ul>
 *
 * <h2>线程模型</h2>
 * OutputStream 假定<b>单写者</b>（隧道 pump 线程）；InputStream 假定单读者。
 * 入站处理在 transport 的接收线程上（契约：不阻塞 —— 本层入站只做 map 操作与
 * 一次 ACK 发送）。重传定时器在 {@link ThreadPools#scheduler()} 上。
 */
public final class ReliableChannel implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger("player_to_player/reliable");

    /** 单帧载荷上限（避免 UDP 路径 IP 分片；亦远低于 RelayClient 的 60000 上限）。 */
    public static final int CHUNK_BYTES = 1200;
    /** 发送窗口（帧数）：256 × 1.2KB ≈ 300KB 在途，家用 RTT 下带宽足够 MC 协议。 */
    private static final int SEND_WINDOW = 256;
    /** 接收乱序缓存上限（帧数）：大于发送窗口，容忍重传与乱序交织。 */
    private static final int RECV_WINDOW = 512;
    /** 已交付未读走字节的上限（接收侧流控阈值）。 */
    private static final int MAX_BUFFERED_BYTES = 4 * 1024 * 1024;
    /** 重传检查周期（毫秒）。 */
    private static final long RTO_CHECK_MILLIS = 50;
    /** 初始 RTO（毫秒）与上限。 */
    private static final long RTO_INITIAL_MILLIS = 400;
    private static final long RTO_MAX_MILLIS = 4_000;
    /** close() 等待对端确认尾部数据（含 FIN）的最长时间（毫秒）。 */
    private static final long CLOSE_LINGER_MILLIS = 3_000;

    private static final byte TYPE_DATA = 1;
    private static final byte TYPE_ACK = 2;
    private static final byte TYPE_FIN = 3;

    private final P2PTransport transport;
    private final String label;

    // ------------------------------------------------------------ 发送侧状态
    /** 发送侧锁：窗口推进用 wait/notify。 */
    private final Object sendLock = new Object();
    /** 未确认帧：seq → 完整线上帧（重传直接重发）。并发跳表便于取最旧。 */
    private final ConcurrentSkipListMap<Integer, byte[]> unacked = new ConcurrentSkipListMap<>();
    /** 下一个待分配的发送序号（sendLock 内更新）。 */
    private int nextSeq;
    /** 已全部确认的最小未确认序号（= 对端 cumAck 的最大观测值）。 */
    private volatile int sendBase;
    /** 最近一次窗口推进/发送时刻与当前 RTO（重传判据）。 */
    private volatile long lastProgressAt = System.currentTimeMillis();
    private volatile long rtoMillis = RTO_INITIAL_MILLIS;
    /** 本端是否已发出 FIN（其后不允许再写）。 */
    private boolean finSent;

    // ------------------------------------------------------------ 接收侧状态
    /** 接收侧锁：保护 expected/乱序缓存/交付队列。 */
    private final Object recvLock = new Object();
    /** 下一个期望的入站序号（= 我方累计 ACK 值）。 */
    private int expectedSeq;
    /** 乱序缓存：seq → 载荷（FIN 以 null 载荷表示）。 */
    private final TreeMap<Integer, byte[]> outOfOrder = new TreeMap<>();
    /** 已按序交付、等待 InputStream 读走的数据段。 */
    private final ArrayDeque<byte[]> delivered = new ArrayDeque<>();
    /** delivered 中的总字节数（流控判据）。 */
    private int deliveredBytes;
    /** 当前读段内的偏移（InputStream 局部游标）。 */
    private int readOffset;
    /** 对端 FIN 已按序到达（读尽 delivered 后 read() 返回 -1）。 */
    private boolean eofReached;

    private final AtomicBoolean open = new AtomicBoolean(true);
    /** 传输层异常死亡时的原因（读写方以 IOException 感知）。 */
    private volatile IOException deathCause;

    private final ScheduledFuture<?> retransmitTask;
    private final ChannelInput input = new ChannelInput();
    private final ChannelOutput output = new ChannelOutput();

    /**
     * @param transport 底层传输（本类接管所有权：设置 receiver、close 时关闭）
     * @param label     日志标签（会话 ID 等）
     */
    public ReliableChannel(P2PTransport transport, String label) {
        this.transport = transport;
        this.label = label;
        transport.setReceiver(this::onFrame);
        // 定时器只做轻量判断 + 至多一次 UDP 发送，符合 scheduler() 短任务约束
        this.retransmitTask = ThreadPools.scheduler().scheduleWithFixedDelay(
                this::checkRetransmit, RTO_CHECK_MILLIS, RTO_CHECK_MILLIS, TimeUnit.MILLISECONDS);
    }

    /** 有序可靠输入流（单读者；对端 FIN 且读尽后返回 -1）。 */
    public InputStream inputStream() {
        return input;
    }

    /** 有序可靠输出流（单写者；窗口满时 write 阻塞形成反压）。 */
    public OutputStream outputStream() {
        return output;
    }

    /** 通道是否仍可用（底层死亡或本端 close 后为 false）。 */
    public boolean isOpen() {
        return open.get() && transport.isOpen();
    }

    /**
     * 优雅关闭：发送 FIN 并等待尾部数据确认（至多 {@value #CLOSE_LINGER_MILLIS} ms），
     * 之后关闭底层传输。幂等；任何线程可调。
     */
    @Override
    public void close() {
        if (!open.compareAndSet(true, false)) {
            return;
        }
        try {
            synchronized (sendLock) {
                if (!finSent && transport.isOpen()) {
                    finSent = true;
                    sendFrameLocked(TYPE_FIN, nextSeq++, null, 0, 0);
                }
            }
            // linger：等对端把尾部（含 FIN）全部确认，尽量不丢关闭前的最后数据
            long deadline = System.currentTimeMillis() + CLOSE_LINGER_MILLIS;
            synchronized (sendLock) {
                while (!unacked.isEmpty() && transport.isOpen()
                        && System.currentTimeMillis() < deadline) {
                    sendLock.wait(Math.max(1, deadline - System.currentTimeMillis()));
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            retransmitTask.cancel(false);
            transport.close();
            wakeAll();
            LOGGER.info("可靠通道关闭: {}", label);
        }
    }

    /** 底层死亡/致命错误时的关闭：读写方以 IOException 感知。 */
    private void closeOnError(IOException cause) {
        deathCause = cause;
        if (open.compareAndSet(true, false)) {
            retransmitTask.cancel(false);
            transport.close();
            LOGGER.warn("可靠通道异常关闭: {} — {}", label, cause.toString());
        }
        wakeAll();
    }

    /** 唤醒所有等待中的读/写线程。 */
    private void wakeAll() {
        synchronized (sendLock) {
            sendLock.notifyAll();
        }
        synchronized (recvLock) {
            recvLock.notifyAll();
        }
    }

    // ---------------------------------------------------------------- 发送侧

    /**
     * 发送一段数据（sendLock 内调用）：组帧、登记未确认表、发出。
     * type=FIN 时 payload 为 null。
     */
    private void sendFrameLocked(byte type, int seq, byte[] payload, int off, int len) {
        ByteBuffer buf = ByteBuffer.allocate(1 + 4 + (payload != null ? len : 0));
        buf.put(type).putInt(seq);
        if (payload != null) {
            buf.put(payload, off, len);
        }
        byte[] frame = buf.array();
        unacked.put(seq, frame);
        transport.send(frame);
    }

    /** 写入器：把字节流切成 ≤CHUNK_BYTES 的 DATA 帧，窗口满则阻塞。 */
    private void writeChunk(byte[] data, int off, int len) throws IOException {
        synchronized (sendLock) {
            if (finSent) {
                throw new IOException("通道已发送 FIN，禁止再写: " + label);
            }
            while (open.get() && nextSeq - sendBase >= SEND_WINDOW) {
                try {
                    sendLock.wait(1_000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IOException("写入等待窗口时被中断", e);
                }
            }
            ensureUsable();
            sendFrameLocked(TYPE_DATA, nextSeq++, data, off, len);
        }
    }

    /** 重传检查（scheduler 线程）：最旧未确认帧滞留超 RTO 则重传它并退避。 */
    private void checkRetransmit() {
        if (!open.get()) {
            return;
        }
        Map.Entry<Integer, byte[]> oldest = unacked.firstEntry();
        if (oldest == null) {
            return;
        }
        long now = System.currentTimeMillis();
        if (now - lastProgressAt < rtoMillis) {
            return;
        }
        // 只重传最旧的一帧：接收方缓存乱序帧，补上洞后累计 ACK 一次性前跳
        transport.send(oldest.getValue());
        lastProgressAt = now;
        rtoMillis = Math.min(rtoMillis * 2, RTO_MAX_MILLIS);
        LOGGER.debug("通道 {} 重传 seq={} (rto→{}ms)", label, oldest.getKey(), rtoMillis);
    }

    /** 处理入站 ACK：丢弃已确认帧、推进窗口、唤醒写者。 */
    private void onAck(int cumAck) {
        if (cumAck <= sendBase) {
            return; // 迟到/重复 ACK
        }
        unacked.headMap(cumAck).clear();
        synchronized (sendLock) {
            if (cumAck > sendBase) {
                sendBase = cumAck;
                lastProgressAt = System.currentTimeMillis();
                rtoMillis = RTO_INITIAL_MILLIS; // 有进展即复位退避
                sendLock.notifyAll();
            }
        }
    }

    // ---------------------------------------------------------------- 接收侧

    /** 入站帧分发（transport 接收线程；契约：不阻塞）。 */
    private void onFrame(byte[] frame) {
        if (frame.length < 5) {
            return; // 畸形帧（入站不可信）：静默丢弃
        }
        ByteBuffer buf = ByteBuffer.wrap(frame);
        byte type = buf.get();
        int number = buf.getInt();
        switch (type) {
            case TYPE_ACK -> onAck(number);
            case TYPE_DATA -> {
                byte[] payload = new byte[buf.remaining()];
                buf.get(payload);
                onData(number, payload);
            }
            case TYPE_FIN -> onData(number, null);
            default -> {
                // 未知帧类型：容忍（未来扩展）
            }
        }
    }

    /** 处理 DATA/FIN（payload=null 表示 FIN）：入乱序缓存、按序交付、回 ACK。 */
    private void onData(int seq, byte[] payload) {
        synchronized (recvLock) {
            if (seq >= expectedSeq && seq < expectedSeq + RECV_WINDOW
                    && !outOfOrder.containsKey(seq)) {
                outOfOrder.put(seq, payload);
            }
            drainInOrderLocked();
            // 无论新旧帧都回当前累计 ACK：迟到帧的 ACK 告知对端窗口已前移
            sendAck(expectedSeq);
            recvLock.notifyAll();
        }
    }

    /**
     * 按序交付（recvLock 内）：把 expectedSeq 起连续的帧移入交付队列。
     * 流控：交付将超出 {@value #MAX_BUFFERED_BYTES} 时停手 —— 不交付即不推进
     * 累计 ACK，发送方被窗口自然限速；读者读走数据后会再次调用本方法。
     */
    private void drainInOrderLocked() {
        while (true) {
            if (!outOfOrder.containsKey(expectedSeq)) {
                return;
            }
            byte[] payload = outOfOrder.get(expectedSeq);
            if (payload == null) {
                // FIN 按序到达：流正常结束
                outOfOrder.remove(expectedSeq);
                expectedSeq++;
                eofReached = true;
                continue;
            }
            if (deliveredBytes + payload.length > MAX_BUFFERED_BYTES) {
                return; // 流控：读者太慢，暂停交付（帧留在乱序缓存里）
            }
            outOfOrder.remove(expectedSeq);
            expectedSeq++;
            if (payload.length > 0) {
                delivered.addLast(payload);
                deliveredBytes += payload.length;
            }
        }
    }

    /** 发送累计 ACK（接收线程/读者线程均可）。 */
    private void sendAck(int cumAck) {
        if (!transport.isOpen()) {
            return;
        }
        try {
            transport.send(ByteBuffer.allocate(5).put(TYPE_ACK).putInt(cumAck).array());
        } catch (Exception e) {
            LOGGER.debug("通道 {} ACK 发送失败: {}", label, e.toString());
        }
    }

    /** 读写公共前置：通道死亡时抛出根因。 */
    private void ensureUsable() throws IOException {
        if (!open.get()) {
            IOException cause = deathCause;
            throw cause != null
                    ? new IOException("可靠通道已关闭: " + label, cause)
                    : new IOException("可靠通道已关闭: " + label);
        }
        if (!transport.isOpen()) {
            closeOnError(new IOException("底层传输已关闭"));
            throw new IOException("底层传输已关闭: " + label);
        }
    }

    // ------------------------------------------------------------ 流实现

    /** 输入流：从交付队列取数据；空则阻塞；EOF/死亡分别返回 -1/抛异常。 */
    private final class ChannelInput extends InputStream {

        @Override
        public int read() throws IOException {
            byte[] one = new byte[1];
            int n = read(one, 0, 1);
            return n < 0 ? -1 : one[0] & 0xFF;
        }

        @Override
        public int read(byte[] out, int off, int len) throws IOException {
            if (len == 0) {
                return 0;
            }
            synchronized (recvLock) {
                while (delivered.isEmpty()) {
                    if (eofReached && outOfOrder.isEmpty()) {
                        return -1; // 数据读尽且对端已 FIN
                    }
                    if (!open.get()) {
                        IOException cause = deathCause;
                        if (cause != null) {
                            throw new IOException("可靠通道已关闭", cause);
                        }
                        return -1; // 本端主动关闭：按流结束处理
                    }
                    try {
                        recvLock.wait(1_000);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new IOException("读取被中断", e);
                    }
                }
                byte[] head = delivered.peekFirst();
                int available = head.length - readOffset;
                int n = Math.min(available, len);
                System.arraycopy(head, readOffset, out, off, n);
                readOffset += n;
                if (readOffset == head.length) {
                    delivered.pollFirst();
                    readOffset = 0;
                }
                deliveredBytes -= n;
                // 读者腾出了空间：继续交付被流控挡住的帧，并把最新累计 ACK 告知对端
                int before = expectedSeq;
                drainInOrderLocked();
                if (expectedSeq != before) {
                    sendAck(expectedSeq);
                }
                return n;
            }
        }

        @Override
        public int available() {
            synchronized (recvLock) {
                return deliveredBytes;
            }
        }

        @Override
        public void close() {
            ReliableChannel.this.close();
        }
    }

    /** 输出流：切帧发送（单写者）。 */
    private final class ChannelOutput extends OutputStream {

        @Override
        public void write(int b) throws IOException {
            write(new byte[]{(byte) b}, 0, 1);
        }

        @Override
        public void write(byte[] data, int off, int len) throws IOException {
            ensureUsable();
            int cursor = off;
            int remaining = len;
            while (remaining > 0) {
                int n = Math.min(remaining, CHUNK_BYTES);
                writeChunk(data, cursor, n);
                cursor += n;
                remaining -= n;
            }
        }

        @Override
        public void close() {
            ReliableChannel.this.close();
        }
    }
}
