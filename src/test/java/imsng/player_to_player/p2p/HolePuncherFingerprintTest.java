package imsng.player_to_player.p2p;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.security.KeyPair;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link HolePuncher} 指纹绑定单测（真实 loopback UDP，无 mock 网络）：
 * <ul>
 *   <li>指纹匹配 → 握手成功、通道可用；</li>
 *   <li>指纹不匹配（等价于中间人替换公钥）→ 必须抛 {@link IOException} 失败；</li>
 *   <li>期望指纹缺失 / 非法 → 按向后兼容降级为"不认证"握手；</li>
 *   <li>{@link P2PChannel} 的 responder 握手补偿（handshakeReply）不因指纹校验失效；</li>
 *   <li>双方真实打洞 + 相互校验指纹 → 加密数据可通。</li>
 * </ul>
 */
class HolePuncherFingerprintTest {

    private static final String SESSION = "test-session-0001";

    /** 假对端：收到非 KEY 包就回一个 KEY 包（携带指定密钥对的公钥），收到 KEY 包只计数不回包。 */
    private static final class FakeKeyPeer implements AutoCloseable {

        private final DatagramSocket socket;
        private final KeyPair replyKey;
        private final AtomicInteger keyPacketsReceived = new AtomicInteger();
        private volatile InetSocketAddress peerAddress;

        FakeKeyPeer(KeyPair replyKey) throws IOException {
            this.replyKey = replyKey;
            this.socket = loopbackSocket();
            Thread thread = new Thread(this::loop, "p2p-test-fake-peer");
            thread.setDaemon(true);
            thread.start();
        }

        private void loop() {
            byte[] buffer = new byte[2048];
            DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
            while (!socket.isClosed()) {
                try {
                    packet.setLength(buffer.length);
                    socket.receive(packet);
                } catch (IOException e) {
                    return; // socket 关闭：正常退出
                }
                peerAddress = (InetSocketAddress) packet.getSocketAddress();
                HolePuncher.Packet parsed = HolePuncher.parse(packet.getData(), packet.getLength());
                if (parsed != null && "KEY".equals(parsed.cmd())) {
                    // 只统计对端（responder 通道）的 KEY 包；不回包，避免两侧互相刷包
                    keyPacketsReceived.incrementAndGet();
                    continue;
                }
                sendKey();
            }
        }

        private void sendKey() {
            InetSocketAddress target = peerAddress;
            if (target == null) {
                return;
            }
            byte[] key = HolePuncher.keyPacket(SESSION, SessionCrypto.encodePublicKey(replyKey));
            try {
                socket.send(new DatagramPacket(key, key.length, target));
            } catch (IOException ignored) {
                // 测试内网不关心发送失败
            }
        }

        /** 模拟 initiator 的 KEY 包迟到重发（触发 responder 通道的握手补偿）。 */
        void resendKey() {
            sendKey();
        }

        boolean awaitKeyPackets(int expected, long timeoutMillis) throws InterruptedException {
            long deadline = System.currentTimeMillis() + timeoutMillis;
            while (System.currentTimeMillis() < deadline) {
                if (keyPacketsReceived.get() >= expected) {
                    return true;
                }
                Thread.sleep(10);
            }
            return keyPacketsReceived.get() >= expected;
        }

        InetSocketAddress localAddress() {
            return (InetSocketAddress) socket.getLocalSocketAddress();
        }

        @Override
        public void close() {
            socket.close();
        }
    }

    @Test
    void punchesThroughWhenPeerFingerprintMatches() throws Exception {
        KeyPair peerKey = SessionCrypto.generateKeyPair();
        try (FakeKeyPeer peer = new FakeKeyPeer(peerKey)) {
            DatagramSocket socket = loopbackSocket();
            P2PChannel channel = HolePuncher.punch(socket, peer.localAddress(), SESSION, true,
                    SessionCrypto.generateKeyPair(), KeyFingerprint.of(peerKey));
            try {
                assertTrue(channel.isOpen(), "指纹匹配时握手应成功");
            } finally {
                channel.close();
            }
        }
    }

    @Test
    void rejectsPeerKeyWhenFingerprintMismatches() throws Exception {
        KeyPair legitKey = SessionCrypto.generateKeyPair();
        KeyPair attackerKey = SessionCrypto.generateKeyPair(); // 等价于路径攻击者换上的公钥
        try (FakeKeyPeer peer = new FakeKeyPeer(attackerKey)) {
            DatagramSocket socket = loopbackSocket();
            IOException failure = assertThrows(IOException.class, () ->
                    HolePuncher.punch(socket, peer.localAddress(), SESSION, true,
                            SessionCrypto.generateKeyPair(), KeyFingerprint.of(legitKey)));
            assertTrue(failure.getMessage().contains("指纹不匹配"), failure.getMessage());
            socket.close(); // 失败路径由调用方回收 socket（与 P2PSessions 一致）
        }
    }

    @Test
    void degradesToLegacyHandshakeWhenExpectedFingerprintMissing() throws Exception {
        KeyPair peerKey = SessionCrypto.generateKeyPair();
        try (FakeKeyPeer peer = new FakeKeyPeer(peerKey)) {
            DatagramSocket socket = loopbackSocket();
            P2PChannel channel = HolePuncher.punch(socket, peer.localAddress(), SESSION, true,
                    SessionCrypto.generateKeyPair(), null);
            try {
                assertTrue(channel.isOpen(), "无期望指纹（旧服务端/降级）应保持旧行为可打通");
            } finally {
                channel.close();
            }
        }
    }

    @Test
    void degradesToLegacyHandshakeWhenExpectedFingerprintMalformed() throws Exception {
        KeyPair peerKey = SessionCrypto.generateKeyPair();
        try (FakeKeyPeer peer = new FakeKeyPeer(peerKey)) {
            DatagramSocket socket = loopbackSocket();
            P2PChannel channel = HolePuncher.punch(socket, peer.localAddress(), SESSION, true,
                    SessionCrypto.generateKeyPair(), "这不是一个合法指纹");
            try {
                assertTrue(channel.isOpen(), "非法期望指纹按降级处理，不得把连接打死");
            } finally {
                channel.close();
            }
        }
    }

    @Test
    void handshakeReplyStillAnswersLateKeyResend() throws Exception {
        KeyPair responderKey = SessionCrypto.generateKeyPair();
        KeyPair initiatorKey = SessionCrypto.generateKeyPair();
        try (FakeKeyPeer initiator = new FakeKeyPeer(initiatorKey)) {
            DatagramSocket socket = loopbackSocket();
            // initiator=false：本方是 responder，通道会带上 handshakeReply（自己的 KEY 包）
            P2PChannel channel = HolePuncher.punch(socket, initiator.localAddress(), SESSION, false,
                    responderKey, KeyFingerprint.of(initiatorKey));
            try {
                assertTrue(channel.isOpen());
                assertTrue(initiator.awaitKeyPackets(1, 5_000),
                        "responder 在打洞阶段应先回过一次自己的 KEY");
                // 指纹校验只在"首次拿到对端公钥"时做；通道层对迟到的 KEY 重发仍须应答
                initiator.resendKey();
                assertTrue(initiator.awaitKeyPackets(2, 5_000),
                        "握手补偿路径必须保持可用（指纹校验不得关掉 handshakeReply）");
            } finally {
                channel.close();
            }
        }
    }

    @Test
    void twoRealPeersEstablishEncryptedChannelWithVerifiedKeys() throws Exception {
        KeyPair keyA = SessionCrypto.generateKeyPair();
        KeyPair keyB = SessionCrypto.generateKeyPair();
        DatagramSocket socketA = loopbackSocket();
        DatagramSocket socketB = loopbackSocket();
        InetSocketAddress endpointA = (InetSocketAddress) socketA.getLocalSocketAddress();
        InetSocketAddress endpointB = (InetSocketAddress) socketB.getLocalSocketAddress();

        CompletableFuture<P2PChannel> futureA = CompletableFuture.supplyAsync(() ->
                punchOrFail(socketA, endpointB, keyA, KeyFingerprint.of(keyB), true));
        CompletableFuture<P2PChannel> futureB = CompletableFuture.supplyAsync(() ->
                punchOrFail(socketB, endpointA, keyB, KeyFingerprint.of(keyA), false));

        P2PChannel channelA = futureA.get(10, TimeUnit.SECONDS);
        P2PChannel channelB = futureB.get(10, TimeUnit.SECONDS);
        try {
            CountDownLatch received = new CountDownLatch(1);
            AtomicReference<byte[]> payload = new AtomicReference<>();
            channelB.setReceiver(data -> {
                payload.set(data);
                received.countDown();
            });
            byte[] message = "verified-channel".getBytes(StandardCharsets.UTF_8);
            channelA.send(message);
            assertTrue(received.await(5, TimeUnit.SECONDS), "指纹互验后加密通道应能正常收发");
            assertArrayEquals(message, payload.get());
        } finally {
            channelA.close();
            channelB.close();
        }
    }

    // ------------------------------------------------------------ 测试工具

    /** 绑定 IPv4 回环（字面量解析，不做 DNS）：两侧地址可直接比较，测试不依赖外部网络。 */
    private static DatagramSocket loopbackSocket() throws IOException {
        return new DatagramSocket(new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 0));
    }

    private static P2PChannel punchOrFail(DatagramSocket socket, InetSocketAddress peer,
                                          KeyPair ownKey, String expectedFingerprint, boolean initiator) {
        try {
            return HolePuncher.punch(socket, peer, SESSION, initiator, ownKey, expectedFingerprint);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }
}
