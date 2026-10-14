package imsng.player_to_player.p2p;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link P2PKeyGate} 单测：服务端"指纹未就绪 → 索取 → 就绪后下发 → 超时降级"的完整时序
 * 全部在这里用假时间确定性覆盖（无需 Netty、无需真实连接）。
 */
class P2PKeyGateTest {

    private static final UUID REQUESTER = UUID.fromString("11111111-1111-4111-8111-111111111111");
    private static final UUID TARGET = UUID.fromString("22222222-2222-4222-8222-222222222222");
    private static final UUID SESSION = UUID.fromString("33333333-3333-4333-8333-333333333333");
    private static final String FINGERPRINT_A =
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    private static final String FINGERPRINT_B =
            "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

    @Test
    void reportNormalizesAndRejectsIllegalValues() {
        P2PKeyGate gate = new P2PKeyGate();

        assertTrue(gate.report(REQUESTER, FINGERPRINT_A.toUpperCase(java.util.Locale.ROOT)));
        assertEquals(FINGERPRINT_A, gate.fingerprintOf(REQUESTER)); // 归一化为小写

        assertFalse(gate.report(TARGET, "not-hex"));
        assertFalse(gate.report(TARGET, ""));
        assertFalse(gate.report(TARGET, null));
        assertFalse(gate.report(null, FINGERPRINT_B));
        assertNull(gate.fingerprintOf(TARGET));
        assertEquals(1, gate.knownFingerprintCount());
    }

    /**
     * 核心时序：一方指纹缺失 → open() 报出缺失方（服务端据此发 P2P_KEY_REQUEST）；
     * 上报到达前既不能提前下发、也不得到点前降级；上报到达后 takeReady 恰好交付一次。
     */
    @Test
    void missingFingerprintDefersDeliveryUntilReportArrives() {
        P2PKeyGate gate = new P2PKeyGate();
        gate.report(REQUESTER, FINGERPRINT_A);

        P2PKeyGate.Pending pending = new P2PKeyGate.Pending(SESSION, REQUESTER, TARGET, 1_000);
        List<UUID> missing = gate.open(pending);

        assertEquals(List.of(TARGET), missing, "只有目标方指纹缺失，应只向它索要");
        assertEquals(1, gate.pendingCount());
        assertTrue(gate.takeReady().isEmpty(), "指纹未齐备不得下发");
        assertTrue(gate.takeExpired(999).isEmpty(), "未到截止时刻不得降级下发");

        assertTrue(gate.report(TARGET, FINGERPRINT_B), "目标方上报后应登记成功");
        List<P2PKeyGate.Pending> ready = gate.takeReady();
        assertEquals(1, ready.size(), "双方齐备后恰好交付一次");
        assertEquals(SESSION, ready.get(0).sessionId());
        assertEquals(REQUESTER, ready.get(0).requester());
        assertEquals(TARGET, ready.get(0).target());
        assertTrue(gate.takeReady().isEmpty(), "取出即移除：不会重复下发");
        assertEquals(0, gate.pendingCount());
        assertNull(gate.complete(SESSION), "已交付的撮合收尾是幂等空操作");
    }

    @Test
    void bothFingerprintsReadyNeedsNoRequest() {
        P2PKeyGate gate = new P2PKeyGate();
        gate.report(REQUESTER, FINGERPRINT_A);
        gate.report(TARGET, FINGERPRINT_B);

        P2PKeyGate.Pending pending = new P2PKeyGate.Pending(SESSION, REQUESTER, TARGET, 5_000);
        assertTrue(gate.open(pending).isEmpty(), "双方指纹就绪 → 无需索要，直接下发");
    }

    /** 超时降级：到点仍未齐备 → 领取该项，缺失方 fingerprintOf 为 null（调用方据此不下发该字段）。 */
    @Test
    void expiredWaitFallsBackToDeliveryWithoutFingerprint() {
        P2PKeyGate gate = new P2PKeyGate();
        gate.report(REQUESTER, FINGERPRINT_A);
        P2PKeyGate.Pending pending = new P2PKeyGate.Pending(SESSION, REQUESTER, TARGET, 1_000);
        gate.open(pending);

        List<P2PKeyGate.Pending> expired = gate.takeExpired(1_000);
        assertEquals(1, expired.size(), "到点必须降级下发，绝不能永久卡住撮合");
        assertEquals(SESSION, expired.get(0).sessionId());
        assertEquals(FINGERPRINT_A, gate.fingerprintOf(expired.get(0).requester()));
        assertNull(gate.fingerprintOf(expired.get(0).target()), "缺失方无指纹 → 该侧握手降级");
        assertTrue(gate.takeExpired(2_000).isEmpty(), "取出即移除：超时下发同样只发生一次");
        assertTrue(gate.takeReady().isEmpty());
    }

    @Test
    void forgetDropsFingerprintWhenClientGoesOffline() {
        P2PKeyGate gate = new P2PKeyGate();
        gate.report(REQUESTER, FINGERPRINT_A);
        gate.forget(REQUESTER);
        assertNull(gate.fingerprintOf(REQUESTER));
        assertEquals(0, gate.knownFingerprintCount());
    }

    @Test
    void clearWipesFingerprintsAndPendingWaits() {
        P2PKeyGate gate = new P2PKeyGate();
        gate.report(REQUESTER, FINGERPRINT_A);
        gate.open(new P2PKeyGate.Pending(SESSION, REQUESTER, TARGET, 1_000));

        gate.clear();

        assertEquals(0, gate.knownFingerprintCount());
        assertEquals(0, gate.pendingCount());
        assertTrue(gate.takeReady().isEmpty());
        assertTrue(gate.takeExpired(Long.MAX_VALUE).isEmpty());
    }

    /** 请求方与目标方相同时（防御性分支）不应把同一个 clientId 索要两次。 */
    @Test
    void openDoesNotDuplicateSelfPeer() {
        P2PKeyGate gate = new P2PKeyGate();
        P2PKeyGate.Pending pending = new P2PKeyGate.Pending(SESSION, REQUESTER, REQUESTER, 1_000);
        assertEquals(List.of(REQUESTER), gate.open(pending));
    }

    /** 并发上报 + 领取：同一撮合只会被一个线程领到（"取出即移除"的 CAS 语义）。 */
    @Test
    void concurrentReportsDeliverAtMostOnce() throws Exception {
        P2PKeyGate gate = new P2PKeyGate();
        gate.report(REQUESTER, FINGERPRINT_A);
        gate.open(new P2PKeyGate.Pending(SESSION, REQUESTER, TARGET, 60_000));

        List<P2PKeyGate.Pending> delivered = new CopyOnWriteArrayList<>();
        CountDownLatch start = new CountDownLatch(1);
        int workers = 16;
        CountDownLatch done = new CountDownLatch(workers);
        for (int i = 0; i < workers; i++) {
            Thread thread = new Thread(() -> {
                try {
                    start.await(5, TimeUnit.SECONDS);
                    gate.report(TARGET, FINGERPRINT_B);
                    delivered.addAll(gate.takeReady());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    done.countDown();
                }
            });
            thread.setDaemon(true);
            thread.start();
        }
        start.countDown();
        assertTrue(done.await(5, TimeUnit.SECONDS), "并发上报线程未在超时内完成");

        assertEquals(1, delivered.size(), "并发路径下同一撮合只允许下发一次");
        assertNotNull(delivered.get(0));
    }
}
