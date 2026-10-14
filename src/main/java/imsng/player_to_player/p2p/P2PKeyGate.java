package imsng.player_to_player.p2p;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * P2P 撮合公钥指纹门控（服务端侧纯逻辑：无 IO、无定时器，可单测）。
 * <p>
 * 背景（Phase 2 收口）：打洞双方的公钥指纹要经控制信道交换，但存在一个天然竞态 ——
 * 客户端的密钥对是"打洞时才需要"的惰性资源，而 {@code P2P_ENDPOINT_EXCHANGE} 紧跟
 * {@code P2P_CONNECT_REQUEST} 下发，此刻对端指纹可能尚未上报。本类把这段状态机从
 * {@code P2PBrokerHandlers} 里抽出来，使"指纹未就绪 → 索取 → 就绪后下发 →
 * 超时降级"的时序可以脱离 Netty 单测。
 * <p>
 * 职责（两块状态，都按 clientId / sessionId 键控）：
 * <ol>
 *   <li><b>指纹表</b>：clientId → 该客户端本次控制会话所用 X25519 公钥的 SHA-256 指纹。
 *       生命周期与控制会话一致：{@code HelloHandler} 在新 HELLO（新会话/新进程可能换了密钥对）
 *       与完全离线时调用 {@link #forget(UUID)} 清理，防止旧指纹被当成"已就绪"。</li>
 *   <li><b>待定撮合</b>：sessionId → {@link Pending}（撮合双方 + 指纹等待截止时刻）。
 *       {@link #open(Pending)} 返回仍需索要指纹的客户端；指纹上报后由
 *       {@link #takeReady()} 领取已齐备的撮合；{@link #takeExpired(long)} 领取超时的撮合
 *       供调用方按"无指纹"降级下发。</li>
 * </ol>
 * <b>认领语义（防重复下发）</b>：{@code takeReady/takeExpired} 取出即移除，
 * 同一撮合只会被一个调用方领到；因此 P2PBrokerHandlers 里的"上报回调"与"超时定时器"
 * 即使并发也不会重复下发 endpoint。调用方在成功下发/撮合终止时调 {@link #complete(UUID)}
 * 幂等收尾。
 * <p>
 * 线程安全：内部全部为 {@link ConcurrentHashMap}，取用均为原子 remove（CAS 语义），
 * 可被多个 Netty 事件循环线程与调度线程并发调用。
 */
public final class P2PKeyGate {

    /**
     * 服务端进程内全局实例（游戏服单例）。客户端不使用本类：
     * 客户端只持有自己的一把会话密钥对（见 {@code P2PSessions}）。
     */
    public static final P2PKeyGate SERVER = new P2PKeyGate();

    /**
     * 一次待定撮合。
     *
     * @param sessionId      服务端生成的撮合会话号
     * @param requester      P2P_CONNECT_REQUEST 的请求方 clientId（直连时 initiator=true）
     * @param target         被请求方 clientId
     * @param deadlineMillis 指纹等待截止时刻（到点仍未就绪 → 降级下发，绝不永久卡住撮合）
     */
    public record Pending(UUID sessionId, UUID requester, UUID target, long deadlineMillis) {
    }

    /** clientId → 指纹（小写 hex）。 */
    private final Map<UUID, String> fingerprints = new ConcurrentHashMap<>();

    /** sessionId → 待定撮合。 */
    private final Map<UUID, Pending> pendings = new ConcurrentHashMap<>();

    /** 可见性包内足够（{@link #SERVER} 是唯一服务端实例，测试自建实例）。 */
    P2PKeyGate() {
    }

    /**
     * 登记某客户端上报的公钥指纹。
     *
     * @param fingerprint 原始字段（入站不可信）：内部经 {@link KeyFingerprint#normalize}
     *                    归一化，非法一律拒绝
     * @return true = 已登记；false = clientId 为空或指纹非法（调用方应记 warn 并忽略）
     */
    public boolean report(UUID clientId, String fingerprint) {
        String normalized = KeyFingerprint.normalize(fingerprint);
        if (clientId == null || normalized == null) {
            return false;
        }
        fingerprints.put(clientId, normalized);
        return true;
    }

    /** 查询某客户端已登记的指纹；未就绪返回 null。 */
    public String fingerprintOf(UUID clientId) {
        return clientId == null ? null : fingerprints.get(clientId);
    }

    /** 清除某客户端的指纹（新 HELLO / 完全离线时调用）。 */
    public void forget(UUID clientId) {
        if (clientId != null) {
            fingerprints.remove(clientId);
        }
    }

    /**
     * 开启一次撮合的指纹等待。
     * <p>
     * <b>先登记再判断</b>：若先判断后登记，指纹恰好在两步之间到达时会"上报回调查不到待定撮合、
     * 开启方又认为缺失"而白等一次超时；先登记可让并发上报的回调直接领走该撮合。
     *
     * @return 仍需索要指纹的客户端列表（去重；空列表 = 双方指纹已就绪，可直接下发 endpoint）
     */
    public List<UUID> open(Pending pending) {
        pendings.put(pending.sessionId(), pending);
        List<UUID> missing = new ArrayList<>(2);
        if (fingerprintOf(pending.requester()) == null) {
            missing.add(pending.requester());
        }
        if (fingerprintOf(pending.target()) == null && !pending.target().equals(pending.requester())) {
            missing.add(pending.target());
        }
        return missing;
    }

    /**
     * 领取（并移除）所有双方指纹已齐备的待定撮合；调用方对每一项下发 endpoint。
     * <p>
     * 在每次指纹上报后调用即可：与 {@link #open(Pending)} 的竞态由"取出即移除"收敛，
     * 不会重复下发。
     */
    public List<Pending> takeReady() {
        List<Pending> ready = new ArrayList<>(0);
        for (Pending candidate : pendings.values()) {
            if (fingerprintOf(candidate.requester()) != null
                    && fingerprintOf(candidate.target()) != null
                    && pendings.remove(candidate.sessionId(), candidate)) {
                ready.add(candidate);
            }
        }
        return ready;
    }

    /**
     * 领取（并移除）所有已过指纹等待截止时刻的待定撮合；调用方按"缺失指纹 = null"
     * 降级下发并记 warn（可用性优先：旧服务端/客户端不上报指纹时打洞照常进行）。
     *
     * @param nowMillis 当前时刻（显式传入，便于单测用假时间驱动超时）
     */
    public List<Pending> takeExpired(long nowMillis) {
        List<Pending> expired = new ArrayList<>(0);
        for (Pending candidate : pendings.values()) {
            if (candidate.deadlineMillis() <= nowMillis
                    && pendings.remove(candidate.sessionId(), candidate)) {
                expired.add(candidate);
            }
        }
        return expired;
    }

    /** 结束某次撮合的指纹等待（下发完成 / 撮合终止）；幂等，返回被移除项或 null。 */
    public Pending complete(UUID sessionId) {
        return sessionId == null ? null : pendings.remove(sessionId);
    }

    /** 待定撮合数（诊断/测试用）。 */
    public int pendingCount() {
        return pendings.size();
    }

    /** 已登记指纹数（诊断/测试用）。 */
    public int knownFingerprintCount() {
        return fingerprints.size();
    }

    /** 清空全部状态（服务端停止 / 单测隔离用）。 */
    public void clear() {
        pendings.clear();
        fingerprints.clear();
    }
}
