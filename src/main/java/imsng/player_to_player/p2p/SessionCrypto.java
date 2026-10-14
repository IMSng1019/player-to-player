package imsng.player_to_player.p2p;

import javax.crypto.Cipher;
import javax.crypto.KeyAgreement;
import javax.crypto.Mac;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.PublicKey;
import java.security.spec.X509EncodedKeySpec;
import java.util.concurrent.atomic.AtomicLong;

/**
 * P2P 会话加密（DESIGN.md 第 7 节"加密通道"）。
 * <p>
 * 全部使用 JDK 自带算法，零第三方依赖：
 * <ol>
 *   <li><b>密钥协商</b>：X25519（{@code KeyAgreement.getInstance("XDH")}），
 *       双方交换公钥（X.509 编码明文）得到共享秘密；</li>
 *   <li><b>密钥派生</b>：HKDF-SHA256（RFC 5869，用 HmacSHA256 手写
 *       extract / expand），以 sessionId 为盐、方向标签为 info，
 *       双向各派生一把 AES-256 密钥 —— initiator 用 "p2p-i2r" 键加密发送、
 *       用 "p2p-r2i" 键解密接收，responder 相反，避免双向共键时 nonce 撞车；</li>
 *   <li><b>数据加密</b>：AES/GCM/NoPadding，帧格式
 *       {@code [8 字节大端 seq][GCM 密文+tag]}，nonce = 4 字节方向常量 ++
 *       8 字节大端 seq（共 12 字节）—— seq 单调递增保证同一密钥下 nonce 永不重复。</li>
 * </ol>
 * <b>防重放</b>：接收方维护"已见最大 seq"水位线，{@code seq <= 水位线} 的帧
 * 一律拒绝。<b>Phase 1 取舍</b>：该策略同时拒绝了 UDP 乱序到达的旧包 ——
 * 即乱序包按丢包处理，由上层可靠性协议（Phase 2）重传；换来实现简单且
 * 无需滑动窗口位图。
 * <p>
 * <b>中间人防护（Phase 2 已收口）</b>：公钥本身仍走打洞 UDP 明文交换，但双方公钥
 * 指纹已先经服务端<b>控制信道</b>（TCP，身份由 HELLO 握手锚定）交换，
 * {@link HolePuncher} 收到对端 KEY 包后用 SHA-256 比对（见 {@link KeyFingerprint}），
 * 不匹配即握手失败 —— 路径攻击者无法再靠"替换双方公钥"窃听或篡改本类协商出的密钥。
 * <p>
 * <b>残留信任模型（必须明确）</b>：信任锚是游戏服控制信道自身（客户端↔游戏服的
 * 明文 TCP + HELLO 身份）。控制信道被攻陷、或玩家连到的就是冒充游戏服的服务器时，
 * 中间人依旧成立 —— 服务器本就在信任边界内，这不是 UDP 打洞层能解决的问题。
 * <p>
 * <b>前向保密的边界</b>：X25519 密钥对由 {@code P2PSessions} 按<b>控制会话</b>生成并
 * 被该会话内所有打洞复用（这样"上报的指纹"与"打洞发出的公钥"必然同源，且服务端按
 * clientId 维护一份指纹即可），因此前向保密的粒度是"控制会话"而非"单次打洞"：
 * 会话结束（离开世界）即弃用旧密钥，但同一会话内的多次打洞共享同一把临时密钥。
 * <p>
 * <b>线程安全</b>：encrypt/decrypt 内部同步，可多线程调用；每方向的 Cipher
 * 实例每帧重新初始化（GCM 每次加密必须换 nonce）。
 */
public final class SessionCrypto {

    /** AES-256 密钥长度（字节）。 */
    private static final int AES_KEY_BYTES = 32;
    /** GCM 认证标签长度（位）。 */
    private static final int GCM_TAG_BITS = 128;
    /** GCM nonce 长度（字节）= 4 方向常量 + 8 seq。 */
    private static final int NONCE_BYTES = 12;
    /** 帧头（seq）长度。 */
    private static final int SEQ_BYTES = 8;

    /** initiator → responder 方向的 HKDF info 与 nonce 方向常量。 */
    private static final byte[] INFO_I2R = "p2p-i2r".getBytes(StandardCharsets.UTF_8);
    private static final byte[] INFO_R2I = "p2p-r2i".getBytes(StandardCharsets.UTF_8);
    private static final byte[] DIR_I2R = {'i', '2', 'r', 0};
    private static final byte[] DIR_R2I = {'r', '2', 'i', 0};

    /** 本方发送密钥与方向常量。 */
    private final SecretKeySpec sendKey;
    private final byte[] sendDir;
    /** 本方接收密钥与方向常量。 */
    private final SecretKeySpec recvKey;
    private final byte[] recvDir;

    /** 发送序号（从 1 开始，0 保留表示"未发送过"）。 */
    private final AtomicLong sendSeq = new AtomicLong(0);
    /** 接收水位线：已接受的最大 seq，小于等于它的帧一律拒绝（防重放）。 */
    private final AtomicLong recvHighWater = new AtomicLong(0);

    private SessionCrypto(SecretKeySpec sendKey, byte[] sendDir, SecretKeySpec recvKey, byte[] recvDir) {
        this.sendKey = sendKey;
        this.sendDir = sendDir;
        this.recvKey = recvKey;
        this.recvDir = recvDir;
    }

    // ------------------------------------------------------------ 握手

    /**
     * 生成本方 X25519 临时密钥对（不落盘；由 {@code P2PSessions} 按控制会话持有并复用，
     * 见类 Javadoc"前向保密的边界"）。
     */
    public static KeyPair generateKeyPair() throws GeneralSecurityException {
        KeyPairGenerator kpg = KeyPairGenerator.getInstance("X25519");
        return kpg.generateKeyPair();
    }

    /** 公钥编码为 X.509 字节串（走打洞 socket 明文传给对端）。 */
    public static byte[] encodePublicKey(KeyPair keyPair) {
        return keyPair.getPublic().getEncoded();
    }

    /**
     * 用本方私钥与对端公钥完成协商，派生双向密钥，建立会话。
     *
     * @param ownKeyPair       本方密钥对
     * @param peerPublicKeyDer 对端公钥（X.509 编码，来自不可信网络 ——
     *                         畸形输入由 KeyFactory 抛异常，调用方按握手失败处理。
     *                         真实性由调用方负责：{@link HolePuncher} 已在收到 KEY 包时
     *                         用控制信道下发的指纹完成校验）
     * @param sessionId        服务端分配的会话 ID（作 HKDF 盐，绑定会话上下文）
     * @param initiator        本方是否为发起方（决定方向密钥怎么分配）
     */
    public static SessionCrypto establish(KeyPair ownKeyPair, byte[] peerPublicKeyDer,
                                          String sessionId, boolean initiator)
            throws GeneralSecurityException {
        // 1. 解析对端公钥并做 X25519 协商
        KeyFactory kf = KeyFactory.getInstance("XDH");
        PublicKey peerKey = kf.generatePublic(new X509EncodedKeySpec(peerPublicKeyDer));
        KeyAgreement ka = KeyAgreement.getInstance("XDH");
        ka.init(ownKeyPair.getPrivate());
        ka.doPhase(peerKey, true);
        byte[] sharedSecret = ka.generateSecret();

        // 2. HKDF：extract（盐 = sessionId 字节）→ 双向 expand
        byte[] salt = sessionId.getBytes(StandardCharsets.UTF_8);
        byte[] prk = hkdfExtract(salt, sharedSecret);
        byte[] keyI2R = hkdfExpand(prk, INFO_I2R, AES_KEY_BYTES);
        byte[] keyR2I = hkdfExpand(prk, INFO_R2I, AES_KEY_BYTES);

        SecretKeySpec i2r = new SecretKeySpec(keyI2R, "AES");
        SecretKeySpec r2i = new SecretKeySpec(keyR2I, "AES");
        // initiator 发送用 i2r 键，接收用 r2i 键；responder 相反
        return initiator
                ? new SessionCrypto(i2r, DIR_I2R, r2i, DIR_R2I)
                : new SessionCrypto(r2i, DIR_R2I, i2r, DIR_I2R);
    }

    // ------------------------------------------------------------ 数据帧

    /**
     * 加密一帧：返回 {@code [8B seq][GCM(密文+tag)]}。
     * seq 自增保证 nonce 唯一；同步以防两线程拿到同 seq 不同密文顺序错乱。
     */
    public synchronized byte[] encrypt(byte[] plaintext) throws GeneralSecurityException {
        long seq = sendSeq.incrementAndGet();
        Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
        cipher.init(Cipher.ENCRYPT_MODE, sendKey, new GCMParameterSpec(GCM_TAG_BITS, nonce(sendDir, seq)));
        byte[] ciphertext = cipher.doFinal(plaintext);
        ByteBuffer out = ByteBuffer.allocate(SEQ_BYTES + ciphertext.length);
        out.putLong(seq);
        out.put(ciphertext);
        return out.array();
    }

    /**
     * 解密一帧；重放（seq <= 水位线）、篡改（GCM tag 校验失败）、畸形帧
     * 一律抛 {@link GeneralSecurityException}，调用方按无效包丢弃。
     */
    public synchronized byte[] decrypt(byte[] frame) throws GeneralSecurityException {
        if (frame == null || frame.length < SEQ_BYTES + GCM_TAG_BITS / 8) {
            throw new GeneralSecurityException("帧过短");
        }
        ByteBuffer buf = ByteBuffer.wrap(frame);
        long seq = buf.getLong();
        // 防重放水位线：先校验后推进（解密失败不能推进，否则攻击者可用垃圾帧挤掉合法 seq）
        if (seq <= recvHighWater.get()) {
            throw new GeneralSecurityException("重放或乱序旧帧: seq=" + seq);
        }
        byte[] ciphertext = new byte[frame.length - SEQ_BYTES];
        buf.get(ciphertext);

        Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
        cipher.init(Cipher.DECRYPT_MODE, recvKey, new GCMParameterSpec(GCM_TAG_BITS, nonce(recvDir, seq)));
        byte[] plaintext = cipher.doFinal(ciphertext); // tag 不符在此抛 AEADBadTagException
        recvHighWater.set(seq); // 认证通过才推进水位线
        return plaintext;
    }

    /** 组装 12 字节 nonce：4 字节方向常量 + 8 字节大端 seq。 */
    private static byte[] nonce(byte[] direction, long seq) {
        ByteBuffer buf = ByteBuffer.allocate(NONCE_BYTES);
        buf.put(direction);
        buf.putLong(seq);
        return buf.array();
    }

    // ------------------------------------------------------------ HKDF（RFC 5869）

    /** HKDF-Extract: PRK = HMAC-SHA256(salt, IKM)。 */
    private static byte[] hkdfExtract(byte[] salt, byte[] ikm) throws GeneralSecurityException {
        Mac mac = Mac.getInstance("HmacSHA256");
        mac.init(new SecretKeySpec(salt.length > 0 ? salt : new byte[32], "HmacSHA256"));
        return mac.doFinal(ikm);
    }

    /** HKDF-Expand: OKM = T(1) || T(2) || ... 截取前 length 字节。 */
    private static byte[] hkdfExpand(byte[] prk, byte[] info, int length) throws GeneralSecurityException {
        Mac mac = Mac.getInstance("HmacSHA256");
        mac.init(new SecretKeySpec(prk, "HmacSHA256"));
        byte[] okm = new byte[length];
        byte[] t = new byte[0];
        int copied = 0;
        for (byte counter = 1; copied < length; counter++) {
            mac.reset();
            mac.update(t);
            mac.update(info);
            mac.update(counter);
            t = mac.doFinal();
            int n = Math.min(t.length, length - copied);
            System.arraycopy(t, 0, okm, copied, n);
            copied += n;
        }
        return okm;
    }
}
