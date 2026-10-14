package imsng.player_to_player.p2p;

import imsng.player_to_player.util.Sha256;

import java.nio.charset.StandardCharsets;
import java.security.KeyPair;
import java.security.MessageDigest;
import java.util.Locale;

/**
 * P2P 公钥指纹工具（Phase 2 收口：控制信道指纹绑定）。
 * <p>
 * 指纹 = X.509 编码公钥字节的 SHA-256，输出 64 位小写十六进制。
 * 双方在打洞开始前各自的公钥指纹已经过服务端控制信道（TCP；身份由 HELLO 锚定，
 * 但该身份是自报的 —— 见下方"信任锚与残留风险"）交换，
 * {@link HolePuncher} 收到 UDP 明文 KEY 包后立即用本类比对：
 * <ul>
 *   <li>比对通过 → 该公钥确实属于服务端撮合时指定的那个 clientId，可安全协商会话密钥；</li>
 *   <li>比对失败 → 抛 {@code IOException}（握手失败），走既有中转降级 ——
 *       路径攻击者无法再用"替换双方公钥"的方式做中间人。</li>
 * </ul>
 * <b>复用既有实现</b>：哈希直接走 {@link Sha256#hex(byte[])}（环境同步同一套口径），
 * 不引入新的哈希/编码实现。
 * <h2>信任锚与残留风险（务必如实理解，勿高估本机制）</h2>
 * 本机制排除了"UDP 路径上的被动/主动攻击者"：攻击者无法在不被发现的前提下替换双方公钥。
 * 但它<b>不覆盖</b>下面两条既有弱点：
 * <ol>
 *   <li><b>控制信道本身未经认证</b>：HELLO 里的 {@code clientId} 是客户端<b>自报</b>的，
 *       服务端不做凭证校验，能连上控制端口的人可以冒用他人 clientId
 *       （见 {@code HelloHandler} 的身份模型）。本机制的保证因此是
 *       "A 拿到的公钥属于服务端认为的那个 B"，而不是"属于玩家 B 本人"。
 *       这是全模组<b>既有</b>的信任模型，并非本次引入；真正收口需要控制信道身份认证
 *       （例如游戏会话令牌），属独立议题。</li>
 *   <li><b>无转录绑定</b>：双方指纹没有参与密钥派生（HKDF 的 salt 只有 sessionId），
 *       "指纹校验"与"密钥协商"在密码学上没有绑定。已核实其实际影响可忽略 ——
 *       每次打洞都用新生成的临时密钥对（见 {@code P2PSessions} 的会话密钥对生命周期），
 *       攻击者无法借未绑定实施重放或跨会话拼接；把双方指纹并入 HKDF 的 info/salt
 *       （转录绑定）是更严谨的做法，留作后续增强。</li>
 * </ol>
 * <p>
 * <b>入站字段一律不可信</b>：{@link #normalize(String)} 对 null/空串/长度不符/含
 * 非 hex 字符的输入统一返回 {@code null}，调用方据此走"无指纹降级"（记 warn），
 * 而绝不能把非法串当成"某个合法指纹"。
 * <p>
 * 线程安全：全部为无状态静态方法。
 */
public final class KeyFingerprint {

    /** SHA-256 指纹的十六进制字符数（32 字节 → 64 字符）。 */
    public static final int HEX_CHARS = 64;

    private KeyFingerprint() {
    }

    /** 计算 X.509 编码公钥的指纹（64 位小写 hex）。 */
    public static String ofEncoded(byte[] publicKeyDer) {
        if (publicKeyDer == null) {
            throw new IllegalArgumentException("publicKeyDer 不得为 null");
        }
        return Sha256.hex(publicKeyDer);
    }

    /** 计算密钥对中公钥的指纹（与打洞时经 KEY 包发出的字节完全同源）。 */
    public static String of(KeyPair keyPair) {
        return ofEncoded(SessionCrypto.encodePublicKey(keyPair));
    }

    /** 判定是否形如合法指纹：恰为 64 位十六进制字符（大小写均可）；其余一律 false。 */
    public static boolean isWellFormed(String fingerprint) {
        if (fingerprint == null || fingerprint.length() != HEX_CHARS) {
            return false;
        }
        for (int i = 0; i < HEX_CHARS; i++) {
            char c = fingerprint.charAt(i);
            boolean hex = (c >= '0' && c <= '9')
                    || (c >= 'a' && c <= 'f')
                    || (c >= 'A' && c <= 'F');
            if (!hex) {
                return false;
            }
        }
        return true;
    }

    /**
     * 归一化为小写 hex；null / 空白 / 长度或字符非法一律返回 null（= 不可用，调用方降级）。
     * <p>
     * 归一化而非拒绝大小写差异：服务端与客户端可能由不同版本/不同平台拼串，
     * 大小写不属于安全语义，统一小写再比对可避开"仅大小写不同"的假失败。
     */
    public static String normalize(String raw) {
        if (raw == null) {
            return null;
        }
        String trimmed = raw.trim();
        if (!isWellFormed(trimmed)) {
            return null;
        }
        return trimmed.toLowerCase(Locale.ROOT);
    }

    /**
     * 期望指纹与对端公钥实际指纹是否一致（内部先归一化，返回 false 表示不一致或参数不可用）。
     * <p>
     * 用 {@link MessageDigest#isEqual} 做常量时间比较：认证判定不做数据相关的短路，
     * 避免比对时间泄漏"前缀猜对了多少"（此处虽只是公钥哈希，仍保持认证代码的一致习惯）。
     */
    public static boolean matches(String expectedFingerprint, byte[] peerPublicKeyDer) {
        String expected = normalize(expectedFingerprint);
        if (expected == null || peerPublicKeyDer == null || peerPublicKeyDer.length == 0) {
            return false;
        }
        byte[] expectedBytes = expected.getBytes(StandardCharsets.US_ASCII);
        byte[] actualBytes = ofEncoded(peerPublicKeyDer).getBytes(StandardCharsets.US_ASCII);
        return MessageDigest.isEqual(expectedBytes, actualBytes);
    }
}
