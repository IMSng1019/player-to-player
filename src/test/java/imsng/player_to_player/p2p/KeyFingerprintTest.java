package imsng.player_to_player.p2p;

import imsng.player_to_player.util.Sha256;
import org.junit.jupiter.api.Test;

import java.security.KeyPair;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link KeyFingerprint} 单测：指纹计算口径、非法输入"降级为不可用"的语义、
 * 以及常量时间比对的判定结果（Phase 2 收口的安全判定基础）。
 */
class KeyFingerprintTest {

    private static final String FINGERPRINT_A =
            "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

    @Test
    void fingerprintIsLowercaseSha256HexOfX509Key() throws Exception {
        KeyPair keyPair = SessionCrypto.generateKeyPair();
        byte[] encoded = SessionCrypto.encodePublicKey(keyPair);

        String fingerprint = KeyFingerprint.of(keyPair);

        assertEquals(64, fingerprint.length());
        assertEquals(Sha256.hex(encoded), fingerprint); // 与既有 SHA-256 工具同一口径
        assertEquals(fingerprint, fingerprint.toLowerCase(java.util.Locale.ROOT));
        assertEquals(fingerprint, KeyFingerprint.ofEncoded(encoded));
    }

    @Test
    void wellFormedAcceptsBothCasesAndRejectsEverythingElse() {
        assertTrue(KeyFingerprint.isWellFormed(FINGERPRINT_A));
        assertTrue(KeyFingerprint.isWellFormed(FINGERPRINT_A.toUpperCase(java.util.Locale.ROOT)));

        assertFalse(KeyFingerprint.isWellFormed(null));
        assertFalse(KeyFingerprint.isWellFormed(""));
        assertFalse(KeyFingerprint.isWellFormed("   "));
        assertFalse(KeyFingerprint.isWellFormed(FINGERPRINT_A.substring(0, 63))); // 短一位
        assertFalse(KeyFingerprint.isWellFormed(FINGERPRINT_A + "0"));            // 长一位
        assertFalse(KeyFingerprint.isWellFormed(FINGERPRINT_A.substring(0, 63) + "z"));
        assertFalse(KeyFingerprint.isWellFormed(FINGERPRINT_A.substring(0, 63) + " "));
    }

    @Test
    void normalizeTrimsAndLowercasesOrReturnsNull() {
        assertEquals(FINGERPRINT_A, KeyFingerprint.normalize(FINGERPRINT_A));
        assertEquals(FINGERPRINT_A,
                KeyFingerprint.normalize(FINGERPRINT_A.toUpperCase(java.util.Locale.ROOT)));
        assertEquals(FINGERPRINT_A, KeyFingerprint.normalize("  " + FINGERPRINT_A + "\n"));

        // 不可用一律 null：调用方据此走"无指纹降级"，绝不能当成某个合法指纹
        assertNull(KeyFingerprint.normalize(null));
        assertNull(KeyFingerprint.normalize(""));
        assertNull(KeyFingerprint.normalize("not-a-fingerprint"));
        assertNull(KeyFingerprint.normalize(FINGERPRINT_A.substring(0, 10)));
    }

    @Test
    void matchesOnlyTheExactPeerKey() throws Exception {
        KeyPair peer = SessionCrypto.generateKeyPair();
        KeyPair other = SessionCrypto.generateKeyPair();

        assertTrue(KeyFingerprint.matches(KeyFingerprint.of(peer), SessionCrypto.encodePublicKey(peer)));
        // 大小写不敏感（归一化后比对），避免纯粹的大小写差异造成假失败
        assertTrue(KeyFingerprint.matches(
                KeyFingerprint.of(peer).toUpperCase(java.util.Locale.ROOT),
                SessionCrypto.encodePublicKey(peer)));
        // 换了一把公钥（等价于中间人替换公钥）：必须判定为不匹配
        assertFalse(KeyFingerprint.matches(KeyFingerprint.of(peer), SessionCrypto.encodePublicKey(other)));
        // 期望值不可用 / 公钥缺失：判定失败（调用方在此之前已决定是否降级）
        assertFalse(KeyFingerprint.matches(null, SessionCrypto.encodePublicKey(peer)));
        assertFalse(KeyFingerprint.matches("zz", SessionCrypto.encodePublicKey(peer)));
        assertFalse(KeyFingerprint.matches(KeyFingerprint.of(peer), null));
        assertFalse(KeyFingerprint.matches(KeyFingerprint.of(peer), new byte[0]));
    }

    @Test
    void differentKeyPairsProduceDifferentFingerprints() throws Exception {
        String first = KeyFingerprint.of(SessionCrypto.generateKeyPair());
        String second = KeyFingerprint.of(SessionCrypto.generateKeyPair());
        assertNotNull(first);
        assertNotEquals(first, second);
    }

    @Test
    void encodedKeyMustNotBeNull() {
        assertThrows(IllegalArgumentException.class, () -> KeyFingerprint.ofEncoded(null));
    }
}
