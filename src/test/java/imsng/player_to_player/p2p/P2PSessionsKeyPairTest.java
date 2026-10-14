package imsng.player_to_player.p2p;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.security.GeneralSecurityException;
import java.security.KeyPair;
import java.util.List;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link P2PSessions} 会话密钥对语义单测（Phase 2 收口的关键前提）：
 * <b>上报给服务端的指纹必须与打洞时发出的公钥同源</b> —— 实现方式就是
 * "一把密钥对一个控制会话、上报与打洞都取同一个实例"。
 */
class P2PSessionsKeyPairTest {

    @AfterEach
    void resetSession() {
        // closeAll 会顺带弃用会话密钥对，避免静态状态污染其他测试
        P2PSessions.closeAll();
    }

    @Test
    void reusesTheSameKeyPairWithinOneSession() throws Exception {
        KeyPair first = P2PSessions.sessionKeyPair();
        KeyPair second = P2PSessions.sessionKeyPair();

        assertSame(first, second, "同一控制会话内必须复用同一把密钥对");
        // 指纹同源：若实现变成"每次取新密钥对"，对端指纹校验就会莫名失败
        assertEquals(KeyFingerprint.of(first), KeyFingerprint.of(second));
    }

    @Test
    void rotatesKeyPairAfterSessionTeardown() throws Exception {
        String before = KeyFingerprint.of(P2PSessions.sessionKeyPair());

        P2PSessions.closeAll();

        String after = KeyFingerprint.of(P2PSessions.sessionKeyPair());
        assertNotEquals(before, after, "世界会话拆除后应换新密钥（并由 register 重新上报指纹）");
    }

    @Test
    void concurrentCallersObserveOnePublishedKeyPair() throws Exception {
        List<KeyPair> pairs = IntStream.range(0, 32).parallel()
                .mapToObj(i -> {
                    try {
                        return P2PSessions.sessionKeyPair();
                    } catch (GeneralSecurityException e) {
                        throw new IllegalStateException(e);
                    }
                })
                .toList();

        KeyPair published = pairs.get(0);
        assertTrue(pairs.stream().allMatch(pair -> pair == published),
                "并发生成只允许发布一把密钥对（CAS 语义）");
    }
}
