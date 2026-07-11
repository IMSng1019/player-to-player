package imsng.player_to_player.util;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * SHA-256 哈希工具。
 * <p>
 * 用于环境文件校验（规范要求：玩家加入世界时用 SHA-256 校验本地环境
 * 与服务端环境是否一致，不一致则增量更新）。
 */
public final class Sha256 {

    /** 文件哈希时的读缓冲大小：128 KB，兼顾吞吐与内存。 */
    private static final int BUFFER_SIZE = 128 * 1024;
    private static final char[] HEX = "0123456789abcdef".toCharArray();

    private Sha256() {
    }

    /** 获取一个新的 SHA-256 摘要器（MessageDigest 非线程安全，不做共享）。 */
    public static MessageDigest newDigest() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            // JDK 必然内置 SHA-256，此处理论不可达
            throw new IllegalStateException("JDK 缺少 SHA-256 实现", e);
        }
    }

    /** 计算字节数组的 SHA-256，返回小写十六进制字符串。 */
    public static String hex(byte[] data) {
        return toHex(newDigest().digest(data));
    }

    /** 计算字符串（UTF-8）的 SHA-256，返回小写十六进制字符串。 */
    public static String hex(String data) {
        return hex(data.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * 流式计算文件 SHA-256（不整读进内存，环境文件可能很大，如 level.dat、mod jar）。
     *
     * @return 小写十六进制哈希
     */
    public static String hexOfFile(Path file) throws IOException {
        MessageDigest digest = newDigest();
        try (InputStream in = Files.newInputStream(file)) {
            byte[] buffer = new byte[BUFFER_SIZE];
            int read;
            while ((read = in.read(buffer)) != -1) {
                digest.update(buffer, 0, read);
            }
        }
        return toHex(digest.digest());
    }

    /** 字节数组转小写十六进制。 */
    public static String toHex(byte[] bytes) {
        char[] out = new char[bytes.length * 2];
        for (int i = 0; i < bytes.length; i++) {
            int v = bytes[i] & 0xFF;
            out[i * 2] = HEX[v >>> 4];
            out[i * 2 + 1] = HEX[v & 0x0F];
        }
        return new String(out);
    }
}
