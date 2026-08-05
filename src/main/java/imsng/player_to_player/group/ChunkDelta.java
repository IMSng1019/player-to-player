package imsng.player_to_player.group;

import imsng.player_to_player.registry.ChunkKey;
import imsng.player_to_player.util.Sha256;

import java.util.Arrays;
import java.util.Objects;

/**
 * 一个区块的不可变增量快照。
 *
 * <p>这里的 payload 是已经压缩好的区块 NBT。version 由客户端单调递增，
 * 用于客户端合并与诊断；服务端最终以 WAL durable epoch 决定先后顺序。
 * sha256 同时用于批次幂等和传输篡改校验。</p>
 */
public final class ChunkDelta {

    /** SHA-256 摘要的二进制长度。 */
    public static final int SHA256_BYTES = 32;
    /** 防御性限制，避免恶意构造在内存中挂起超大载荷。 */
    public static final int MAX_PAYLOAD_BYTES = 64 * 1024 * 1024;

    private final ChunkKey key;
    private final long version;
    private final byte[] payload;
    private final String sha256;

    /**
     * 构造并校验一个快照。调用方传入的数组会立即复制，之后可安全跨线程使用。
     */
    public ChunkDelta(ChunkKey key, long version, byte[] payload, String sha256) {
        this.key = Objects.requireNonNull(key, "key");
        if (key.dimension() == null || key.dimension().isBlank()) {
            throw new IllegalArgumentException("区块维度标识不能为空");
        }
        if (version < 0) {
            throw new IllegalArgumentException("区块版本不能为负数");
        }
        if (payload == null || payload.length == 0
                || payload.length > MAX_PAYLOAD_BYTES) {
            throw new IllegalArgumentException("区块载荷为空或超出大小限制");
        }
        this.version = version;
        this.payload = payload.clone();
        String expected = Sha256.hex(this.payload);
        if (sha256 == null || !expected.equalsIgnoreCase(sha256)) {
            throw new IllegalArgumentException("区块载荷 SHA-256 校验失败");
        }
        this.sha256 = expected;
    }

    /** 从压缩载荷计算摘要并创建记录。 */
    public static ChunkDelta of(ChunkKey key, long version, byte[] payload) {
        Objects.requireNonNull(payload, "payload");
        return new ChunkDelta(key, version, payload, Sha256.hex(payload));
    }

    public ChunkKey key() {
        return key;
    }

    public long version() {
        return version;
    }

    /** 返回防御性副本，避免调用方改变已排队的批次。 */
    public byte[] payload() {
        return payload.clone();
    }

    /** 小写十六进制 SHA-256。 */
    public String sha256() {
        return sha256;
    }

    /** 传输格式中单条记录的保守大小估算（不含批次头）。 */
    int estimatedWireBytes() {
        int dimensionBytes = key.dimension().getBytes(java.nio.charset.StandardCharsets.UTF_8).length;
        long total = 16L + dimensionBytes + payload.length + SHA256_BYTES;
        if (total > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("区块记录大小溢出");
        }
        return (int) total;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof ChunkDelta that)) {
            return false;
        }
        return version == that.version
                && key.equals(that.key)
                && sha256.equals(that.sha256)
                && Arrays.equals(payload, that.payload);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(key, version, sha256);
        return 31 * result + Arrays.hashCode(payload);
    }
}
