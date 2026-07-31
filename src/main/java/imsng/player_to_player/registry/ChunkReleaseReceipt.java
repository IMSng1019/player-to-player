package imsng.player_to_player.registry;

import java.util.Objects;
import java.util.UUID;

/**
 * 一次区块释放成功后的耐久幂等回执。
 * <p>
 * {@code releaseId} 标识客户端的一次逻辑释放，后续网络重试必须携带同一值；
 * 区块、组和 payload 指纹共同约束该身份，防止错误复用 UUID 时把另一份数据误报为
 * 已写入。{@code finalDataWritten} 是服务端第一次执行所得的原始 ACK 结果。
 */
public record ChunkReleaseReceipt(
        UUID releaseId,
        ChunkKey key,
        UUID groupId,
        int payloadBytes,
        String payloadSha256,
        boolean finalDataWritten) {

    public ChunkReleaseReceipt {
        Objects.requireNonNull(releaseId, "releaseId");
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(groupId, "groupId");
        Objects.requireNonNull(payloadSha256, "payloadSha256");
        if (payloadBytes < 0) {
            throw new IllegalArgumentException("释放 payload 字节数不能为负数");
        }
        if (payloadSha256.isBlank()) {
            throw new IllegalArgumentException("释放 payload SHA-256 不能为空");
        }
        if ((payloadBytes > 0) != finalDataWritten) {
            throw new IllegalArgumentException(
                    "成功释放回执的最终数据写入结果与 payload 不一致");
        }
    }

    /** 校验一次网络重试是否确实属于本回执对应的原始请求。 */
    public boolean matchesRequest(ChunkKey requestKey, UUID requestGroupId,
                                  int requestPayloadBytes,
                                  String requestPayloadSha256) {
        return key.equals(requestKey)
                && groupId.equals(requestGroupId)
                && payloadBytes == requestPayloadBytes
                && payloadSha256.equals(requestPayloadSha256);
    }
}
