package imsng.player_to_player.group;

import imsng.player_to_player.registry.ChunkKey;
import imsng.player_to_player.util.Sha256;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

/**
 * 区块增量批次。批次在服务端以 batchId + sequence + sha256 幂等落盘，
 * epoch 是服务端成功 force 后返回的耐久序号。
 */
public final class ChunkDeltaBatch {

    private static final int MAGIC = 0x50545042; // "PTPB"
    private static final int FORMAT_VERSION = 1;
    private static final int MAX_DELTAS = 65_536;
    private static final int MAX_DIMENSION_BYTES = 256;
    private static final int HEADER_BYTES = 4 + 1 + 16 + 8 + 4 + 4;

    private final UUID batchId;
    private final long epoch;
    private final int sequence;
    private final List<ChunkDelta> deltas;

    public ChunkDeltaBatch(UUID batchId, long epoch, int sequence,
                           List<ChunkDelta> deltas) {
        this.batchId = Objects.requireNonNull(batchId, "batchId");
        if (epoch < 0) {
            throw new IllegalArgumentException("批次 epoch 不能为负数");
        }
        if (sequence < 0) {
            throw new IllegalArgumentException("批次 sequence 不能为负数");
        }
        Objects.requireNonNull(deltas, "deltas");
        if (deltas.isEmpty() || deltas.size() > MAX_DELTAS) {
            throw new IllegalArgumentException("批次区块数量非法");
        }
        LinkedHashMap<ChunkKey, ChunkDelta> unique = new LinkedHashMap<>();
        for (ChunkDelta delta : deltas) {
            Objects.requireNonNull(delta, "delta");
            if (unique.put(delta.key(), delta) != null) {
                throw new IllegalArgumentException("批次包含重复区块键: "
                        + delta.key().asString());
            }
        }
        this.epoch = epoch;
        this.sequence = sequence;
        this.deltas = List.copyOf(unique.values());
    }

    public UUID batchId() {
        return batchId;
    }

    /** 语义别名：批次 ID 同时作为退出/运行期事务关联 ID。 */
    public UUID transactionId() {
        return batchId;
    }

    public long epoch() {
        return epoch;
    }

    public int sequence() {
        return sequence;
    }

    public List<ChunkDelta> deltas() {
        return deltas;
    }

    /**
     * 将多个快照按区块键合并，并按保守 wire 预算切成稳定顺序的批次。
     * 同键只保留 version 最大的记录；同版本时保留最后出现的记录（其内容必须不同
     * 时仍会由摘要区分）。
     */
    public static List<ChunkDeltaBatch> split(UUID transactionId, long firstEpoch,
                                               List<ChunkDelta> input,
                                               int maxBytes) {
        Objects.requireNonNull(transactionId, "transactionId");
        Objects.requireNonNull(input, "input");
        if (firstEpoch < 0 || maxBytes <= 0) {
            throw new IllegalArgumentException("批次切分参数非法");
        }
        LinkedHashMap<ChunkKey, ChunkDelta> latest = new LinkedHashMap<>();
        for (ChunkDelta delta : input) {
            Objects.requireNonNull(delta, "delta");
            ChunkDelta previous = latest.get(delta.key());
            if (previous == null || delta.version() >= previous.version()) {
                latest.put(delta.key(), delta);
            }
        }
        if (latest.isEmpty()) {
            return List.of();
        }
        List<ChunkDeltaBatch> result = new ArrayList<>();
        List<ChunkDelta> current = new ArrayList<>();
        // maxBytes 是增量记录预算，批次固定头部由协议帧预算单独承担。
        // 这样调用方可以用同一预算组合不同数量的区块，而不会因为 UUID/epoch
        // 等一次性字段导致小批次被错误拒绝。
        int currentBytes = 0;
        int sequence = 0;
        for (ChunkDelta delta : latest.values()) {
            int deltaBytes = delta.estimatedWireBytes();
            if (deltaBytes > maxBytes) {
                throw new IllegalArgumentException("单个区块增量超过批次上限: "
                        + delta.key().asString());
            }
            if (!current.isEmpty() && currentBytes > maxBytes - deltaBytes) {
                result.add(new ChunkDeltaBatch(transactionId,
                        Math.addExact(firstEpoch, result.size()),
                        sequence++, current));
                current = new ArrayList<>();
                currentBytes = 0;
            }
            current.add(delta);
            currentBytes += deltaBytes;
        }
        if (!current.isEmpty()) {
            result.add(new ChunkDeltaBatch(transactionId,
                    Math.addExact(firstEpoch, result.size()),
                    sequence, current));
        }
        return Collections.unmodifiableList(result);
    }

    /** 二进制编码；调用方可直接将结果作为一个控制帧 payload。 */
    public byte[] encode() {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(bytes);
            out.writeInt(MAGIC);
            out.writeByte(FORMAT_VERSION);
            out.writeLong(batchId.getMostSignificantBits());
            out.writeLong(batchId.getLeastSignificantBits());
            out.writeLong(epoch);
            out.writeInt(sequence);
            out.writeInt(deltas.size());
            for (ChunkDelta delta : deltas) {
                byte[] dimension = delta.key().dimension().getBytes(StandardCharsets.UTF_8);
                if (dimension.length == 0 || dimension.length > MAX_DIMENSION_BYTES) {
                    throw new IllegalArgumentException("维度标识长度非法");
                }
                out.writeShort(dimension.length);
                out.write(dimension);
                out.writeInt(delta.key().x());
                out.writeInt(delta.key().z());
                out.writeLong(delta.version());
                byte[] payload = delta.payload();
                out.writeInt(payload.length);
                out.write(payload);
                out.write(hexToBytes(delta.sha256()));
            }
            out.flush();
            return bytes.toByteArray();
        } catch (IOException impossible) {
            throw new IllegalStateException("编码区块增量批次失败", impossible);
        }
    }

    /** 解码并完整验证批次；任何截断、重复键或摘要不匹配均拒绝。 */
    public static ChunkDeltaBatch decode(byte[] encoded) {
        if (encoded == null || encoded.length < HEADER_BYTES) {
            throw new IllegalArgumentException("批次载荷为空或过短");
        }
        try {
            DataInputStream in = new DataInputStream(new ByteArrayInputStream(encoded));
            if (in.readInt() != MAGIC || in.readUnsignedByte() != FORMAT_VERSION) {
                throw new IllegalArgumentException("未知区块增量批次格式");
            }
            UUID id = new UUID(in.readLong(), in.readLong());
            long epoch = in.readLong();
            int sequence = in.readInt();
            int count = in.readInt();
            if (count <= 0 || count > MAX_DELTAS) {
                throw new IllegalArgumentException("批次区块数量非法");
            }
            List<ChunkDelta> deltas = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                int dimensionLength = in.readUnsignedShort();
                if (dimensionLength == 0 || dimensionLength > MAX_DIMENSION_BYTES) {
                    throw new IllegalArgumentException("维度标识长度非法");
                }
                byte[] dimensionBytes = in.readNBytes(dimensionLength);
                if (dimensionBytes.length != dimensionLength) {
                    throw new EOFException("批次维度字段截断");
                }
                String dimension = new String(dimensionBytes, StandardCharsets.UTF_8);
                int x = in.readInt();
                int z = in.readInt();
                long version = in.readLong();
                int payloadLength = in.readInt();
                if (payloadLength <= 0 || payloadLength > ChunkDelta.MAX_PAYLOAD_BYTES
                        || payloadLength > in.available()) {
                    throw new IllegalArgumentException("区块载荷长度非法");
                }
                byte[] payload = in.readNBytes(payloadLength);
                byte[] digest = in.readNBytes(ChunkDelta.SHA256_BYTES);
                if (payload.length != payloadLength
                        || digest.length != ChunkDelta.SHA256_BYTES) {
                    throw new EOFException("批次区块记录截断");
                }
                deltas.add(new ChunkDelta(new ChunkKey(dimension, x, z), version,
                        payload, Sha256.toHex(digest)));
            }
            if (in.available() != 0) {
                throw new IllegalArgumentException("批次尾部存在未解析数据");
            }
            return new ChunkDeltaBatch(id, epoch, sequence, deltas);
        } catch (IllegalArgumentException e) {
            throw e;
        } catch (IOException | RuntimeException e) {
            throw new IllegalArgumentException("区块增量批次解码失败", e);
        }
    }

    private static byte[] hexToBytes(String hex) {
        if (hex == null || hex.length() != ChunkDelta.SHA256_BYTES * 2) {
            throw new IllegalArgumentException("SHA-256 摘要格式非法");
        }
        byte[] result = new byte[ChunkDelta.SHA256_BYTES];
        for (int i = 0; i < result.length; i++) {
            int high = Character.digit(hex.charAt(i * 2), 16);
            int low = Character.digit(hex.charAt(i * 2 + 1), 16);
            if (high < 0 || low < 0) {
                throw new IllegalArgumentException("SHA-256 摘要包含非法字符");
            }
            result[i] = (byte) ((high << 4) | low);
        }
        return result;
    }
}
