package imsng.player_to_player.metadata;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * v9 区块 TAIL_ACK 之后的双向元数据屏障。接收方必须实际应用并确认至少此版本，
 * 才能发 ACK 并把同一可靠流移交游戏隧道；定长边界防止预读后续 tunnel_prepare。
 */
public record MetadataTransferFence(String worldId, long term, long version) {
    private static final int MAGIC = 0x4D455439; // MET9
    public MetadataTransferFence {
        if (worldId == null || worldId.isBlank() || worldId.length() > 512 || term <= 0 || version < 0)
            throw new IllegalArgumentException("非法元数据迁移屏障");
    }
    public static MetadataTransferFence of(MetadataSnapshot snapshot) {
        return new MetadataTransferFence(snapshot.worldId(), snapshot.term(), snapshot.version());
    }
    public boolean coveredBy(MetadataSnapshot applied) {
        return worldId.equals(applied.worldId()) && term == applied.term() && applied.version() >= version;
    }
    public void write(DataOutputStream output, boolean acknowledgement) throws IOException {
        output.writeInt(MAGIC); output.writeBoolean(acknowledgement);
        output.writeUTF(worldId); output.writeLong(term); output.writeLong(version); output.flush();
    }
    public static MetadataTransferFence read(DataInputStream input, boolean acknowledgement) throws IOException {
        if (input.readInt() != MAGIC || input.readBoolean() != acknowledgement)
            throw new IOException("元数据迁移屏障消息类型不符");
        try { return new MetadataTransferFence(input.readUTF(), input.readLong(), input.readLong()); }
        catch (IllegalArgumentException invalid) { throw new IOException("元数据迁移屏障损坏", invalid); }
    }
}
