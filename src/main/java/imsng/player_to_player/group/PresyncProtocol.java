package imsng.player_to_player.group;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * 预同步线协议（Phase 3，跑在 {@code ReliableChannel} 的有序可靠字节流上）。
 * <p>
 * 会话头（{@code TcpTunnel.writeHeader} 的 JSON，op=presync）之后，双向各自
 * 发送定长类型 + 变长载荷的记录流：
 * <pre>
 *   发送方（让出方 A）→ 接收方（接管方 B）：
 *     byte  CHUNK(1)          UTF chunkKey, int len, byte[len] gzip 区块 NBT
 *     byte  PLAYER(2)         UTF playerUuid, int len, byte[len] gzip 玩家 NBT
 *     byte  SNAPSHOT_DONE(3)  （快照阶段结束，其后为增量）
 *     byte  TAIL_DONE(4)      （尾部增量结束 —— A 已暂停演算，这是最终态）
 *   接收方 B → 发送方 A：
 *     byte  ACK_SNAPSHOT(5)   （快照已全部入库）
 *     byte  ACK_TAIL(6)       （尾部已全部入库 —— A 可回报 switched）
 * </pre>
 * 记录一律"整块状态覆盖"（同 key 后到覆盖先到）：相比规范设想的逐 tick 事件
 * 重放，区块级全量覆盖是<b>幂等</b>的 —— 丢失中间版本无损最终一致，实现与
 * 校验都简单得多；代价是增量粒度粗（整区块），对家用带宽仍在可行范围
 * （单区块 gzip 后通常 5~50 KB）。
 * <p>
 * 单条记录载荷上限 {@value #MAX_RECORD_BYTES}（防恶意/病态数据撑爆内存）。
 */
public final class PresyncProtocol {

    /** 记录类型。 */
    public static final byte TYPE_CHUNK = 1;
    public static final byte TYPE_PLAYER = 2;
    public static final byte TYPE_SNAPSHOT_DONE = 3;
    public static final byte TYPE_TAIL_DONE = 4;
    public static final byte TYPE_ACK_SNAPSHOT = 5;
    public static final byte TYPE_ACK_TAIL = 6;

    /** 单条记录载荷上限（gzip 后的区块/玩家 NBT；正常数据远小于此）。 */
    public static final int MAX_RECORD_BYTES = 8 * 1024 * 1024;

    private PresyncProtocol() {
    }

    /** 写一条带键与载荷的记录（CHUNK/PLAYER；调用方串行化写入）。 */
    public static void writeRecord(DataOutputStream out, byte type, String key, byte[] payload)
            throws IOException {
        out.writeByte(type);
        out.writeUTF(key);
        out.writeInt(payload.length);
        out.write(payload);
        out.flush();
    }

    /** 写一条无载荷标记（SNAPSHOT_DONE/TAIL_DONE/ACK_*）。 */
    public static void writeMarker(DataOutputStream out, byte type) throws IOException {
        out.writeByte(type);
        out.flush();
    }

    /** 读载荷（长度前缀 + 上限校验；超限视为协议错误抛异常断开）。 */
    public static byte[] readPayload(DataInputStream in) throws IOException {
        int len = in.readInt();
        if (len < 0 || len > MAX_RECORD_BYTES) {
            throw new IOException("预同步记录长度非法: " + len);
        }
        byte[] payload = new byte[len];
        in.readFully(payload);
        return payload;
    }
}
