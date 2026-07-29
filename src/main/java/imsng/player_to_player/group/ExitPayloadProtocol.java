package imsng.player_to_player.group;

import imsng.player_to_player.netproto.Protocol;

import java.util.Arrays;

/** 退出最终 NBT 的有界分片协议常量与客户端切片工具。 */
public final class ExitPayloadProtocol {

    /** 每片 512 KB，使低上行带宽下也能在 15 秒请求超时内完成单片确认。 */
    public static final int FRAGMENT_BYTES = 512 * 1024;

    /** 区块 NBT 解压上限为 64 MB；gzip 极小开销后仍需留出余量。 */
    public static final int MAX_ASSEMBLED_BYTES = 72 * 1024 * 1024;

    static {
        if (FRAGMENT_BYTES >= Protocol.MAX_FRAME_BYTES) {
            throw new IllegalStateException("退出分片必须小于协议单帧上限");
        }
    }

    private ExitPayloadProtocol() {
    }

    public static int fragmentCount(int totalBytes) {
        if (totalBytes <= 0 || totalBytes > MAX_ASSEMBLED_BYTES) {
            throw new IllegalArgumentException("退出载荷大小非法: " + totalBytes);
        }
        return (totalBytes + FRAGMENT_BYTES - 1) / FRAGMENT_BYTES;
    }

    public static byte[] fragment(byte[] payload, int index) {
        if (payload == null) {
            throw new IllegalArgumentException("退出载荷为空");
        }
        int count = fragmentCount(payload.length);
        if (index < 0 || index >= count) {
            throw new IllegalArgumentException("退出分片序号越界: " + index);
        }
        int from = index * FRAGMENT_BYTES;
        return Arrays.copyOfRange(payload, from,
                Math.min(payload.length, from + FRAGMENT_BYTES));
    }
}
