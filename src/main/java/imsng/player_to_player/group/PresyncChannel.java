package imsng.player_to_player.group;

import java.io.InputStream;
import java.io.OutputStream;

/** 预同步所需的最小流接口；允许独立进程通过 IPC 精确读取，不跨阶段预读游戏字节。 */
public interface PresyncChannel {
    InputStream inputStream();
    OutputStream outputStream();
    boolean isOpen();
    void abort();
}
