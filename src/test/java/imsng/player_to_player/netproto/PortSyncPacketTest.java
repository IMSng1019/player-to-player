package imsng.player_to_player.netproto;

import io.netty.buffer.Unpooled;
import net.minecraft.network.FriendlyByteBuf;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * {@link PortSyncPacket} 的编解码契约：端口按 varint 写入、读回一致，
 * 且不残留未消费字节（残留会让后续帧错位）。
 */
class PortSyncPacketTest {

    @Test
    void roundTripsServerPorts() {
        FriendlyByteBuf buf = new FriendlyByteBuf(Unpooled.buffer());
        new PortSyncPacket(25580, 25581).write(buf);

        PortSyncPacket decoded = PortSyncPacket.TYPE.read(buf);

        assertEquals(25580, decoded.controlPort());
        assertEquals(25581, decoded.relayPort());
        assertEquals(0, buf.readableBytes());
    }

    @Test
    void roundTripsZeroRelayPort() {
        // relayPort=0：服务端不兼任中转/使用外部中转，客户端不得据此连接本机中转
        FriendlyByteBuf buf = new FriendlyByteBuf(Unpooled.buffer());
        new PortSyncPacket(30000, 0).write(buf);

        PortSyncPacket decoded = PortSyncPacket.TYPE.read(buf);

        assertEquals(30000, decoded.controlPort());
        assertEquals(0, decoded.relayPort());
    }
}
