package imsng.player_to_player.netproto;

import net.fabricmc.fabric.api.networking.v1.FabricPacket;
import net.fabricmc.fabric.api.networking.v1.PacketType;
import net.minecraft.network.FriendlyByteBuf;
import net.minecraft.resources.ResourceLocation;

/**
 * 服务端 → 客户端：服务端自身两个 P2P 端口的下发（MC 原版自定义包通道）。
 * <p>
 * <b>为什么需要这个包</b>：控制面本身跑在独立的 TCP 端口上（默认 25580），
 * 客户端必须先知道这个端口才能建立控制连接 —— 它不可能由控制连接自己下发。
 * 旧实现要求客户端本地配置与服务端一致；服务端改端口后客户端会表现为
 * "模组功能全部失效"。本包在玩家加入 MC 服务器后、控制连接建立前，把服务端
 * <b>实际绑定</b>的两个端口直接送到客户端，消除这个鸡生蛋问题。
 * <p>
 * 端口语义：
 * <ul>
 *   <li>{@code controlPort}：控制面端口，恒 &gt; 0；</li>
 *   <li>{@code relayPort}：服务端<b>本机</b>兼任中转时的中转端口；服务端不兼任中转
 *       或使用外部中转地址时为 0（外部中转的地址/端口仍由 HELLO_ACK 下发）。</li>
 * </ul>
 * <p>
 * 方向只有 S→C，但类型同时登记了服务端空接收器：Fabric 会把它写进服务端发给
 * 客户端的频道清单，客户端据此在旧版/非本模组服务端上快速放弃等待，而不是
 * 每次加入都空等超时。
 * <p>
 * 兼容性：旧客户端不声明本频道，服务端仍会发送，旧客户端按未知频道忽略；
 * 新客户端在旧服务端上等不到包，回退本地配置。均为加性行为，{@link Protocol#VERSION} 不变。
 */
public final class PortSyncPacket implements FabricPacket {

    /** MC 通道名：命名空间与模组 id 一致。 */
    public static final ResourceLocation ID =
            new ResourceLocation("player_to_player", "server_ports");

    /** 包类型（读写器）；两端共用同一份定义。 */
    public static final PacketType<PortSyncPacket> TYPE =
            PacketType.create(ID, PortSyncPacket::new);

    private final int controlPort;
    private final int relayPort;

    public PortSyncPacket(int controlPort, int relayPort) {
        this.controlPort = controlPort;
        this.relayPort = relayPort;
    }

    /** 解码构造器（Fabric 按 {@link #TYPE} 调用）。 */
    public PortSyncPacket(FriendlyByteBuf buf) {
        this(buf.readVarInt(), buf.readVarInt());
    }

    public int controlPort() {
        return controlPort;
    }

    public int relayPort() {
        return relayPort;
    }

    @Override
    public void write(FriendlyByteBuf buf) {
        // 端口都在 [0, 65535]，varint 最省空间；0 表示"无此端口"。
        buf.writeVarInt(controlPort);
        buf.writeVarInt(relayPort);
    }

    @Override
    public PacketType<?> getType() {
        return TYPE;
    }
}
