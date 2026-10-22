package imsng.player_to_player.client.group;

import imsng.player_to_player.client.session.SwitchTelemetry;
import imsng.player_to_player.client.session.WorldSession;
import imsng.player_to_player.netproto.ControlConnection;
import imsng.player_to_player.p2p.P2PSessions;
import imsng.player_to_player.p2p.PreparedTunnel;
import imsng.player_to_player.p2p.ReliableChannel;
import net.minecraft.client.Minecraft;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.UUID;

/** 副客户端加入：先准备经对端确认的传输，再走原版断开/登录。失败保留当前世界。 */
public final class SecondaryJoiner {
    private static final Logger LOGGER = LoggerFactory.getLogger("player_to_player/secondary");
    private SecondaryJoiner() { }

    /** 后台准备，不触碰当前 Minecraft 世界；合并可在冻结前调用。 */
    public static PreparedTunnel prepare(ControlConnection conn, UUID primary, UUID transaction) throws Exception {
        SwitchTelemetry.mark("transport_begin");
        var transport = P2PSessions.connect(conn, primary);
        if (!WorldSession.isCurrentConnection(conn)) {
            transport.close();
            throw new IllegalStateException("准备期间世界会话已经结束");
        }
        var prepared = PreparedTunnel.prepare(new ReliableChannel(transport, "join:" + transaction),
                transaction.toString());
        SwitchTelemetry.mark("transport_prepared");
        return prepared;
    }

    /** 原版断开完成后才通知旧主关闭 LAN；登录完成由独立阶段计时记录。 */
    public static void activate(Minecraft minecraft, ControlConnection conn, UUID primary,
                                PreparedTunnel prepared, Runnable disconnected) throws Exception {
        if (!WorldSession.isCurrentConnection(conn)) {
            prepared.close();
            throw new IllegalStateException("世界会话已过期");
        }
        int port = prepared.activate();
        minecraft.execute(() -> {
            if (!WorldSession.isCurrentConnection(conn)) {
                prepared.close();
                return;
            }
            try {
                WorldSwitcher.markTunnelPort(port);
                WorldSwitcher.switchToTunnel(minecraft, port, primary.toString());
                disconnected.run();
            } catch (RuntimeException failure) {
                prepared.close();
                SwitchTelemetry.finish("switch_failed");
                LOGGER.error("预建隧道激活后的原版切换失败", failure);
                WorldSession.transitionFailed(conn, "无法完成组世界连接，请稍后重新连接。");
            }
        });
    }

    public static void join(Minecraft minecraft, ControlConnection conn, UUID primary) {
        PreparedTunnel prepared = null;
        try {
            prepared = prepare(conn, primary, UUID.randomUUID());
            activate(minecraft, conn, primary, prepared, () -> { });
        } catch (Exception failure) {
            if (prepared != null) prepared.close();
            SwitchTelemetry.finish("transport_failed");
            LOGGER.error("加入组世界准备失败，保持当前世界连接", failure);
            WorldSession.transitionFailed(conn, "无法连接组内主客户端，请检查网络后重试。");
        }
    }
}
