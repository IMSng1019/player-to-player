package imsng.player_to_player.client.session;

import imsng.player_to_player.util.WorkflowTrace;
import net.minecraft.client.Minecraft;
import java.util.UUID;

/**
 * 客户端切换计时：JOIN 只是登录完成，不代表脚下区块可用。
 * 在原版加载界面退出且脚下区块到达后记录 playable；该指标不宣称全部视距已渲染。
 */
public final class SwitchTelemetry {
    private static WorkflowTrace active;
    private static boolean joined;
    private static boolean preparingInitialJoin;
    private static String displayStatus = "正在连接服务端";
    private SwitchTelemetry() { }

    public static synchronized void begin(String reason, String id) {
        finish("superseded");
        active = new WorkflowTrace(reason, id == null ? UUID.randomUUID().toString() : id);
        joined = false;
        preparingInitialJoin = "join".equals(reason);
        displayStatus = "split".equals(reason) ? "正在等待原组保存并卸载交接区域" : "正在连接服务端";
    }

    public static synchronized void mark(String phase) {
        if (active != null) active.mark(phase);
        displayStatus = switch (phase) {
            case "env_begin" -> "正在校验和同步世界环境";
            case "env_ready", "role_begin" -> "正在分配组内角色";
            case "player_state_begin", "player_state_ready" -> "正在准备玩家状态";
            case "transport_begin", "transport_prepared" -> "正在准备组内连接";
            case "disconnect_begin" -> "正在保存并退出旧世界";
            case "local_load_begin", "login_begin" -> "正在加载目标世界";
            default -> displayStatus;
        };
        if ("disconnect_begin".equals(phase)) preparingInitialJoin = false;
    }

    public static synchronized String displayStatus() { return displayStatus; }

    public static synchronized void joined() {
        joined = true;
        mark("login_complete");
    }

    public static synchronized void finish(String result) {
        if (active != null) active.finish(result);
        active = null;
        joined = false;
        preparingInitialJoin = false;
    }

    public static synchronized void tick(Minecraft client) {
        WorldSession.tickPendingSplit(client);
        if (active == null) return;
        if (preparingInitialJoin && client.level != null && client.screen == null) {
            client.setScreen(new JoinProgressScreen());
        }
        if (joined && client.screen == null && client.player != null && client.level != null
                && client.level.hasChunkAt(client.player.blockPosition())) {
            finish("playable");
        } else if (active.elapsedMillis() > 300_000) {
            if (preparingInitialJoin) WorldSession.transitionFailed(null, "准备组世界超时，请检查连接后重试。");
            finish("timeout");
        }
    }
}
