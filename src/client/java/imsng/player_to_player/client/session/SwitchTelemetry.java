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
    private SwitchTelemetry() { }

    public static synchronized void begin(String reason, String id) {
        finish("superseded");
        active = new WorkflowTrace(reason, id == null ? UUID.randomUUID().toString() : id);
        joined = false;
    }

    public static synchronized void mark(String phase) {
        if (active != null) active.mark(phase);
    }

    public static synchronized void joined() {
        joined = true;
        mark("login_complete");
    }

    public static synchronized void finish(String result) {
        if (active != null) active.finish(result);
        active = null;
        joined = false;
    }

    public static synchronized void tick(Minecraft client) {
        if (active == null) return;
        if (joined && client.screen == null && client.player != null && client.level != null
                && client.level.hasChunkAt(client.player.blockPosition())) {
            finish("playable");
        } else if (active.elapsedMillis() > 300_000) {
            finish("timeout");
        }
    }
}
