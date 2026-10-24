package imsng.player_to_player.client.env;

import imsng.player_to_player.env.restart.RestartTask;
import net.minecraft.Util;
import net.minecraft.client.gui.GuiGraphics;
import net.minecraft.client.gui.components.Button;
import net.minecraft.client.gui.screens.Screen;
import net.minecraft.network.chat.Component;

/** 主菜单中展示未完成的本机任务；重试先重新预检，并使用当前账号，不重放旧认证参数。 */
final class EnvironmentRestartRecoveryScreen extends Screen {
    private final Screen parent;
    private final RestartTask task;

    EnvironmentRestartRecoveryScreen(Screen parent, RestartTask task) {
        super(Component.literal("上次环境重启未完成"));
        this.parent = parent;
        this.task = task;
    }
    @Override protected void init() {
        int y = height / 2 + 25;
        addRenderableWidget(Button.builder(Component.literal("使用当前账号重新连接并重试"), button -> {
            ClientEnvironmentRestart.retryRecovered(task);
            EnvironmentPreflightClient.retryRecovered(minecraft, parent, task.address());
        }).bounds(width / 2 - 130, y, 260, 20).build());
        addRenderableWidget(Button.builder(Component.literal("打开已安装的游戏目录"), button ->
                Util.getPlatform().openFile(task.directory().toFile()))
                .bounds(width / 2 - 130, y + 25, 260, 20).build());
        addRenderableWidget(Button.builder(Component.literal("返回主菜单"), button -> onClose())
                .bounds(width / 2 - 130, y + 50, 260, 20).build());
    }
    @Override public void render(GuiGraphics graphics, int mouseX, int mouseY, float delta) {
        renderDirtBackground(graphics);
        graphics.drawCenteredString(font, title, width / 2, height / 2 - 90, 0xFFFFFF);
        String reason = switch (task.failure()) {
            case CHILD_EXITED -> "新客户端在启动确认前已退出。";
            case INITIALIZATION_UNCONFIRMED -> "超过 5 分钟未收到初始化确认，游戏可能未完成加载。";
            case LAUNCH_FAILED -> "新客户端未能启动。";
            case HANDSHAKE_FAILED, HELPER_FAILED -> "重启辅助进程未能完成交接。";
            default -> "重启流程中断，尚未收到新客户端启动确认。";
        };
        graphics.drawWordWrap(font, Component.literal(reason + "\n服务器：" + task.address()
                + "\n重试将重新校验环境，并使用当前登录身份。"), Math.max(20, width / 2 - 250),
                height / 2 - 60, Math.min(500, width - 40), 0xCCCCCC);
        super.render(graphics, mouseX, mouseY, delta);
    }
    @Override public void onClose() { minecraft.setScreen(parent); }
    @Override public boolean isPauseScreen() { return false; }
}
