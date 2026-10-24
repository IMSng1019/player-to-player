package imsng.player_to_player.client.env;

import net.minecraft.Util;
import net.minecraft.client.gui.GuiGraphics;
import net.minecraft.client.gui.components.Button;
import net.minecraft.client.gui.screens.Screen;
import net.minecraft.network.chat.Component;

/** 登录前独立下载界面；此时没有 MC 登录连接，取消也不会触发玩家或组退出。 */
final class EnvironmentPreflightScreen extends Screen {
    private final Runnable cancel;
    private String status = "正在校验并同步服务器环境…";
    private ClientRuntimeProfile.Installation installation;
    private Runnable retry;
    private boolean restarting;
    private boolean committed;
    private String restartReason = "";

    EnvironmentPreflightScreen(Runnable cancel) {
        super(Component.literal("准备服务器环境"));
        this.cancel = cancel;
    }

    void status(String value) { status = value; }

    void requireRestart(String reason, ClientRuntimeProfile.Installation value, Runnable retry) {
        restartReason = reason;
        status = reason + (value.canLaunch() ? "" : "\n" + value.limitation());
        installation = value;
        this.retry = retry;
        refresh();
    }

    void preparingRestart() {
        restarting = true;
        status = "环境已安装并校验。正在准备自动重启…\n此时仍可取消连接。";
        refresh();
    }

    void restartCommitted() {
        committed = true;
        status = "正在提交重启并正常退出游戏…\n保存和退出完成后，将自动启动同步环境并连接原服务器。";
        refresh();
    }

    void restartFailed(String reason) {
        restarting = false;
        committed = false;
        status = restartReason + "\n" + reason;
        refresh();
    }

    private void refresh() {
        clearWidgets();
        init();
    }

    @Override protected void init() {
        int y = height / 2 + 40;
        if (installation != null) {
            if (installation.canLaunch()) {
                var restart = addRenderableWidget(Button.builder(Component.literal("重试：用同步环境重启"), button -> retry.run())
                        .bounds(width / 2 - 120, y, 240, 20).build());
                restart.active = !restarting && !committed;
                y += 25;
            }
            addRenderableWidget(Button.builder(Component.literal("打开已安装的游戏目录"), button ->
                    Util.getPlatform().openFile(installation.directory().toFile()))
                    .bounds(width / 2 - 120, y, 240, 20).build()).active = !committed;
            y += 25;
        }
        addRenderableWidget(Button.builder(Component.literal("取消连接"), button -> cancel.run())
                .bounds(width / 2 - 120, y, 240, 20).build()).active = !committed;
    }

    @Override public void render(GuiGraphics graphics, int mouseX, int mouseY, float delta) {
        renderDirtBackground(graphics);
        graphics.drawCenteredString(font, title, width / 2, height / 2 - 90, 0xFFFFFF);
        graphics.drawWordWrap(font, Component.literal(status), Math.max(20, width / 2 - 250),
                height / 2 - 60, Math.min(500, width - 40), 0xCCCCCC);
        super.render(graphics, mouseX, mouseY, delta);
    }

    @Override public void onClose() { if (!committed) cancel.run(); }
    @Override public boolean isPauseScreen() { return false; }
}
