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

    EnvironmentPreflightScreen(Runnable cancel) {
        super(Component.literal("准备服务器环境"));
        this.cancel = cancel;
    }

    void status(String value) { status = value; }

    void requireRestart(String reason, ClientRuntimeProfile.Installation value) {
        status = reason + "\n" + value.limitation();
        installation = value;
        clearWidgets();
        init();
    }

    @Override protected void init() {
        int y = height / 2 + 40;
        if (installation != null) {
            if (installation.canLaunch()) {
                addRenderableWidget(Button.builder(Component.literal("用同步环境重启游戏"), button -> {
                    try {
                        installation.launch();
                        minecraft.stop();
                    } catch (Exception failure) {
                        status = "无法启动同步环境：" + failure.getMessage();
                    }
                }).bounds(width / 2 - 120, y, 240, 20).build());
                y += 25;
            }
            addRenderableWidget(Button.builder(Component.literal("打开已安装的游戏目录"), button ->
                    Util.getPlatform().openFile(installation.directory().toFile()))
                    .bounds(width / 2 - 120, y, 240, 20).build());
            y += 25;
        }
        addRenderableWidget(Button.builder(Component.literal("取消连接"), button -> cancel.run())
                .bounds(width / 2 - 120, y, 240, 20).build());
    }

    @Override public void render(GuiGraphics graphics, int mouseX, int mouseY, float delta) {
        renderDirtBackground(graphics);
        graphics.drawCenteredString(font, title, width / 2, height / 2 - 90, 0xFFFFFF);
        graphics.drawWordWrap(font, Component.literal(status), Math.max(20, width / 2 - 250),
                height / 2 - 60, Math.min(500, width - 40), 0xCCCCCC);
        super.render(graphics, mouseX, mouseY, delta);
    }

    @Override public void onClose() { cancel.run(); }
    @Override public boolean isPauseScreen() { return false; }
}
