package imsng.player_to_player.client.session;

import net.minecraft.client.gui.GuiGraphics;
import net.minecraft.client.gui.components.Button;
import net.minecraft.client.gui.screens.Screen;
import net.minecraft.network.chat.Component;

/**
 * 物理服引导与分离排空期间的等待界面。保持网络和客户端 tick，但不让输入落到
 * 临时物理世界；玩家可以明确退出，不会以能走动的临时世界误判最终加入已经完成。
 */
public final class JoinProgressScreen extends Screen {
    public JoinProgressScreen() { super(Component.literal("正在准备组世界")); }

    @Override protected void init() {
        addRenderableWidget(Button.builder(Component.literal("退出连接"), button ->
                WorldSession.cancelWaiting()).bounds(width / 2 - 75, height / 2 + 35, 150, 20).build());
    }

    @Override public void render(GuiGraphics graphics, int mouseX, int mouseY, float delta) {
        renderDirtBackground(graphics);
        graphics.drawCenteredString(font, title, width / 2, height / 2 - 35, 0xFFFFFF);
        graphics.drawCenteredString(font, Component.literal(SwitchTelemetry.displayStatus()),
                width / 2, height / 2 - 10, 0xCCCCCC);
        super.render(graphics, mouseX, mouseY, delta);
    }

    @Override public boolean shouldCloseOnEsc() { return false; }
    @Override public boolean isPauseScreen() { return false; }
}
