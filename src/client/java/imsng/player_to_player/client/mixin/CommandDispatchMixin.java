package imsng.player_to_player.client.mixin;

import imsng.player_to_player.group.CommandRelayClient;
import net.minecraft.network.chat.LastSeenMessages;
import net.minecraft.network.protocol.game.ServerboundChatCommandPacket;
import net.minecraft.server.level.ServerPlayer;
import net.minecraft.server.network.ServerGamePacketListenerImpl;
import org.spongepowered.asm.mixin.Mixin;
import org.spongepowered.asm.mixin.Shadow;
import org.spongepowered.asm.mixin.injection.At;
import org.spongepowered.asm.mixin.injection.Inject;
import org.spongepowered.asm.mixin.injection.callback.CallbackInfo;

/**
 * 玩家指令的拦截面（Phase 4 全量上链；口径见
 * {@code docs/plans/2026-10-15-command-routing-fix-plan.md} 附录 P）。
 *
 * <p>注入 {@code ServerGamePacketListenerImpl.performChatCommand}（私有）里
 * <b>本地执行那一次调用之前</b>的 {@code Commands.performCommand} 调用点
 * （1.20.4 的字节码：{@code server.getCommands().performCommand(parseResults, command)}，
 * 已用 javap 核实）。选这个位置而不是方法 HEAD，是为了让原版的签名/LastSeenMessages
 * 校验（{@code collectSignedArguments}）与 {@code handleMessageDecodeFailure} 照常执行 ——
 * 取消的只是"本地执行"这一步。</p>
 *
 * <p>为什么是数据包入口而不是 {@code Commands.performCommand}：只有"玩家在聊天栏敲指令"
 * 会走到这里（聊天栏与 F3+F4/F3+N 都经 {@code ServerboundChatCommandPacket}）。
 * 告示牌、命令方块、数据包函数与第三方程序化调用不经过这里，它们照旧在本地按
 * 服务端下发的真实权限位执行（{@code LocalPermissions}）—— 这正是定稿口径
 * "本地权限位用于未拦截路径"的含义。</p>
 *
 * <p>门控全在 {@link CommandRelayClient#interceptPlayerCommand} 里：非被接管的集成
 * 服务端（物理服务端、单人游戏）直接返回 false，原版行为零差异；返回 true 才取消
 * 本地执行 —— 该指令已上链，服务端聚合后的回执会在稍后显示。</p>
 */
@Mixin(ServerGamePacketListenerImpl.class)
public abstract class CommandDispatchMixin {

    /** 集成服务端玩家（房主本人或其组内经隧道加入的副客户端）。 */
    @Shadow
    public ServerPlayer player;

    /** 本地执行前的拦截：上链成功即取消（上链失败一律放行，绝不吞指令）。 */
    @Inject(
            method = "performChatCommand(Lnet/minecraft/network/protocol/game/ServerboundChatCommandPacket;Lnet/minecraft/network/chat/LastSeenMessages;)V",
            at = @At(value = "INVOKE",
                    target = "Lnet/minecraft/commands/Commands;performCommand(Lcom/mojang/brigadier/ParseResults;Ljava/lang/String;)V"),
            cancellable = true)
    private void p2p$routePlayerCommand(ServerboundChatCommandPacket packet,
                                        LastSeenMessages lastSeenMessages, CallbackInfo ci) {
        if (CommandRelayClient.interceptPlayerCommand(player, packet.command())) {
            ci.cancel();
        }
    }
}
