package imsng.player_to_player.mixin;

import imsng.player_to_player.group.GroupRuntime;
import imsng.player_to_player.group.LoginLoadGuard;
import net.minecraft.network.Connection;
import net.minecraft.server.level.ServerPlayer;
import net.minecraft.server.network.CommonListenerCookie;
import net.minecraft.server.players.PlayerList;
import org.spongepowered.asm.mixin.Mixin;
import org.spongepowered.asm.mixin.injection.At;
import org.spongepowered.asm.mixin.injection.Inject;
import org.spongepowered.asm.mixin.injection.callback.CallbackInfo;

/**
 * 登录窗口（P0-3 诊断/兜底）的权威开启点。
 *
 * <p>{@code PlayerList.placeNewPlayer} 在同一服务端线程上连续完成
 * {@code load(playerdata)} → {@code setServerLevel} → {@code addNewPlayer}，
 * 是"登录锚点 + 登录期区块加载"的准确边界。窗口从这里开启、由
 * {@code GroupServerHooks} 的 JOIN 回调结束：
 * <ul>
 *   <li>比 {@code ServerPlayer} 构造器更精确：死亡重生只走构造器、不走
 *       {@code placeNewPlayer}，因此不会产生假的 120 秒登录窗口；</li>
 *   <li>比 JOIN 更早：playerdata 读取与 addNewPlayer 触发的区块加载都在窗口内。</li>
 * </ul>
 * 构造器内另有只覆盖 {@code fudgeSpawnLocation} 的短窗口（{@code ServerPlayerMixin}）。
 */
@Mixin(PlayerList.class)
public abstract class PlayerListMixin {

    @Inject(
            method = "placeNewPlayer(Lnet/minecraft/network/Connection;"
                    + "Lnet/minecraft/server/level/ServerPlayer;"
                    + "Lnet/minecraft/server/network/CommonListenerCookie;)V",
            at = @At("HEAD")
    )
    private void player_to_player$beginLoginWindow(Connection connection,
                                                   ServerPlayer player,
                                                   CommonListenerCookie cookie,
                                                   CallbackInfo ci) {
        if (player == null || !GroupRuntime.isManagedLevel(player.serverLevel())) {
            return;
        }
        // HEAD 时 playerdata 尚未读取，位置仍是构造器初始值（共享出生点），仅作日志锚点；
        // JOIN 日志会打印读取后的权威位置。
        LoginLoadGuard.beginLogin(player.getUUID(),
                player.getGameProfile().getName(),
                player.serverLevel().dimension().location().toString(),
                player.chunkPosition().x, player.chunkPosition().z);
    }
}
