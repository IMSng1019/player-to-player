package imsng.player_to_player.mixin;

import imsng.player_to_player.config.GlobalConfig;
import imsng.player_to_player.core.NodeContext;
import net.minecraft.server.MinecraftServer;
import net.minecraft.server.level.progress.ChunkProgressListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.spongepowered.asm.mixin.Mixin;
import org.spongepowered.asm.mixin.Unique;
import org.spongepowered.asm.mixin.injection.At;
import org.spongepowered.asm.mixin.injection.Inject;
import org.spongepowered.asm.mixin.injection.callback.CallbackInfo;

/**
 * 出生点常加载区块禁用（规范出处：player_to_player-prompt.txt
 * "装载了该模组的服务器不加载出生点常加载区块"；见 docs/DESIGN.md "出生点常加载禁用"）。
 * <p>
 * <b>注入点选择理由</b>：{@code MinecraftServer.prepareLevels(ChunkProgressListener)}
 * 是原版启动流程中唯一负责"出生点预热"的方法：它给出生点挂 START 级 ticket
 * （常加载来源）、阻塞等待周边区块全部就绪、再启动强制区块（forced chunks）。
 * 在 HEAD 直接取消即可一并跳过这三件事——本模组下所有区块的加载权都由客户端经
 * 区块注册表申领，服务端不应自行加载任何区块。相比之下若去改
 * DistanceManager/TicketType 等更深层结构，侵入面大且版本脆弱。
 * <p>
 * <b>配置门控</b>：仅当 SERVER 模式且 {@link GlobalConfig#disableSpawnChunks} 为 true
 * 时取消；把 {@code player-to-player/config.json} 中 {@code disableSpawnChunks}
 * 设为 false 即恢复原版出生点常加载。config 为 null（极早期调用）时放行原版行为。
 * <p>
 * <b>已知风险</b>：
 * <ul>
 *   <li>跳过后 {@code /forceload} 指定的强制区块在启动阶段不会恢复加载
 *       （原版在 prepareLevels 尾部处理）——本模组语义下服务端本就不 tick 区块，
 *       属可接受偏差，Phase 2+ 若需要可由注册表侧补偿；</li>
 *   <li>首位玩家加入时出生点区块未预热，登录瞬间的区块由主客户端演算供给，
 *       这正是本模组的设计路径；</li>
 *   <li>prepareLevels 是 private 方法，mixin 注入不受访问级别限制，
 *       已用 javap 对 mojmap 1.20.4 核实其签名为
 *       {@code prepareLevels(Lnet/minecraft/server/level/progress/ChunkProgressListener;)V}。</li>
 * </ul>
 */
@Mixin(MinecraftServer.class)
public abstract class MinecraftServerMixin {

    @Unique
    private static final Logger player_to_player$LOGGER =
            LoggerFactory.getLogger("player_to_player/mixin");

    /**
     * 在 prepareLevels 头部按配置整体跳过出生点常加载与等待。
     */
    @Inject(
            method = "prepareLevels(Lnet/minecraft/server/level/progress/ChunkProgressListener;)V",
            at = @At("HEAD"),
            cancellable = true
    )
    private void player_to_player$skipSpawnPreparation(ChunkProgressListener listener, CallbackInfo ci) {
        NodeContext ctx = NodeContext.get();
        GlobalConfig config = ctx.config();
        // 配置尚未装载（理论上 prepareLevels 晚于模组初始化，此处仅作防御）时放行原版。
        if (config == null) {
            return;
        }
        if (ctx.isServer() && config.disableSpawnChunks) {
            player_to_player$LOGGER.info(
                    "已跳过出生点常加载区块的预热与等待（disableSpawnChunks=true）："
                            + "区块加载全部交由客户端经区块注册表申领");
            ci.cancel();
        }
    }
}
