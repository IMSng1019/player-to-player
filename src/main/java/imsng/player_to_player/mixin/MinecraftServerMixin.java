package imsng.player_to_player.mixin;

import imsng.player_to_player.config.GlobalConfig;
import imsng.player_to_player.core.NodeContext;
import imsng.player_to_player.group.GroupControlTasks;
import imsng.player_to_player.group.GroupRuntime;
import imsng.player_to_player.world.SpawnChunkPolicy;
import net.minecraft.server.MinecraftServer;
import net.minecraft.server.level.progress.ChunkProgressListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.spongepowered.asm.mixin.Mixin;
import org.spongepowered.asm.mixin.Unique;
import org.spongepowered.asm.mixin.injection.At;
import org.spongepowered.asm.mixin.injection.Inject;
import org.spongepowered.asm.mixin.injection.callback.CallbackInfo;
import org.spongepowered.asm.mixin.injection.callback.CallbackInfoReturnable;

/**
 * 出生点常加载区块禁用（规范出处：player_to_player-prompt.txt
 * "装载了该模组的服务器不加载出生点常加载区块"；见 docs/DESIGN.md "出生点常加载禁用"）。
 * <p>
 * <b>覆盖范围</b>：本 mixin 只负责<b>启动预热</b>这一条入口（{@code prepareLevels}）。
 * 每次登录还会经过 {@code ServerPlayer.fudgeSpawnLocation} 同步加载共享出生点区块，
 * 受管组世界的该入口由 {@code ServerPlayerMixin} 取消，避免读取玩家存档前加载无关的
 * 共享出生点。运行时 {@code /setworldspawn} 重新添加 START 票据的入口则由
 * {@code ServerLevelMixin} 拦截；三处分别保护启动、登录和出生点变更路径。
 * <p>
 * <b>注入点选择理由</b>：{@code MinecraftServer.prepareLevels(ChunkProgressListener)}
 * 是原版启动流程中唯一负责"出生点预热"的方法：它给出生点挂 START 级 ticket
 * （常加载来源）、阻塞等待周边区块全部就绪、再启动强制区块（forced chunks）。
 * 在 HEAD 直接取消即可一并跳过这三件事，避免与玩家实际位置无关的常驻出生区块。
 * 玩家真实出生、重生和视距所需的加载仍由各自路径处理。相比之下若去改
 * DistanceManager/TicketType 等更深层结构，侵入面大且版本脆弱。
 * <p>
 * <b>配置门控</b>：SERVER / PROXY_SERVER 模式且
 * {@link GlobalConfig#disableSpawnChunks} 为 true 时取消；这两种模式可通过关闭该配置
 * 恢复原版行为，配置尚未装载时也放行。CLIENT 模式下，组世界从预置阶段起始终跳过
 * 预热，普通单人世界不受影响；具体判定与运行期入口共用 {@link SpawnChunkPolicy}。
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
     * 未进入同步区块等待时由正常服务器轮询推进迁移控制任务；每次最多执行一项，
     * 原版任务队列保持独立，区块等待入口也不会因此重入游戏指令。
     */
    @Inject(method = "pollTask()Z", at = @At("HEAD"), cancellable = true)
    private void player_to_player$pollMigrationControlTasks(CallbackInfoReturnable<Boolean> cir) {
        if (GroupControlTasks.poll((MinecraftServer) (Object) this)) {
            cir.setReturnValue(true);
        }
    }

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
        // GroupRuntime 在打开组世界前 arm，早于 SERVER_STARTED 的实际接管；不能只
        // 检查 isManagedLevel，否则启动窗口内仍会留下原版 START 常驻票据。
        if (!SpawnChunkPolicy.shouldDisable(ctx.mode(), GroupRuntime.isArmedOrActive(), ctx.config())) {
            return;
        }
        if (ctx.isClient()) {
            // 1.20.4 原版会挂半径 11 的 START 票据并等待 441（21×21）个 ticking
            // 区块生成完成；跳过整段预热，避免无关远端区块在玩家实际登录前被加载。
            player_to_player$LOGGER.info("组客户端集成服务端：已跳过出生点常加载区块预热");
        } else {
            player_to_player$LOGGER.info(
                    "已跳过出生点常加载区块的预热与等待（mode={}，disableSpawnChunks=true）",
                    ctx.mode().id());
        }
        ci.cancel();
    }
}
