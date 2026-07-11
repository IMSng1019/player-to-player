package imsng.player_to_player.mixin;

import imsng.player_to_player.config.GlobalConfig;
import imsng.player_to_player.core.NodeContext;
import net.minecraft.server.level.ServerLevel;
import net.minecraft.world.entity.Entity;
import net.minecraft.world.level.entity.PersistentEntitySectionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.spongepowered.asm.mixin.Final;
import org.spongepowered.asm.mixin.Mixin;
import org.spongepowered.asm.mixin.Shadow;
import org.spongepowered.asm.mixin.Unique;
import org.spongepowered.asm.mixin.injection.At;
import org.spongepowered.asm.mixin.injection.Inject;
import org.spongepowered.asm.mixin.injection.callback.CallbackInfo;

import java.util.function.BooleanSupplier;

/**
 * 服务端世界 tick 挂起（本模组核心特性，规范出处：player_to_player-prompt.txt
 * "服务端不再承担世界 tick 运算"；工程决策见 docs/DESIGN.md "世界 tick 挂起"一节）。
 * <p>
 * <b>注入点选择理由</b>：选择 {@code ServerLevel.tick(BooleanSupplier)} 而不是
 * {@code MinecraftServer.tickChildren}——后者除了逐维度调用 ServerLevel.tick 之外，
 * 还负责网络连接 tick（ServerConnectionListener.tick）、玩家列表心跳
 * （PlayerList tick / KeepAlive）、命令函数、聊天等"服务面"逻辑，整体跳过会导致
 * 玩家无法登录、心跳超时被踢出。
 * <p>
 * <b>但 ServerLevel.tick 并非纯世界演算</b>（此前认为"整体取消是安全的"是错误结论，
 * 详见 docs/review-findings.md critical 项）：它内部还包含两个"区块与实体服务面"调用——
 * <ol>
 *   <li>{@code this.getChunkSource().tick(hasTimeLeft, true)}：驱动 DistanceManager
 *       ticket 更新、区块加载/卸载、向玩家发送区块包、定期存盘；</li>
 *   <li>{@code this.entityManager.tick()}：处理实体分区（entity section）装载队列，
 *       区块内实体依赖它完成入世。</li>
 * </ol>
 * 若在 HEAD 整体 cancel，服务端将永远不给玩家发区块，玩家登录后卡死在
 * "Loading terrain" 界面。因此本 mixin 在取消前<b>手动保留这两个服务面调用</b>：
 * 其中 ServerChunkCache.tick 的第二参数传 {@code false}（原版传 true），
 * 使其跳过内部私有方法 {@code tickChunks()}（自然生成、区块随机演算、tickChunk 等
 * 世界演算入口），但保留 distanceManager 更新与 chunkMap.tick（发包/加载/卸载/存盘）
 * ——已用 javap 对 1.20.4 mojmap 核实上述两个方法与 entityManager 字段的签名。
 * 由此实现的效果：区块服务与实体装载存活（玩家能登录、能看到区块与实体），
 * 而时间、天气、生物生成、随机 tick、方块/流体计划 tick、袭击、实体 tick、
 * 方块实体 tick 等世界演算全部挂起——正是"运算下放主客户端"的语义。
 * <p>
 * <b>配置门控</b>：仅当本节点为 SERVER 模式（{@link NodeContext#isServer()}）且总配置
 * {@link GlobalConfig#suspendWorldTick} 为 true 时才取消 tick。
 * <b>回退方式</b>：在 {@code player-to-player/config.json} 中把 {@code suspendWorldTick}
 * 设为 false 即可完全恢复原版行为（便于排障对比）。
 * <p>
 * <b>已知风险</b>：
 * <ul>
 *   <li>时间/天气在服务端侧冻结——Phase 2+ 由主客户端演算后经协议回写，属预期行为；</li>
 *   <li>实体虽能随区块装载入世，但 {@code entityTickList} 不推进，实体在服务端
 *       视角静止；服务端仅作为区块注册表与 MCA 写回的权威，不依赖这些演算结果；</li>
 *   <li>挂起路径下 ServerChunkCache.tick 每 tick 以 tickChunks=false 被调用一次，
 *       与原版调用次数一致，不存在重复 tick；但若其他模组也注入 ServerLevel.tick
 *       或 ServerChunkCache.tick 期望原版参数，会与本 mixin 冲突——该服务端本就
 *       要求按前缀约定只装 {@code server-} 前缀模组，风险可控。</li>
 * </ul>
 */
@Mixin(ServerLevel.class)
public abstract class ServerLevelMixin {

    @Unique
    private static final Logger player_to_player$LOGGER =
            LoggerFactory.getLogger("player_to_player/mixin");

    /** 只在首次生效时打一条日志，避免每 tick 刷屏。 */
    @Unique
    private static volatile boolean player_to_player$suspendLogged = false;

    /**
     * 实体分区管理器（mojmap: ServerLevel#entityManager，private final，
     * 类型 PersistentEntitySectionManager&lt;Entity&gt;，已用 javap 核实）。
     * 挂起世界 tick 时仍需手动调用其 tick() 处理实体装载队列。
     */
    @Shadow
    @Final
    private PersistentEntitySectionManager<Entity> entityManager;

    /**
     * 在 ServerLevel.tick 头部按配置挂起世界演算，但保留区块与实体服务面。
     * <p>
     * 显式写全方法描述符 {@code tick(Ljava/util/function/BooleanSupplier;)V}：
     * ServerLevel 中还有 tickTime/tickChunk/tickCustomSpawners 等同前缀方法，
     * 用全描述符避免任何歧义（已用 javap 对 mojmap 1.20.4 核实签名）。
     */
    @Inject(
            method = "tick(Ljava/util/function/BooleanSupplier;)V",
            at = @At("HEAD"),
            cancellable = true
    )
    private void player_to_player$suspendWorldTick(BooleanSupplier hasTimeLeft, CallbackInfo ci) {
        NodeContext ctx = NodeContext.get();
        GlobalConfig config = ctx.config();
        // 极早期调用（配置尚未由 P2PBootstrap 装载）时 config 可能为 null，
        // 此时一律放行原版行为，宁可多 tick 几下也不能让世界处于未定义状态。
        if (config == null) {
            return;
        }
        if (ctx.isServer() && config.suspendWorldTick) {
            if (!player_to_player$suspendLogged) {
                player_to_player$suspendLogged = true;
                player_to_player$LOGGER.info(
                        "服务端世界 tick 已挂起（suspendWorldTick=true）：世界演算下放至主客户端，"
                                + "仅保留区块服务（加载/发包/存盘）与实体装载；"
                                + "如需恢复原版行为请在 config.json 中关闭该项");
            }
            // 取消前手动保留两个服务面调用，否则玩家会卡死在 Loading terrain：
            // 1) 区块服务：第二参数 tickChunks=false 跳过 tickChunks()（自然生成/
            //    区块随机演算等世界演算），但保留 DistanceManager ticket 更新与
            //    chunkMap.tick（区块加载/卸载/向玩家发包/存盘）。
            ((ServerLevel) (Object) this).getChunkSource().tick(hasTimeLeft, false);
            // 2) 实体装载：处理实体分区装载队列，让实体能随区块正常入世。
            this.entityManager.tick();
            ci.cancel();
        }
    }
}
