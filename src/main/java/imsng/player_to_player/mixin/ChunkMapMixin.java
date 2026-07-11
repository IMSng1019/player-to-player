package imsng.player_to_player.mixin;

import imsng.player_to_player.config.GlobalConfig;
import imsng.player_to_player.core.NodeContext;
import imsng.player_to_player.group.ChunkClaimClient;
import imsng.player_to_player.group.GroupRuntime;
import imsng.player_to_player.group.PresyncStore;
import imsng.player_to_player.registry.ChunkKey;
import net.minecraft.Util;
import net.minecraft.nbt.CompoundTag;
import net.minecraft.server.level.ChunkMap;
import net.minecraft.server.level.ServerLevel;
import net.minecraft.world.level.ChunkPos;
import net.minecraft.world.level.chunk.ChunkAccess;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.spongepowered.asm.mixin.Final;
import org.spongepowered.asm.mixin.Mixin;
import org.spongepowered.asm.mixin.Shadow;
import org.spongepowered.asm.mixin.Unique;
import org.spongepowered.asm.mixin.injection.At;
import org.spongepowered.asm.mixin.injection.Inject;
import org.spongepowered.asm.mixin.injection.callback.CallbackInfoReturnable;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * 区块加载门控 + 物理服务端存盘取消（Phase 2 核心 Mixin，规范"特殊的区块加载"）。
 *
 * <h2>注入点一：{@code readChunk(ChunkPos)} @RETURN —— 主客户端集成服务端的申请门控</h2>
 * readChunk 是原版"从磁盘读区块 NBT"的唯一入口（scheduleChunkLoad 调用；已用
 * javap 核实为 private、返回 {@code CompletableFuture<Optional<CompoundTag>>}）。
 * 世代管线的一切后续（ChunkSerializer.read、各 ChunkStatus 生成步骤）都挂在这个
 * future 之后 —— 在 RETURN 处把它替换成"先申请占用"的组合 future，即可让
 * <b>加载与生成整体</b>受区块注册表管辖：
 * <ul>
 *   <li>授予 + 服务端有数据 → 经控制连接拉取权威 NBT，走与磁盘读取相同的
 *       {@code upgradeChunkTag} 升级管线（{@link ChunkMapAccess}）；</li>
 *   <li>授予 + 无数据 → 沿用原 future（本地缓存读，通常为空 → 按种子生成）；</li>
 *   <li>被拒 → future 悬置，ChunkClaimClient 按间隔重试直至授予 —— 区块保持
 *       未加载（玩家视野缺口），与规范"该区块不加载"一致。</li>
 * </ul>
 * 选 @RETURN 而非 @HEAD 取消重写：原 future 保留了原版 read + 升级链，
 * "无数据走本地"分支直接复用，避免手工复刻 {@code Util.backgroundExecutor()}
 * 升级调度的细节；代价只是被拒时多一次无害的本地磁盘读。
 * <p>
 * 门控条件 {@link GroupRuntime#isManagedLevel}（一次 volatile 读）：单人游戏、
 * 服务端、未接管的集成服务端一律走原版路径，零行为差异。
 *
 * <h2>注入点二：{@code save(ChunkAccess)} @HEAD —— 物理服务端存盘取消</h2>
 * 物理服务端在玩家登录期间会加载（乃至生成）玩家周边区块，这些<b>内存副本
 * 不参与演算、注定陈旧</b>。若放任原版的卸载存盘/自动存盘，会发生：
 * 主客户端上行了新数据 → IOWorker 写入 MCA → 服务端稍后卸载陈旧内存区块 →
 * 再次写入同一位置 → <b>新数据被旧数据覆盖</b>。在 save 的 HEAD 直接返回 false
 * （"未发生写入"），使 MCA 的唯一写入来源收敛为客户端上行
 * （server.ChunkDataHandlers），彻底消除覆盖竞态。
 * 门控与世界 tick 挂起同款（SERVER 模式 + suspendWorldTick），关掉配置即回原版。
 * <p>
 * <b>已知风险/边界</b>：
 * <ul>
 *   <li>save 取消后物理服务端登录期生成的区块不落盘 —— 区块生成是种子确定性的，
 *       客户端申请到该区块时会自行生成同样的地形，无数据损失；</li>
 *   <li>POI/实体（poi/、entities/ 目录）走独立存储，不经 ChunkStorage.write，
 *       Phase 2 不同步（箱内物品等方块实体在区块 NBT 内<b>不受影响</b>），
 *       游离实体的跨端同步在 Phase 3 预同步中处理；</li>
 *   <li>readChunk/save 均为私有方法且无重载歧义，签名已 javap 核实。</li>
 * </ul>
 */
@Mixin(ChunkMap.class)
public abstract class ChunkMapMixin {

    @Unique
    private static final Logger player_to_player$LOGGER =
            LoggerFactory.getLogger("player_to_player/mixin");

    /** 所属维度（mojmap: ChunkMap#level，package-private final，javap 已核实）。 */
    @Shadow
    @Final
    ServerLevel level;

    /** 物理服务端存盘取消只在首次生效时打日志，避免每次卸载刷屏。 */
    @Unique
    private static volatile boolean player_to_player$saveBlockLogged = false;

    // ------------------------------------------------- 注入点一：加载门控

    @Inject(method = "readChunk", at = @At("RETURN"), cancellable = true)
    private void player_to_player$gateChunkLoad(
            ChunkPos pos, CallbackInfoReturnable<CompletableFuture<Optional<CompoundTag>>> cir) {
        if (!GroupRuntime.isManagedLevel(this.level)) {
            return; // 非组接管的世界：原版路径
        }
        ChunkClaimClient claims = GroupRuntime.claims();
        if (claims == null) {
            return; // 接管解除中的窗口期：放行原版（此时服务器已在停机）
        }
        ChunkKey key = new ChunkKey(this.level.dimension().location().toString(), pos.x, pos.z);
        CompletableFuture<Optional<CompoundTag>> localRead = cir.getReturnValue();
        // 组合：申请（授予前悬置）→ 预同步暂存优先（Phase 3 合并接管方：A 经 P2P
        // 直发的最终态，与服务端存档同源且省一次往返）→ 有服务端数据则拉取并升级，
        // 否则用本地读结果。各阶段完成线程为 Netty 事件循环/io 池；NBT 升级
        // （DataFixer）转 Util.backgroundExecutor() —— 与原版 readChunk 的升级调度一致。
        cir.setReturnValue(claims.claimWithRetry(key).thenCompose(outcome -> {
            if (!outcome.hasServerData()) {
                CompoundTag staged = PresyncStore.take(key);
                if (staged == null) {
                    return localRead;
                }
                return CompletableFuture.completedFuture(Optional.of(staged)).thenApplyAsync(
                        opt -> opt.map(tag ->
                                ((ChunkMapAccess) this).player_to_player$upgradeChunkTag(tag)),
                        Util.backgroundExecutor());
            }
            // fetchChunkData 内部同样先查预同步暂存，未命中才走 CHUNK_DATA_REQUEST
            return claims.fetchChunkData(key).thenApplyAsync(
                    opt -> opt.map(tag ->
                            ((ChunkMapAccess) this).player_to_player$upgradeChunkTag(tag)),
                    Util.backgroundExecutor());
        }));
    }

    // -------------------------------------------- 注入点二：物理服务端存盘取消

    @Inject(method = "save", at = @At("HEAD"), cancellable = true)
    private void player_to_player$blockPhysicalServerSave(
            ChunkAccess chunk, CallbackInfoReturnable<Boolean> cir) {
        NodeContext ctx = NodeContext.get();
        GlobalConfig config = ctx.config();
        if (config == null || !ctx.isServer() || !config.suspendWorldTick) {
            return; // 客户端（含集成服务端）与未挂起的服务端：原版存盘
        }
        if (!player_to_player$saveBlockLogged) {
            player_to_player$saveBlockLogged = true;
            player_to_player$LOGGER.info(
                    "物理服务端区块存盘已取消（suspendWorldTick=true）："
                            + "MCA 的唯一写入来源为主客户端上行，防止陈旧内存态覆盖");
        }
        cir.setReturnValue(false);
    }
}
