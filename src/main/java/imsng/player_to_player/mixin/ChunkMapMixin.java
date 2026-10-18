package imsng.player_to_player.mixin;

import imsng.player_to_player.config.GlobalConfig;
import imsng.player_to_player.core.NodeContext;
import imsng.player_to_player.group.ChunkClaimClient;
import imsng.player_to_player.group.GroupRuntime;
import imsng.player_to_player.group.LoginAnchorPolicy;
import imsng.player_to_player.group.LoginLoadGuard;
import imsng.player_to_player.group.PresyncStore;
import imsng.player_to_player.registry.ChunkKey;
import net.minecraft.Util;
import net.minecraft.nbt.CompoundTag;
import net.minecraft.server.level.ChunkHolder;
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
import org.spongepowered.asm.mixin.injection.callback.CallbackInfo;
import org.spongepowered.asm.mixin.injection.callback.CallbackInfoReturnable;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * 区块加载门控 + 物理服务端存盘取消（Phase 2 核心 Mixin，规范"特殊的区块加载"）。
 *
 * <h2>注入点一：{@code scheduleUnload(long, ChunkHolder)} @HEAD —— 预卸载 ACK 屏障</h2>
 * 原版取得 holder.chunkToSave 前，把物理服最终写回与释放确认 future 作为 save dependency
 * 加入 holder。ACK 前原版卸载链不会继续；ACK 后仍完全沿用原版保存与内存移除流程。
 *
 * <h2>注入点二：{@code readChunk(ChunkPos)} @RETURN —— 主客户端集成服务端的申请门控</h2>
 * readChunk 是原版"从磁盘读区块 NBT"的唯一入口（scheduleChunkLoad 调用；已用
 * javap 核实为 private、返回 {@code CompletableFuture<Optional<CompoundTag>>}）。
 * 世代管线的一切后续（ChunkSerializer.read、各 ChunkStatus 生成步骤）都挂在这个
 * future 之后 —— 在 RETURN 处把它替换成"先申请占用"的组合 future，即可让
 * <b>加载与生成整体</b>受区块注册表管辖：
 * <ul>
 *   <li>授予 + 服务端有数据 → 经控制连接拉取权威 NBT，走与磁盘读取相同的
 *       {@code upgradeChunkTag} 升级管线（{@link ChunkMapAccess}）；</li>
 *   <li>授予 + 无数据 → 沿用原 future（本地缓存读，通常为空 → 按种子生成）；
 *       <b>前提是本地存档世界身份已校验</b>（{@link GroupRuntime#isLocalWorldIdentityVerified()}），
 *       否则按无缓存处理走种子生成 —— 详见 {@code player_to_player$localReadOrEmpty}；</li>
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
 * <h2>注入点三：{@code save(ChunkAccess)} @HEAD —— 物理服务端存盘取消</h2>
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

    /** 身份门控关闭本地磁盘回退读时只打一次 WARN，避免每区块刷屏。 */
    @Unique
    private static volatile boolean player_to_player$identityGateLogged = false;

    // ----------------------------------------------- 注入点一：预卸载屏障

    /**
     * 原版把区块放入 pendingUnloads 前，先把物理服释放确认 future 并入 holder 的
     * chunkToSave。这里不取消 scheduleUnload：ACK 完成后原版继续执行保存、实体卸载与
     * 内存移除；失败时依赖保持未完成，区块自然停留在预卸载状态。
     */
    @Inject(method = "scheduleUnload", at = @At("HEAD"))
    private void player_to_player$installPreUnloadBarrier(
            long packedPos, ChunkHolder holder, CallbackInfo ci) {
        if (!GroupRuntime.isManagedLevel(this.level)) {
            return;
        }
        CompletableFuture<ChunkAccess> originalChunkToSave = holder.getChunkToSave();
        CompletableFuture<Void> dependency = GroupRuntime.beginChunkPreUnload(
                this.level, originalChunkToSave);
        ((ChunkHolderSaveDependencyAccess) holder).player_to_player$addSaveDependency(
                "player_to_player:pre_unload", dependency);
    }

    // ------------------------------------------------- 注入点二：加载门控

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
        CompletableFuture<Optional<CompoundTag>> composed = claims.claimWithRetry(key)
                .thenCompose(outcome -> {
                    if (!outcome.hasServerData()) {
                        CompoundTag staged = PresyncStore.take(key);
                        if (staged == null) {
                            return player_to_player$localReadOrEmpty(localRead, key);
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
                });
        cir.setReturnValue(player_to_player$guardLoginWait(claims, key, composed));
    }

    /**
     * 登录期加载看门狗（P0-3）。
     * <p>
     * 被拒的 claim 会让 future 一直悬置，而原版登录会在服务端主线程同步
     * {@code join()} 这个区块 future（{@code ServerChunkCache.getChunk} 经
     * {@code managedBlock} 等），历史事故里主线程被挂住 295 秒。
     * {@code readChunk} 由区块调度在主线程发起，因此这里对"登录窗口 + 服务端主线程"
     * 的组合加载武装：
     * <ul>
     *   <li>5 秒 WARN：区块键、阻塞组、玩家/维度/锚点、调用栈；</li>
     *   <li>60 秒硬超时：<b>仅当同一个登录窗口仍然有效</b>时，以
     *       {@link LoginLoadGuard.LoginLoadTimeoutException} 异常结束本次加载。</li>
     * </ul>
     * <b>不能用 IOException</b>：原版 {@code scheduleChunkLoad} 会把 IOException
     * 解释为读盘失败并生成空 ProtoChunk，随后按种子生成本地数据（可能覆盖其他组）。
     * 非 IOException 的 CompletionException 会走原版"标记失败 + rethrow"分支。
     * JOIN 之后窗口结束，定时器不再硬失败（此时服务端主线程已不可能被这次登录阻塞）。
     */
    @Unique
    private CompletableFuture<Optional<CompoundTag>> player_to_player$guardLoginWait(
            ChunkClaimClient claims, ChunkKey key,
            CompletableFuture<Optional<CompoundTag>> composed) {
        if (!LoginLoadGuard.isLoginPhase()) {
            return composed;
        }
        LoginLoadGuard.recordClaim(key);
        // 共享出生点区块在登录期出现 = P0-1 没有覆盖到的路径，留下 WARN + 调用栈
        net.minecraft.core.BlockPos spawn = this.level.getSharedSpawnPos();
        if (LoginAnchorPolicy.isSharedSpawnChunk(
                spawn.getX(), spawn.getZ(), key.x(), key.z())) {
            LoginLoadGuard.warnSpawnChunkClaim(key);
        }
        boolean serverThread = this.level.getServer() != null
                && this.level.getServer().isSameThread();
        if (!LoginAnchorPolicy.shouldArmSyncWatchdog(true, serverThread)) {
            return composed;
        }
        return LoginLoadGuard.guard(key, composed,
                () -> player_to_player$describeClaimDiagnostics(claims, key), true);
    }

    /** 看门狗告警/超时时使用的只读 claim 诊断。 */
    @Unique
    private static String player_to_player$describeClaimDiagnostics(
            ChunkClaimClient claims, ChunkKey key) {
        ChunkClaimClient.ClaimDiagnostics diagnostics = claims.diagnostics(key);
        if (diagnostics == null) {
            return "阻塞组=未知（尚未收到拒绝应答或正在等待服务端/网络）";
        }
        return "阻塞组=" + diagnostics.blockingGroup()
                + " 阻塞区块=" + diagnostics.blockingChunk()
                + " 尝试=" + diagnostics.attempts()
                + " 首次拒绝=" + diagnostics.firstDeniedAtMillis() + "ms"
                + " 最近拒绝=" + diagnostics.lastDeniedAtMillis() + "ms";
    }

    /**
     * "服务端无数据"时的本地磁盘回退读 —— 前提是本地存档已完成世界身份校验
     * （{@link GroupRuntime#isLocalWorldIdentityVerified()}）。
     * <p>
     * 世界文件夹键只有 {@code IP+世界名}：服务端换世界后本地 {@code data/<世界名>/}
     * 里可能仍是<b>另一个世界</b>的 {@code region/*.mca}。此时回退读会把旧地形当作
     * 权威数据交给集成服务端，随后这些区块被上行写进服务端 MCA（双向污染）。
     * 校验未通过（未 arm / 服务端骨架给不出身份 / 接管解除窗口）时按"本地无缓存"
     * 处理：返回空结果，让原版按<b>本存档 level.dat 的种子</b>生成 —— 该 level.dat
     * 已经过 {@code LocalWorldLauncher} 校验或据服务端骨架重建，与物理服务端同源。
     *
     * @param localRead 原版 {@code readChunk} 的返回值（读本存档 region 文件）
     */
    @Unique
    private CompletableFuture<Optional<CompoundTag>> player_to_player$localReadOrEmpty(
            CompletableFuture<Optional<CompoundTag>> localRead, ChunkKey key) {
        if (GroupRuntime.isLocalWorldIdentityVerified()) {
            return localRead;
        }
        if (!player_to_player$identityGateLogged) {
            player_to_player$identityGateLogged = true;
            player_to_player$LOGGER.warn(
                    "本地存档世界身份未经校验，已禁用本地磁盘回退读（按无缓存处理，走种子生成）: {}",
                    key.asString());
        }
        return CompletableFuture.completedFuture(Optional.empty());
    }

    // -------------------------------------------- 注入点三：物理服务端存盘取消
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
