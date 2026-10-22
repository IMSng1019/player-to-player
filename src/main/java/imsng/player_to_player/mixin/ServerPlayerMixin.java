package imsng.player_to_player.mixin;

import com.mojang.authlib.GameProfile;
import imsng.player_to_player.config.GlobalConfig;
import imsng.player_to_player.core.NodeContext;
import imsng.player_to_player.group.GroupRuntime;
import imsng.player_to_player.group.LoginAnchorPolicy;
import imsng.player_to_player.group.LoginLoadGuard;
import net.minecraft.server.MinecraftServer;
import net.minecraft.server.level.ClientInformation;
import net.minecraft.server.level.ServerLevel;
import net.minecraft.server.level.ServerPlayer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.spongepowered.asm.mixin.Mixin;
import org.spongepowered.asm.mixin.Unique;
import org.spongepowered.asm.mixin.injection.At;
import org.spongepowered.asm.mixin.injection.Inject;
import org.spongepowered.asm.mixin.injection.callback.CallbackInfo;

/**
 * 受管组世界的登录锚点修复（P0，2026-10-18）。
 *
 * <h2>注入点一：{@code fudgeSpawnLocation(ServerLevel)} @HEAD — 取消出生点修正</h2>
 * 原版 {@code ServerPlayer} 构造器在<b>读 playerdata 之前</b>调用
 * {@code fudgeSpawnLocation}，它会同步 {@code level.getChunk(sharedSpawn)} 找安全出生点。
 * 服务端主线程在这个 {@code join()} 上等待区块生成，而受管组世界的加载门控
 * （{@code ChunkMapMixin.readChunk}）在共享出生点被其他组持有时会拒绝并悬置 future，
 * 于是主线程被无限期挂住（实测 295 秒静默卡死）。
 * <p>
 * 受管组世界本就<b>不应加载出生点常加载区块</b>（AGENTS.md L76 / prompt L44），
 * 因此直接取消该方法。真正的位置由后续 {@code PlayerList.placeNewPlayer} 的
 * {@code load(playerdata)} 设置；已用 1.20.4 字节码核实 {@code load} 在
 * {@code addNewPlayer}（区块 ticket）之前，所以不需要重排原版顺序。
 * <p>
 * 门控：{@link GroupRuntime#isManagedLevel} 一次 volatile 读 —— 专用服、单人游戏、
 * 未接管的集成服零行为差异。{@code disableSpawnChunks} 的逃生口语义只保留给专用服的
 * 启动预热；受管世界若恢复出生点加载会与 claim 门控直接冲突，因此这里不提供关闭路径。
 *
 * <h2>注入点二：{@code <init>} @HEAD — 开启登录看门狗窗口</h2>
 * 构造器早于 playerdata 读取，也就早于所有登录期区块加载（含 fudge）。在这里
 * 开启 {@link LoginLoadGuard} 窗口，5 秒 WARN / 60 秒硬超时才能覆盖登录全链路。
 * 构造器窗口在 RETURN 结束；真正登录时由 {@code PlayerListMixin} 重新开启窗口，
 * 再由 {@code GroupServerHooks} 的 JOIN 回调结束。
 */
@Mixin(ServerPlayer.class)
public abstract class ServerPlayerMixin {

    @Unique
    private static final Logger player_to_player$LOGGER =
            LoggerFactory.getLogger("player_to_player/mixin");

    /**
     * 受管组世界跳过原版出生点安全修正（构造器阶段，playerdata 尚未读取）。
     */
    @Inject(
            method = "fudgeSpawnLocation(Lnet/minecraft/server/level/ServerLevel;)V",
            at = @At("HEAD"),
            cancellable = true
    )
    private void player_to_player$skipSpawnFudge(ServerLevel level, CallbackInfo ci) {
        if (!GroupRuntime.isManagedLevel(level)) {
            return; // 专用服 / 单人 / 未接管集成服：原版行为
        }
        NodeContext ctx = NodeContext.get();
        GlobalConfig config = ctx.config();
        // config 为 null 属极早期防御：按默认开启处理（GlobalConfig#disableSpawnChunks 默认 true）
        boolean disableSpawnChunks = config == null || config.disableSpawnChunks;
        if (!LoginAnchorPolicy.shouldSkipSpawnFudge(true, disableSpawnChunks)) {
            return;
        }
        if (!disableSpawnChunks) {
            // 配置逃生口只对专用服的 prepareLevels 生效：受管世界若恢复出生点加载，
            // 会重新触发原点 23×23 依赖区块的跨组 claim 与主线程挂起（P0 事故）。
            player_to_player$LOGGER.warn(
                    "受管组世界 disableSpawnChunks=false，但登录仍跳过出生点 fudge: "
                            + "dimension={} spawn={}（否则会与区块 claim 门控冲突）",
                    level.dimension().location(), level.getSharedSpawnPos());
        }
        player_to_player$LOGGER.info(
                "受管组世界跳过原版出生点 fudge: dimension={} spawn={}",
                level.dimension().location(), level.getSharedSpawnPos());
        ci.cancel();
    }

    /**
     * 构造器级登录窗口：HEAD 开启、RETURN 结束，仅用于覆盖 fudgeSpawnLocation
     * 这条防御路径。若 fudge 真的被阻塞，构造器不会返回，窗口保持有效，看门狗可兜底；
     * 正常返回后窗口立即结束，死亡重生（同样走构造器、不走 JOIN）不会留下 120 秒假窗口。
     * 真正的登录窗口由 {@code PlayerListMixin} 在 placeNewPlayer HEAD 开启。
     * <p>HEAD 位于父类构造器调用之前，此时 JVM 尚未初始化 this；Mixin 要求处理器
     * 必须是 static。这里只读取构造器参数和静态运行期状态，不访问玩家实例。
     */
    @Inject(
            method = "<init>(Lnet/minecraft/server/MinecraftServer;Lnet/minecraft/server/level/ServerLevel;"
                    + "Lcom/mojang/authlib/GameProfile;Lnet/minecraft/server/level/ClientInformation;)V",
            at = @At("HEAD")
    )
    private static void player_to_player$beginConstructorWindow(MinecraftServer server, ServerLevel level,
                                                         GameProfile profile,
                                                         ClientInformation clientInformation,
                                                         CallbackInfo ci) {
        if (profile == null || !GroupRuntime.isManagedLevel(level)) {
            return;
        }
        // 构造器初始位置是共享出生点（playerdata 还没读取），这里仅作为日志锚点记录；
        // 真实登录锚点由 placeNewPlayer 的 load(playerdata) 决定。
        int spawnChunkX = level.getSharedSpawnPos().getX() >> 4;
        int spawnChunkZ = level.getSharedSpawnPos().getZ() >> 4;
        LoginLoadGuard.beginLogin(profile.getId(), profile.getName(),
                level.dimension().location().toString(), spawnChunkX, spawnChunkZ);
    }

    /** 构造器正常返回：结束防御窗口；fudge 若阻塞则不会走到这里。 */
    @Inject(
            method = "<init>(Lnet/minecraft/server/MinecraftServer;Lnet/minecraft/server/level/ServerLevel;"
                    + "Lcom/mojang/authlib/GameProfile;Lnet/minecraft/server/level/ClientInformation;)V",
            at = @At("RETURN")
    )
    private void player_to_player$endConstructorWindow(MinecraftServer server, ServerLevel level,
                                                       GameProfile profile,
                                                       ClientInformation clientInformation,
                                                       CallbackInfo ci) {
        if (profile != null && GroupRuntime.isManagedLevel(level)) {
            LoginLoadGuard.endLogin(profile.getId(), "构造器完成");
        }
    }
}
