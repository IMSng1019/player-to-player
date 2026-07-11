package imsng.player_to_player.mixin;

import imsng.player_to_player.group.GroupRuntime;
import imsng.player_to_player.group.PearlHandoff;
import net.minecraft.server.level.ServerLevel;
import net.minecraft.world.entity.projectile.ThrownEnderpearl;
import net.minecraft.world.phys.HitResult;
import org.spongepowered.asm.mixin.Mixin;
import org.spongepowered.asm.mixin.injection.At;
import org.spongepowered.asm.mixin.injection.Inject;
import org.spongepowered.asm.mixin.injection.callback.CallbackInfo;

/**
 * 末影珍珠特殊加载的注入点（Phase 4，规范"末影珍珠"；判定与协议逻辑全部在
 * {@link PearlHandoff}，本类只做门控与转发）：
 * <ul>
 *   <li><b>tick HEAD</b>：被接管集成服务端上的珍珠每 tick 做一次出界预判
 *       （下一位置不属本组 → 只读探测归属 → 被其他组占用/缓冲则交接向量数据；
 *       {@code ThrownEnderpearl.tick()} 签名已 javap 核实）—— 不取消原版 tick：
 *       探测在途时珍珠照常飞行/暂停，交接确认后由 PearlHandoff discard；</li>
 *   <li><b>onHit HEAD（cancellable）</b>：远端注入的珍珠（带
 *       {@link PearlHandoff#REMOTE_TAG} 记号）落地 → 回报落点给抛出者所在组
 *       （规范"落地后看做一个小型 tp"）并取消原版 onHit —— 原版路径拿不到
 *       抛出者实体（人在别的组），只会白白 discard 而丢失落点回报。</li>
 * </ul>
 * 客户端逻辑侧（isClientSide）与未接管服务端零行为差异（门控首判）。
 */
@Mixin(ThrownEnderpearl.class)
public abstract class ThrownEnderpearlMixin {

    /** 每 tick 出界预判（廉价门控：非被接管维度直接返回）。 */
    @Inject(method = "tick()V", at = @At("HEAD"))
    private void p2p$outboundCheck(CallbackInfo ci) {
        ThrownEnderpearl self = (ThrownEnderpearl) (Object) this;
        if (self.level() instanceof ServerLevel level && GroupRuntime.isManagedLevel(level)) {
            PearlHandoff.onPearlTick(self, level);
        }
    }

    /** 远端珍珠落地：回报落点 + 取消原版处理。 */
    @Inject(method = "onHit(Lnet/minecraft/world/phys/HitResult;)V",
            at = @At("HEAD"), cancellable = true)
    private void p2p$remoteHit(HitResult result, CallbackInfo ci) {
        ThrownEnderpearl self = (ThrownEnderpearl) (Object) this;
        if (self.level() instanceof ServerLevel level && GroupRuntime.isManagedLevel(level)
                && PearlHandoff.onRemoteHit(self, level)) {
            ci.cancel();
        }
    }
}
