package imsng.player_to_player.mixin;

import imsng.player_to_player.group.GroupRuntime;
import imsng.player_to_player.group.PortalPreloader;
import net.minecraft.core.BlockPos;
import net.minecraft.server.level.ServerPlayer;
import net.minecraft.world.entity.Entity;
import org.spongepowered.asm.mixin.Mixin;
import org.spongepowered.asm.mixin.injection.At;
import org.spongepowered.asm.mixin.injection.Inject;
import org.spongepowered.asm.mixin.injection.callback.CallbackInfo;

/**
 * 传送门预检的注入点（Phase 4，规范"玩家进入传送门……提前加载另外一个维度的
 * 未知区块检查是否被其他组加载"；判定与协议逻辑全部在 {@link PortalPreloader}）。
 * <p>
 * 注入 {@code Entity.handleInsidePortal(BlockPos)}（签名已 javap 核实）——
 * 玩家<b>站进下界传送门</b>的第一时间（原版从此处开始 4 秒传送等待计时）即触发
 * 目的维度落点的只读探测：被其他组占用 → 提前打合并触发信号，让预连接/预同步
 * 借等待窗口先跑起来。不取消原版逻辑（探测纯旁路）；跨维度加载的正确性
 * 本就由区块申请门控保证，此处只负责规范要求的"提前"。
 * <p>
 * 门控顺序：先 instanceof ServerPlayer（滤掉客户端逻辑侧与非玩家实体，纳秒级），
 * 再 isManagedLevel（一次 volatile 读）；限速（5s/人）在 PortalPreloader 内。
 */
@Mixin(Entity.class)
public abstract class EntityPortalMixin {

    /** 玩家站进下界传送门：目的维度落点提前探测（每 tick 触发，下游限速）。 */
    @Inject(method = "handleInsidePortal(Lnet/minecraft/core/BlockPos;)V", at = @At("HEAD"))
    private void p2p$portalPreload(BlockPos portalPos, CallbackInfo ci) {
        Entity self = (Entity) (Object) this;
        if (self instanceof ServerPlayer player
                && GroupRuntime.isManagedLevel(player.serverLevel())) {
            PortalPreloader.onPlayerInPortal(player);
        }
    }
}
