package imsng.player_to_player.world;

import imsng.player_to_player.config.GlobalConfig;
import imsng.player_to_player.core.NodeMode;

/**
 * 出生点常加载的统一角色策略，供启动预热和运行期出生点变更共用。
 * <p>
 * 受管组世界的加载权必须经区块申领取得，不能因配置尚未装载或关闭逃生选项而恢复
 * 出生点常驻票据。物理服务端与中转服务端则保留按配置恢复原版行为的排障入口。
 * 本类仅根据传入状态作判断，不访问全局上下文或 Minecraft 世界，便于独立验证。
 */
public final class SpawnChunkPolicy {

    private SpawnChunkPolicy() {
    }

    /**
     * 判断本节点是否应禁止原版出生点常驻票据。
     *
     * @param mode 节点运行角色
     * @param managedGroup 组世界是否已预置或接管；仅在 CLIENT 模式生效
     * @param config 总配置；null 表示配置尚未装载
     */
    public static boolean shouldDisable(NodeMode mode, boolean managedGroup, GlobalConfig config) {
        if (mode == NodeMode.CLIENT) {
            // 普通单人世界始终沿用原版；受管组世界始终阻止未经申领的出生点常加载。
            return managedGroup;
        }
        return (mode == NodeMode.SERVER || mode == NodeMode.PROXY_SERVER)
                && config != null && config.disableSpawnChunks;
    }
}
