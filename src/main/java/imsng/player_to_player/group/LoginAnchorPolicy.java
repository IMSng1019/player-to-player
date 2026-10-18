package imsng.player_to_player.group;

/**
 * 登录锚点策略（纯函数，供 Mixin 与单元测试共用）。
 *
 * <p>把"是否跳过出生点修正 / 是否武装同步加载看门狗 / 某区块是否为共享出生点区块"
 * 这些判定从 Minecraft 类中剥离，测试环境无需构造 {@code ServerLevel}/{@code ServerPlayer}。
 */
public final class LoginAnchorPolicy {

    private LoginAnchorPolicy() {
    }

    /**
     * 是否取消原版 {@code ServerPlayer.fudgeSpawnLocation}。
     *
     * <p>只有"被组运行时接管的集成服务端世界"才需要干预；专用服、单人世界、
     * 未接管的集成服一律保留原版行为。
     *
     * <p><b>为什么不看 {@code disableSpawnChunks}</b>：该配置的逃生口语义只适用于
     * 专用服的启动预热（{@code prepareLevels}）。受管组世界里加载共享出生点区块会
     * 直接与区块 claim 门控冲突（原点 23×23 依赖区块可能被其他组持有），一旦允许
     * 关闭就会重新引入"登录主线程无限等待"的 P0 事故。因此这里无条件跳过；
     * 参数保留用于调用方统一传参/日志。
     *
     * @param managedLevel         该维度是否属于被接管的集成服务端
     * @param disableSpawnChunks   {@code GlobalConfig.disableSpawnChunks}（不改变本判定）
     * @return true = 跳过出生点 fudge，登录锚点交给 playerdata 的真实位置
     */
    public static boolean shouldSkipSpawnFudge(boolean managedLevel, boolean disableSpawnChunks) {
        return managedLevel;
    }

    /**
     * 是否武装"登录期区块加载"看门狗/硬超时。
     *
     * <p>只有登录窗口内、由服务端主线程发起的区块调度才需要看门狗。注意
     * {@code ChunkMap.readChunk} 本身通常就在主线程被调度，所以本判据不是
     * "调用方此刻是否正在 join"的精确判定；但登录窗口很短、硬超时使用
     * 非 IOException（不会触发原版空区块回退），因此窗口内发起的加载都可以安全兜底。
     * 登录窗口之外不干预，避免正常游玩的区块加载被误杀。
     */
    public static boolean shouldArmSyncWatchdog(boolean loginPhase, boolean serverThread) {
        return loginPhase && serverThread;
    }

    /**
     * 该区块是否落在玩家渲染矩形之外（P0-4 诊断）。
     *
     * @param keyDimension    区块申请所属维度
     * @param playerDimension 玩家登录后的权威维度
     * @param keyX/keyZ       区块坐标
     * @param playerX/playerZ 玩家区块坐标
     * @param viewDistance    玩家实际渲染半径（区块）
     * @return true = 视野外；跨维度也按视野外处理
     */
    public static boolean isOutsideRenderArea(String keyDimension, String playerDimension,
                                              int keyX, int keyZ,
                                              int playerX, int playerZ,
                                              int viewDistance) {
        if (keyDimension == null || playerDimension == null
                || !keyDimension.equals(playerDimension)) {
            return true;
        }
        return Math.abs(keyX - playerX) > viewDistance
                || Math.abs(keyZ - playerZ) > viewDistance;
    }

    /**
     * 该区块是否是世界共享出生点所在区块（方块坐标 >> 4）。
     *
     * <p>受管组世界登录期出现这个区块的加载请求，说明 P0-1 的 fudge 豁免没有覆盖到
     * 该路径，需要打印 WARN + 调用栈作为回归证据。
     */
    public static boolean isSharedSpawnChunk(int spawnBlockX, int spawnBlockZ,
                                             int chunkX, int chunkZ) {
        return (spawnBlockX >> 4) == chunkX && (spawnBlockZ >> 4) == chunkZ;
    }
}
