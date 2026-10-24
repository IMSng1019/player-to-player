package imsng.player_to_player.env.restart;

import java.util.UUID;

/**
 * UI 与后台回调共用的重启仲裁。连接意图与环境版本分开：服务器在自动回服期间再次
 * 更新版本也不能重置预算。手动重试显式放行，换到其他服务器才开始新的自动连接链。
 */
public final class RestartCoordinator {
    public record Intent(long generation, UUID chainId, String address) { }
    private String address;
    private UUID chainId;
    private long generation;
    private Intent current;
    private boolean automaticUsed;
    private boolean busy;
    private boolean committed;

    public RestartCoordinator(RestartTask resumed) {
        if (resumed != null) {
            address = resumed.address();
            chainId = resumed.chainId();
            automaticUsed = true;
        }
    }
    public synchronized Intent begin(String target) {
        if (committed) return null;
        if (!target.equals(address)) {
            address = target;
            chainId = UUID.randomUUID();
            automaticUsed = false;
        }
        busy = false;
        return current = new Intent(++generation, chainId, address);
    }
    public synchronized boolean current(Intent intent) { return intent != null && intent == current; }
    public synchronized boolean reserve(Intent intent, boolean manual) {
        if (!current(intent) || busy || committed || (!manual && automaticUsed)) return false;
        busy = true;
        automaticUsed = true;
        return true;
    }
    /** 只在主线程重新核验预检 epoch、目标和当前界面后调用；此后取消/切服都被拒绝。 */
    public synchronized boolean commit(Intent intent) {
        if (!current(intent) || !busy || committed) return false;
        committed = true;
        return true;
    }
    public synchronized boolean cancel(Intent intent) {
        if (!current(intent) || committed) return false;
        current = null;
        busy = false;
        return true;
    }
    /** 辅助进程失败后保留旧游戏；不归还自动预算，防止重复回调制造启动风暴。 */
    public synchronized boolean fail(Intent intent) {
        if (!current(intent)) return false;
        busy = false;
        committed = false;
        return true;
    }
    public synchronized boolean committed() { return committed; }
}
