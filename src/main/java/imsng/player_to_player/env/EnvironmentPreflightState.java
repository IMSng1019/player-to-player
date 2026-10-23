package imsng.player_to_player.env;

import java.util.Objects;

/** 登录预检票据：绑定完整主机端口、单次连接意图和静态版本，不跨取消/切服复用。 */
public final class EnvironmentPreflightState {
    /** 同一地址在票据、隔离目录、QuickPlay中使用完全相同且包含端口的规范形式。 */
    public static String addressKey(String host, int port) {
        if (port < 1 || port > 65535) throw new IllegalArgumentException("MC端口无效");
        String value = Objects.requireNonNull(host).toLowerCase(java.util.Locale.ROOT);
        if (value.indexOf(':') >= 0 && !value.startsWith("[")) value = "[" + value + "]";
        return value + ":" + port;
    }
    private long epoch;
    private String address;
    private String runtimeHash;

    public synchronized long begin(String target) {
        address = Objects.requireNonNull(target);
        runtimeHash = null;
        return ++epoch;
    }

    public synchronized boolean current(long expected) {
        return expected == epoch && address != null;
    }

    public synchronized boolean ready(long expected, String hash) {
        if (!current(expected)) return false;
        runtimeHash = Objects.requireNonNull(hash);
        return true;
    }

    public synchronized boolean matches(String target, String hash) {
        return address != null && address.equals(target) && runtimeHash != null && runtimeHash.equals(hash);
    }

    public synchronized void cancel() {
        ++epoch;
        address = null;
        runtimeHash = null;
    }
}
