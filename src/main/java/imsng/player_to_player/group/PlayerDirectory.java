package imsng.player_to_player.group;

import com.google.gson.JsonObject;
import imsng.player_to_player.util.JsonUtil;

import java.util.Collection;
import java.util.List;
import java.util.TreeSet;

/**
 * 当前物理服的在线玩家名快照，只供原版命令补全使用。
 * 不向本组注入虚假的 ServerPlayer/PlayerInfo，也不扩大选择器实际执行的世界范围；
 * 跨组命令仍交由权威服务端按玩家表路由。控制连接换代/断开时必须清空。
 */
public final class PlayerDirectory {
    public static final PlayerDirectory SESSION = new PlayerDirectory();
    private volatile List<String> names = List.of();

    /** 缺省字段表示旧版发送方；空数组则明确表示全局名单已空。 */
    public void update(JsonObject json) {
        if (!json.has("onlinePlayers") || !json.get("onlinePlayers").isJsonArray()) return;
        TreeSet<String> next = new TreeSet<>();
        for (var value : json.getAsJsonArray("onlinePlayers")) {
            if (!value.isJsonObject()) continue;
            String name = JsonUtil.getString(value.getAsJsonObject(), "name", "");
            if (name.matches("[A-Za-z0-9_]{1,16}")) next.add(name);
        }
        names = List.copyOf(next);
    }

    /** 保留原版/其他模组提供的本地候选，新增远端名字后稳定排序并去重。 */
    public List<String> namesIncluding(Collection<String> vanilla) {
        TreeSet<String> combined = new TreeSet<>(vanilla);
        combined.addAll(names);
        return List.copyOf(combined);
    }

    public void reset() { names = List.of(); }
}
