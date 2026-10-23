package imsng.player_to_player.env;

import com.google.gson.JsonObject;
import imsng.player_to_player.netproto.Protocol;
import imsng.player_to_player.util.JsonUtil;
import java.util.Map;
import java.util.TreeMap;

/** 登录查询及只读控制握手共用的环境描述，避免登录端口和控制面各自猜测配置。 */
public record EnvironmentPreflightInfo(int controlPort, int relayPort, String worldName,
                                       String worldPath, String snapshotId, String runtimeHash,
                                       Map<String, String> worldProperties, String serverInstanceId) {
    public EnvironmentPreflightInfo(int controlPort, int relayPort, String worldName,
                                    String worldPath, String snapshotId, String runtimeHash,
                                    Map<String, String> worldProperties) {
        this(controlPort, relayPort, worldName, worldPath, snapshotId, runtimeHash, worldProperties,
                "00000000-0000-0000-0000-000000000000");
    }
    public EnvironmentPreflightInfo {
        if (controlPort < 1 || controlPort > 65535 || relayPort < 0 || relayPort > 65535)
            throw new IllegalArgumentException("预检端口无效");
        worldPath = EnvironmentRuntimeIdentity.normalizeWorldPath(worldPath);
        worldProperties = Map.copyOf(worldProperties);
        java.util.UUID.fromString(serverInstanceId);
        if (!snapshotId.isEmpty() && !snapshotId.matches("[0-9a-f]{64}"))
            throw new IllegalArgumentException("快照版本无效");
        if (!runtimeHash.isEmpty() && !runtimeHash.matches("[0-9a-f]{64}"))
            throw new IllegalArgumentException("运行环境版本无效");
    }

    public boolean ready() { return !snapshotId.isEmpty() && !runtimeHash.isEmpty(); }

    /** NAT回退端口也必须对应原登录目标，不能误连到同一主机上的另一个P2P服务器。 */
    public boolean sameRuntime(EnvironmentPreflightInfo other) {
        return ready() && other.ready() && sameServer(other) && runtimeHash.equals(other.runtimeHash);
    }

    public boolean sameServer(EnvironmentPreflightInfo other) {
        return serverInstanceId.equals(other.serverInstanceId) && worldName.equals(other.worldName)
                && worldPath.equals(other.worldPath);
    }

    /** 控制服务经NAT映射后，客户端保存实际连接成功的外部端口供正式会话复用。 */
    public EnvironmentPreflightInfo withControlPort(int actualPort) {
        return new EnvironmentPreflightInfo(actualPort, relayPort, worldName, worldPath,
                snapshotId, runtimeHash, worldProperties, serverInstanceId);
    }

    public JsonObject toJson() {
        JsonObject json = new JsonObject();
        json.addProperty("accepted", true);
        json.addProperty("version", Protocol.VERSION);
        json.addProperty("controlPort", controlPort);
        json.addProperty("relayPort", relayPort);
        json.addProperty("worldName", worldName);
        json.addProperty("worldPath", worldPath);
        json.addProperty("snapshotId", snapshotId);
        json.addProperty("runtimeHash", runtimeHash);
        json.addProperty("envReady", ready());
        json.addProperty("serverInstanceId", serverInstanceId);
        JsonObject properties = new JsonObject();
        new TreeMap<>(worldProperties).forEach(properties::addProperty);
        json.add("worldProperties", properties);
        return json;
    }

    public static EnvironmentPreflightInfo fromJson(JsonObject json) {
        if (JsonUtil.getInt(json, "version", -1) != Protocol.VERSION)
            throw new IllegalArgumentException("服务端与客户端协议版本不同，请更新模组");
        Map<String, String> properties = new TreeMap<>();
        if (json.has("worldProperties") && json.get("worldProperties").isJsonObject()) {
            json.getAsJsonObject("worldProperties").entrySet().forEach(e -> {
                if (e.getValue().isJsonPrimitive()) properties.put(e.getKey(), e.getValue().getAsString());
            });
        }
        return new EnvironmentPreflightInfo(JsonUtil.getInt(json, "controlPort", -1),
                JsonUtil.getInt(json, "relayPort", 0), JsonUtil.getString(json, "worldName", "world"),
                JsonUtil.getString(json, "worldPath", "world"), JsonUtil.getString(json, "snapshotId", ""),
                JsonUtil.getString(json, "runtimeHash", ""), properties,
                JsonUtil.getString(json, "serverInstanceId", ""));
    }
}
