package imsng.player_to_player.env;

import com.google.gson.JsonObject;
import imsng.player_to_player.util.JsonUtil;
import imsng.player_to_player.util.Sha256;
import net.fabricmc.api.EnvType;
import net.fabricmc.loader.api.FabricLoader;
import net.fabricmc.loader.api.entrypoint.PreLaunchEntrypoint;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.util.Map;
import java.util.TreeMap;

/**
 * 在模组 main/client 初始化之前捕获隔离实例真实的启动文件。版本属性只负责找到
 * 安装记录，不能充当校验结果：玩家关服后手改配置，下一次启动必须被检查出来。
 * 后续模组写盘不会改写该快照，因此登录校验反映启动时内容而非下载后的磁盘内容。
 */
public final class ClientRuntimeStartup implements PreLaunchEntrypoint {
    public static final String MANIFEST_FILE = "client-startup-manifest.json";
    private static volatile Snapshot startup;
    private static volatile String runtimeHash = "";

    public record Snapshot(Map<String, EnvironmentManifest.Entry> files) {
        public Snapshot { files = Map.copyOf(files); }

        public boolean matches(EnvironmentManifest expected, String worldPath) {
            for (var file : expected.files().entrySet()) {
                // 旧服务端清单可能仍携带 options.txt；语言、音量等属于本机玩家，
                // 不得因恢复个人偏好而否认已经加载的玩法环境并再次重启。
                if (ClientLocalSettings.isLocalPath(file.getKey())) continue;
                if (file.getKey().startsWith(EnvironmentRuntimeIdentity.normalizeWorldPath(worldPath) + "/")) continue;
                if (!file.getValue().equals(files.get(file.getKey()))) return false;
            }
            return true;
        }
    }

    @Override public void onPreLaunch() {
        FabricLoader loader = FabricLoader.getInstance();
        if (loader.getEnvironmentType() != EnvType.CLIENT) return;
        String expectedHash = System.getProperty("player_to_player.clientRuntime", "");
        if (expectedHash.isBlank()) return;
        Path gameDir = loader.getGameDir().toAbsolutePath().normalize();
        Path record = gameDir.getParent().resolve(MANIFEST_FILE);
        try {
            if (!Files.isRegularFile(record, LinkOption.NOFOLLOW_LINKS)) return;
            JsonObject json = JsonUtil.parseObject(Files.readString(record));
            if (!expectedHash.equals(JsonUtil.getString(json, "runtimeHash", ""))) return;
            EnvironmentManifest installed = EnvironmentManifest.fromJson(json.getAsJsonObject("manifest"));
            startup = capture(gameDir, installed);
            runtimeHash = expectedHash;
        } catch (IOException | RuntimeException invalid) {
            // 仍允许玩家打开主菜单修复环境；没有有效启动证据时预检会重新安装并阻止入服。
            startup = null;
            runtimeHash = "";
        }
    }

    public static boolean matches(String requiredHash, EnvironmentManifest expected, String worldPath) {
        Snapshot value = startup;
        return value != null && requiredHash.equals(runtimeHash) && value.matches(expected, worldPath);
    }

    public static Snapshot capture(Path gameDir, EnvironmentManifest installed) throws IOException {
        Path root = gameDir.toAbsolutePath().normalize();
        Map<String, EnvironmentManifest.Entry> files = new TreeMap<>();
        for (String relative : installed.files().keySet()) {
            // 本机选项不属于启动证据；即使旧安装记录包含它，也不读取或哈希此文件。
            if (ClientLocalSettings.isLocalPath(relative)) continue;
            Path path = root.resolve(relative).normalize();
            if (!path.startsWith(root) || !Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS)) continue;
            // 拒绝由父目录软链接指向实例外的文件，不能拿外部配置冒充独立安装。
            if (!path.toRealPath().startsWith(root.toRealPath())) continue;
            files.put(relative, new EnvironmentManifest.Entry(Sha256.hexOfFile(path), Files.size(path)));
        }
        return new Snapshot(files);
    }
}
