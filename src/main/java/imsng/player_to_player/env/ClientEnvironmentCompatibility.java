package imsng.player_to_player.env;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import imsng.player_to_player.util.JsonUtil;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.zip.ZipFile;

/** 用 Fabric 实际加载容器的来源哈希检查运行状态，绝不把“JAR 已下载”当作“已加载”。 */
public final class ClientEnvironmentCompatibility {
    public record LoadedMod(String id, String version, String environment, String sha256,
                            boolean nested, boolean developmentOnly) {
        public LoadedMod(String id, String version, String environment, String sha256) {
            this(id, version, environment, sha256, false, false);
        }
        public LoadedMod(String id, String version, String environment, String sha256, boolean nested) {
            this(id, version, environment, sha256, nested, false);
        }
    }
    public record Metadata(String id, String version, String environment) { }
    private ClientEnvironmentCompatibility() { }

    public static List<String> check(Path environmentRoot, EnvironmentManifest manifest,
                                     Map<String, LoadedMod> loaded) throws IOException {
        return check(environmentRoot, manifest, loaded, "client");
    }

    /** 主计算进程使用 server；渲染进程使用 client。两者都按真正加载的 JAR 字节检查。 */
    public static List<String> check(Path environmentRoot, EnvironmentManifest manifest,
                                     Map<String, LoadedMod> loaded, String targetEnvironment) throws IOException {
        if (!targetEnvironment.equals("server") && !targetEnvironment.equals("client"))
            throw new IllegalArgumentException("未知 Fabric 运行端");
        List<String> problems = new ArrayList<>();
        Set<String> required = new HashSet<>();
        for (var file : manifest.files().entrySet()) {
            if (!file.getKey().startsWith("mods/") || !file.getKey().endsWith(".jar")) continue;
            Metadata metadata = metadata(environmentRoot.resolve(file.getKey()));
            // 物理服务端专用模组由独立 worker 加载；Fabric 客户端本来也会过滤它们。
            if (!metadata.environment().equals("*") && !metadata.environment().equals(targetEnvironment)) continue;
            if (!required.add(metadata.id())) throw new IOException("环境存在重复模组 ID: " + metadata.id());
            LoadedMod active = loaded.get(metadata.id());
            if (active == null) problems.add("尚未加载 " + metadata.id() + " " + metadata.version());
            else if (!active.version().equals(metadata.version()) || !active.sha256().equals(file.getValue().sha256()))
                problems.add("已加载版本/内容不同: " + metadata.id());
        }
        for (LoadedMod active : loaded.values()) {
            if (!required.contains(active.id()) && !active.nested() && !active.developmentOnly()
                    && (!targetEnvironment.equals("client") || !active.environment().equals("client")))
                problems.add("当前进程额外加载了通用模组: " + active.id());
        }
        return List.copyOf(problems);
    }

    public static Metadata metadata(Path jar) throws IOException {
        try (ZipFile zip = new ZipFile(jar.toFile())) {
            var entry = zip.getEntry("fabric.mod.json");
            if (entry == null || entry.getSize() > 1024 * 1024) throw new IOException("不是有效 Fabric 模组: " + jar.getFileName());
            try (var reader = new InputStreamReader(zip.getInputStream(entry), StandardCharsets.UTF_8)) {
                JsonObject json = JsonParser.parseReader(reader).getAsJsonObject();
                String id = JsonUtil.getString(json, "id", "");
                if (id.isBlank()) throw new IOException("模组缺少 ID: " + jar.getFileName());
                return new Metadata(id, JsonUtil.getString(json, "version", ""), JsonUtil.getString(json, "environment", "*"));
            } catch (RuntimeException invalid) {
                throw new IOException("模组描述无效: " + jar.getFileName(), invalid);
            }
        }
    }
}
