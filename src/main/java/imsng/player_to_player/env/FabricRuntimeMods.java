package imsng.player_to_player.env;

import imsng.player_to_player.util.Sha256;
import net.fabricmc.loader.api.FabricLoader;
import net.fabricmc.loader.api.ModContainer;
import net.fabricmc.loader.api.metadata.ModOrigin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

/** Fabric 已解析模组容器的真实来源快照，客户端与独立服务端共用。 */
public final class FabricRuntimeMods {
    private static final Logger LOGGER = LoggerFactory.getLogger("player_to_player/runtime-mods");
    public record Captured(Map<String, ClientEnvironmentCompatibility.LoadedMod> mods,
                           Map<String, Path> clientOnlyJars) { }
    private FabricRuntimeMods() { }

    public static Captured capture() throws IOException {
        FabricLoader loader = FabricLoader.getInstance();
        Path modsRoot = loader.getGameDir().toAbsolutePath().normalize().resolve("mods");
        Map<String, ClientEnvironmentCompatibility.LoadedMod> loaded = new HashMap<>();
        Map<String, Path> clientOnly = new HashMap<>();
        for (ModContainer mod : loader.getAllMods()) {
            if (!mod.getMetadata().getType().equals("fabric")) continue;
            boolean nested = mod.getOrigin().getKind() == ModOrigin.Kind.NESTED;
            Path source = source(mod);
            String hash = source != null && Files.isRegularFile(source) ? Sha256.hexOfFile(source) : "";
            String environment = switch (mod.getMetadata().getEnvironment()) {
                case CLIENT -> "client";
                case SERVER -> "server";
                case UNIVERSAL -> "*";
            };
            // 仅开发模式允许项目输出/Gradle依赖作为额外来源。若清单明确要求同ID，
            // check仍必须验证其真实哈希；绝不把目录输出伪造成相同的发行JAR。
            boolean development = loader.isDevelopmentEnvironment()
                    && (source == null || !source.toAbsolutePath().normalize().startsWith(modsRoot));
            String id = mod.getMetadata().getId();
            loaded.put(id, new ClientEnvironmentCompatibility.LoadedMod(id,
                    mod.getMetadata().getVersion().getFriendlyString(), environment, hash, nested, development));
            if (environment.equals("client") && !nested && source != null && !hash.isEmpty()) clientOnly.put(id, source);
            if (development && !nested) LOGGER.debug("开发启动额外模组来源: {}（不冒充已分发JAR）", id);
        }
        return new Captured(Map.copyOf(loaded), Map.copyOf(clientOnly));
    }

    private static Path source(ModContainer mod) {
        ModOrigin origin = mod.getOrigin();
        if (origin.getKind() == ModOrigin.Kind.PATH) {
            var paths = origin.getPaths();
            return paths.size() == 1 ? paths.get(0) : null;
        }
        if (origin.getKind() == ModOrigin.Kind.NESTED) {
            // 不拿父JAR哈希代替内嵌JAR：同版本模块也可能由另一个外层集合提供。
            var parent = mod.getContainingMod();
            if (parent.isPresent()) {
                var nested = parent.get().findPath(origin.getParentSubLocation());
                if (nested.isPresent()) return nested.get();
            }
        }
        // 容器的父模组可能因物理端不同而未加载；已挂载的jar根仍可定位实际加载文件。
        for (Path root : mod.getRootPaths()) {
            java.net.URI uri = root.toUri();
            if (!uri.getScheme().equals("jar")) continue;
            String scheme = uri.getSchemeSpecificPart();
            int bang = scheme.indexOf("!/");
            if (bang > 0) {
                try { return Path.of(java.net.URI.create(scheme.substring(0, bang))); }
                catch (IllegalArgumentException ignored) { }
            }
        }
        return null;
    }
}
