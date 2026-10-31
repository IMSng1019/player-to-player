package imsng.player_to_player.runtime;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.jar.Attributes;
import java.util.jar.JarFile;
import java.util.zip.ZipException;
import imsng.player_to_player.util.JsonUtil;

/** 独立计算实例的直接 Java 启动参数。 */
public final class RuntimeLaunchCommand {
    /** 官方安装器下载包与安装后生成的启动包使用不同入口，文件名均可能由管理员修改。 */
    private static final Set<String> SERVER_LAUNCHERS = Set.of(
            "net.fabricmc.installer.ServerLauncher",
            "net.fabricmc.loader.impl.launch.server.FabricServerLauncher");
    /** 开发描述只能由本机Gradle生成并通过JVM参数指定，绝不读取远端环境同名文件。 */
    public record DevelopmentSpec(String classpath, String mainClass, List<String> jvmArguments) { }
    private RuntimeLaunchCommand() { }
    public static List<String> build(Path java, Path runtime, Path saves, String world,
                                     Path session) throws IOException {
        List<String> command = new ArrayList<>();
        command.add(java.toString());
        String development = System.getProperty("p2p.worker.developmentSpec", "");
        if (!development.isBlank()) {
            DevelopmentSpec spec = JsonUtil.readFile(Path.of(development), DevelopmentSpec.class);
            if (spec == null || spec.classpath() == null || spec.classpath().isBlank()
                    || spec.mainClass() == null || spec.mainClass().isBlank()) {
                throw new IOException("本机开发服务端启动描述不完整");
            }
            command.addAll(spec.jvmArguments() == null ? List.of() : spec.jvmArguments());
            command.add("-Dp2p.worker.session=" + session.toAbsolutePath());
            command.add("-cp");
            command.add(spec.classpath());
            command.add(spec.mainClass());
        } else {
            Path launcher = findServerLauncher(runtime);
            command.add("-Dp2p.worker.session=" + session.toAbsolutePath());
            command.add("-jar");
            command.add(launcher.toString());
        }
        command.add("--universe");
        command.add(saves.toAbsolutePath().toString());
        command.add("--world");
        command.add(world);
        command.add("nogui");
        return List.copyOf(command);
    }

    /**
     * 从已校验、已安装的环境中选择真正的 Fabric 服务端入口。只检查根目录普通 JAR，
     * 不执行 start.sh/bat，也不递归进入 .fabric/server：其中可能同时保留多个旧 Loader。
     * Main-Class 用于识别启动方式，文件真实性仍由上游环境清单和安装校验负责。
     */
    private static Path findServerLauncher(Path runtime) throws IOException {
        List<Path> candidates = new ArrayList<>();
        try (var files = Files.list(runtime.toAbsolutePath().normalize())) {
            for (Path file : files.sorted().toList()) {
                if (!Files.isRegularFile(file, LinkOption.NOFOLLOW_LINKS)
                        || !file.getFileName().toString().toLowerCase(Locale.ROOT).endsWith(".jar")) continue;
                try (JarFile jar = new JarFile(file.toFile(), false)) {
                    var manifest = jar.getManifest();
                    String main = manifest == null ? null
                            : manifest.getMainAttributes().getValue(Attributes.Name.MAIN_CLASS);
                    if (main != null && SERVER_LAUNCHERS.contains(main)) candidates.add(file);
                } catch (ZipException invalidJar) {
                    // 根目录可能含无关或损坏的工具包；它们不能作为 Fabric 启动候选。
                }
            }
        }
        if (candidates.isEmpty()) {
            throw new IOException("同步环境根目录未找到可识别的 Fabric 服务端启动 JAR；"
                    + "请同步 fabric-server-launch.jar 或官方版本化启动包及其依赖");
        }
        if (candidates.size() != 1) {
            // 不以固定文件名、字典序或版本号猜测管理员意图，避免启动错误的游戏/Loader。
            throw new IOException("同步环境根目录存在多个 Fabric 服务端启动 JAR，请只保留当前使用的启动包："
                    + candidates.stream().map(path -> path.getFileName().toString()).toList());
        }
        return candidates.get(0);
    }
}
