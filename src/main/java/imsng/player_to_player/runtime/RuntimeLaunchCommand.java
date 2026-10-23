package imsng.player_to_player.runtime;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import imsng.player_to_player.util.JsonUtil;

/** 独立计算实例的直接 Java 启动参数。 */
public final class RuntimeLaunchCommand {
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
            Path launcher = runtime.toAbsolutePath().resolve("fabric-server-launch.jar");
            if (!Files.isRegularFile(launcher)) {
                throw new IOException("同步环境缺少 fabric-server-launch.jar；无法启动独立 Fabric 服务端");
            }
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
}
