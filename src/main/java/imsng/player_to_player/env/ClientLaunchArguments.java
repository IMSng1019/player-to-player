package imsng.player_to_player.env;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

/** 重建当前启动器给出的参数；保留认证参数，不拼接 shell 文本、不打印令牌。 */
public final class ClientLaunchArguments {
    private static final Set<String> REPLACED = Set.of("--gameDir", "--server", "--port",
            "--quickPlayMultiplayer", "--quickPlaySingleplayer", "--quickPlayRealms", "--quickPlayPath");
    private ClientLaunchArguments() { }

    /** 仅处理 Loader 解析后的游戏参数；每次手动重试都以当前会话替换旧账号字段。 */
    public static List<String> withIdentity(List<String> original, java.util.Map<String, String> identity) {
        List<String> result = new ArrayList<>();
        for (int index = 0; index < original.size(); index++) {
            String arg = original.get(index);
            int equals = arg.indexOf('=');
            String key = equals > 0 ? arg.substring(0, equals) : arg;
            if (identity.containsKey(key)) {
                if (equals < 0 && index + 1 < original.size()) index++;
            } else result.add(arg);
        }
        identity.forEach((key, value) -> { result.add(key); result.add(value); });
        return List.copyOf(result);
    }

    public static List<String> rewrite(List<String> original, Path gameDir, String address) {
        List<String> result = new ArrayList<>();
        for (int index = 0; index < original.size(); index++) {
            String arg = original.get(index);
            int equals = arg.indexOf('=');
            String key = equals > 0 ? arg.substring(0, equals) : arg;
            if (REPLACED.contains(key)) {
                if (equals < 0 && index + 1 < original.size()) index++;
                continue;
            }
            // Loader 的绝对 modsFolder 会绕过新的 gameDir；必须与 addMods 一起清除。
            if (arg.startsWith("-Dfabric.addMods=") || arg.startsWith("-Dfabric.modsFolder=")) continue;
            if (arg.startsWith("-javaagent:") || arg.startsWith("-agentpath:")) {
                int colon = arg.indexOf(':');
                String agent = arg.substring(colon + 1);
                int options = agent.indexOf('=');
                String path = options < 0 ? agent : agent.substring(0, options);
                result.add(arg.substring(0, colon + 1) + Path.of(path).toAbsolutePath()
                        + (options < 0 ? "" : agent.substring(options)));
                continue;
            }
            // 新进程的工作目录也要隔离；原启动器常以相对路径指定 natives，必须保留其实际位置。
            if (arg.startsWith("-Djava.library.path=") || arg.startsWith("-Dorg.lwjgl.librarypath=")
                    || arg.startsWith("-Dfabric.gameJarPath=") || arg.startsWith("-Dfabric.gameJarPath.client=")) {
                result.add(arg.substring(0, equals + 1) + absolutePathList(arg.substring(equals + 1)));
                continue;
            }
            if (arg.startsWith("--assetsDir=")) {
                result.add("--assetsDir=" + Path.of(arg.substring(equals + 1)).toAbsolutePath());
                continue;
            }
            result.add(arg);
            if ((arg.equals("-cp") || arg.equals("-classpath") || arg.equals("--class-path"))
                    && index + 1 < original.size()) {
                result.add(absolutePathList(original.get(++index)));
            } else if (arg.equals("--assetsDir") && index + 1 < original.size()) {
                result.add(Path.of(original.get(++index)).toAbsolutePath().toString());
            }
        }
        // JVM 属性必须在主类之前；原参数的第一个位置始终属于 JVM 参数区。
        result.add(0, "-Dfabric.modsFolder=" + gameDir.toAbsolutePath().resolve("mods"));
        result.add("--gameDir");
        result.add(gameDir.toAbsolutePath().toString());
        result.add("--quickPlayMultiplayer");
        result.add(address);
        return List.copyOf(result);
    }

    private static String absolutePathList(String value) {
        String[] paths = value.split(Pattern.quote(File.pathSeparator), -1);
        for (int i = 0; i < paths.length; i++) paths[i] = Path.of(paths[i]).toAbsolutePath().toString();
        return String.join(File.pathSeparator, paths);
    }
}
