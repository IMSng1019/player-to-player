package imsng.player_to_player.client.env;

import imsng.player_to_player.core.NodeContext;
import imsng.player_to_player.env.ClientEnvironmentCompatibility;
import imsng.player_to_player.env.ClientLaunchArguments;
import imsng.player_to_player.env.ClientRuntimeStartup;
import imsng.player_to_player.env.EnvironmentManifest;
import imsng.player_to_player.env.EnvironmentPreflightInfo;
import imsng.player_to_player.env.EnvironmentRuntimeIdentity;
import imsng.player_to_player.util.Sha256;
import net.fabricmc.loader.api.FabricLoader;
import net.minecraft.client.Minecraft;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * 渲染客户端的独立游戏目录及重启。Fabric 在游戏启动后不能追加模组，因此所有
 * 模组/启动配置变化都先生成新目录，由统一协调器使用当前登录身份自动重启。
 * 当前全局 mods/config 从不被覆盖；认证参数仅留在内存，不落日志和说明文件。
 */
public final class ClientRuntimeProfile {
    public static final String DATA_ROOT_PROPERTY = "player_to_player.dataRoot";
    public static final String RUNTIME_PROPERTY = "player_to_player.clientRuntime";
    public record Loaded(Map<String, ClientEnvironmentCompatibility.LoadedMod> mods,
                         Map<String, Path> clientOnlyJars) { }
    public record Installation(Path directory, String runtimeHash, String serverAddress,
                               boolean canLaunch, String limitation) {
        public List<String> commandForRestart() {
            return canLaunch ? launchCommand(directory, runtimeHash, serverAddress) : List.of();
        }
    }
    private ClientRuntimeProfile() { }

    /** 只统计顶层 Fabric 容器；内嵌模块受其顶层 JAR 哈希覆盖，避免把 API 子模块当额外模组。 */
    public static Loaded captureLoadedMods() throws IOException {
        var captured = imsng.player_to_player.env.FabricRuntimeMods.capture();
        return new Loaded(captured.mods(), captured.clientOnlyJars());
    }

    public static boolean wasLaunchedFor(String runtimeHash) {
        return runtimeHash.equals(System.getProperty(RUNTIME_PROPERTY, ""));
    }

    /** 每次安装创建新目录，避免覆盖当前运行的某个旧版本实例。 */
    public static Installation install(Path worldFolder, Path environment, EnvironmentManifest manifest,
                                       EnvironmentPreflightInfo info, Loaded loaded, String serverAddress) throws IOException {
        Path directory = worldFolder.resolve("client-runtime")
                .resolve(info.runtimeHash().substring(0, 16) + "-" + UUID.randomUUID()).resolve("game")
                .toAbsolutePath().normalize();
        Files.createDirectories(directory);
        Map<String, EnvironmentManifest.Entry> installedFiles = new java.util.TreeMap<>();
        java.util.Set<String> serverModIds = new java.util.HashSet<>();
        for (var entry : manifest.files().entrySet()) {
            String relative = entry.getKey();
            if (!EnvironmentRuntimeIdentity.isRuntimePath(relative, info.worldPath())) continue;
            // 世界数据包给 worker 的存档，渲染客户端不应凭配置目录自行打开服务端存档。
            if (relative.startsWith(info.worldPath() + "/")) continue;
            Path source = environment.toAbsolutePath().normalize().resolve(relative).normalize();
            Path target = directory.resolve(relative).normalize();
            if (!source.startsWith(environment.toAbsolutePath().normalize()) || !target.startsWith(directory)
                    || !Files.isRegularFile(source, LinkOption.NOFOLLOW_LINKS))
                throw new IOException("环境安装路径无效: " + relative);
            Files.createDirectories(target.getParent());
            Files.copy(source, target, StandardCopyOption.REPLACE_EXISTING);
            if (!Sha256.hexOfFile(target).equals(entry.getValue().sha256()))
                throw new IOException("安装后环境校验失败: " + relative);
            installedFiles.put(relative, entry.getValue());
            if (relative.startsWith("mods/") && relative.endsWith(".jar"))
                serverModIds.add(ClientEnvironmentCompatibility.metadata(target).id());
        }
        // 保留明确声明 environment=client 的渲染辅助模组；通用模组不随意混进服务器环境。
        for (var local : loaded.clientOnlyJars().entrySet()) {
            if (serverModIds.contains(local.getKey())) continue;
            Path target = directory.resolve("mods/client-local-" + local.getKey() + ".jar");
            Files.createDirectories(target.getParent());
            Files.copy(local.getValue(), target);
        }
        com.google.gson.JsonObject startupRecord = new com.google.gson.JsonObject();
        startupRecord.addProperty("runtimeHash", info.runtimeHash());
        startupRecord.add("manifest", new EnvironmentManifest(installedFiles).toJson());
        imsng.player_to_player.util.JsonUtil.writeFileAtomic(
                directory.getParent().resolve(ClientRuntimeStartup.MANIFEST_FILE), startupRecord);
        boolean canLaunch = supportsLaunch();
        String limitation = !canLaunch
                ? "当前启动方式无法安全自动重启。请在启动器中复制当前 Fabric 1.20.4 配置，将游戏目录设为下方已安装目录，然后启动并连接原服务器。"
                : "客户端将自动正常退出，再用此环境启动并连接原服务器；失败后可在游戏中手动重试。";
        Files.writeString(directory.resolve("P2P-启动说明.txt"),
                "此目录已安装服务器要求的客户端模组和配置。\n游戏目录：" + directory
                        + "\n服务器：" + serverAddress + "\n" + limitation
                        + "\n如在启动器手动启动，请添加 JVM 参数：\n-D" + DATA_ROOT_PROPERTY + "="
                        + NodeContext.get().paths().root().toAbsolutePath()
                        + "\n-D" + RUNTIME_PROPERTY + "=" + info.runtimeHash()
                        + "\n-Dfabric.modsFolder=" + directory.resolve("mods")
                        + "\n同时移除原配置中的 fabric.addMods 参数，避免混入旧模组。\n",
                StandardCharsets.UTF_8);
        return new Installation(directory, info.runtimeHash(), serverAddress, canLaunch, limitation);
    }

    /** 特殊包装启动器可能依赖额外引导状态；无法证明可还原时保留明确的手动路径。 */
    private static boolean supportsLaunch() {
        if (FabricLoader.getInstance().isDevelopmentEnvironment()) return false;
        String main = System.getProperty("sun.java.command", "").split(" ", 2)[0];
        return (main.equals("net.fabricmc.loader.impl.launch.knot.KnotClient")
                || main.equals("net.fabricmc.loader.launch.knot.KnotClient"))
                && !System.getProperty("java.class.path", "").isBlank();
    }

    private static List<String> launchCommand(Path directory, String runtimeHash, String serverAddress) {
        if (!supportsLaunch()) return List.of();
        // Windows 的 JDK17 ProcessHandle.Info 不提供 arguments，改用 Loader 已解析的
        // 原游戏参数和 JVM 自身输入参数。认证/代理参数保留在内存中，不写入磁盘或日志。
        String executable = Path.of(System.getProperty("java.home"), "bin",
                System.getProperty("os.name", "").toLowerCase(java.util.Locale.ROOT).contains("win")
                        ? "javaw.exe" : "java").toString();
        List<String> original = new ArrayList<>(java.lang.management.ManagementFactory
                .getRuntimeMXBean().getInputArguments());
        original.removeIf(arg -> arg.startsWith("-Djava.library.path=") || arg.startsWith("-Dorg.lwjgl.librarypath="));
        for (String key : List.of("java.library.path", "org.lwjgl.librarypath")) {
            String value = System.getProperty(key, "");
            if (!value.isBlank()) original.add("-D" + key + "=" + value);
        }
        original.add("-cp");
        original.add(System.getProperty("java.class.path"));
        original.add("net.fabricmc.loader.impl.launch.knot.KnotClient");
        var user = Minecraft.getInstance().getUser();
        original.addAll(ClientLaunchArguments.withIdentity(List.of(FabricLoader.getInstance().getLaunchArguments(false)),
                Map.of("--username", user.getName(), "--uuid", user.getProfileId().toString(),
                        "--accessToken", user.getAccessToken(), "--userType", user.getType().getName(),
                        "--xuid", user.getXuid().orElse(""), "--clientId", user.getClientId().orElse(""))));
        List<String> rewritten = new ArrayList<>(ClientLaunchArguments.rewrite(original, directory, serverAddress));
        rewritten.removeIf(arg -> arg.startsWith("-D" + DATA_ROOT_PROPERTY + "=")
                || arg.startsWith("-D" + RUNTIME_PROPERTY + "=") || arg.startsWith("-Duser.dir=")
                || arg.startsWith("-D" + imsng.player_to_player.env.restart.RestartTask.TASK_PROPERTY + "="));
        // Windows 控制台启动的 java.exe 改用同目录 javaw.exe，重启时不额外弹出控制台窗口。
        Path java = Path.of(executable);
        if (java.getFileName().toString().equalsIgnoreCase("java.exe")
                && Files.isRegularFile(java.resolveSibling("javaw.exe"))) java = java.resolveSibling("javaw.exe");
        List<String> command = new ArrayList<>();
        command.add(java.toString());
        command.add("-D" + DATA_ROOT_PROPERTY + "=" + NodeContext.get().paths().root().toAbsolutePath());
        command.add("-D" + RUNTIME_PROPERTY + "=" + runtimeHash);
        command.add("-Duser.dir=" + directory);
        command.addAll(rewritten);
        return List.copyOf(command);
    }
}
