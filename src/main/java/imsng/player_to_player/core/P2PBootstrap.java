package imsng.player_to_player.core;

import imsng.player_to_player.config.GlobalConfig;
import imsng.player_to_player.config.P2PPaths;
import imsng.player_to_player.proxy.ProxyServerService;
import imsng.player_to_player.server.P2PServerService;
import net.fabricmc.api.EnvType;
import net.fabricmc.fabric.api.event.lifecycle.v1.ServerLifecycleEvents;
import net.fabricmc.loader.api.FabricLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.UUID;

/**
 * 公共引导（服务端与客户端共用的初始化流程，模组主入口调用）。
 * <p>
 * 职责（对应规范"玩家加载模组"事件的公共部分）：
 * <ol>
 *   <li>检测/创建 player-to-player 文件夹；</li>
 *   <li>载入总配置文件（首次按运行环境自动生成 mode：服务器环境 → server，客户端环境 → client）；</li>
 *   <li>填充 {@link NodeContext}；</li>
 *   <li>按模式挂接服务端/中转端服务的生命周期（客户端侧的引导在
 *       {@code client.boot.ClientBootstrap} 中完成）。</li>
 * </ol>
 */
public final class P2PBootstrap {

    private static final Logger LOGGER = LoggerFactory.getLogger("player_to_player/bootstrap");

    private P2PBootstrap() {
    }

    /** 模组主入口（ModInitializer.onInitialize）调用，双端均执行。 */
    public static void onCommonInit() {
        FabricLoader loader = FabricLoader.getInstance();
        EnvType envType = loader.getEnvironmentType();
        Path gameDir = loader.getGameDir();

        // 1. 目录骨架：没有 player-to-player 文件夹则创建（规范要求）
        P2PPaths paths = new P2PPaths(gameDir);
        try {
            paths.ensureBaseDirs();
        } catch (IOException e) {
            // 目录都建不了属于致命环境问题，直接快速失败比带病运行更安全
            throw new IllegalStateException("无法创建 player-to-player 目录: " + paths.root(), e);
        }

        // 2. 总配置：首次生成时按环境自动检测默认模式
        NodeMode defaultMode = envType == EnvType.SERVER ? NodeMode.SERVER : NodeMode.CLIENT;
        GlobalConfig config = GlobalConfig.loadOrCreate(paths.globalConfig(), defaultMode);
        NodeMode mode = config.nodeMode();

        // 物理环境与配置模式的合法性纠正：
        // 客户端物理环境上配置 server/proxy_server 无意义（没有服务端类），强制回退 client。
        if (envType == EnvType.CLIENT && mode != NodeMode.CLIENT) {
            LOGGER.warn("客户端环境下配置了 mode={}，已强制回退为 client", mode.id());
            mode = NodeMode.CLIENT;
        }

        // 3. 填充全局上下文
        NodeContext ctx = NodeContext.get();
        ctx.setPaths(paths);
        ctx.setConfig(config);
        ctx.setMode(mode);
        // 服务端/中转端没有玩家 UUID，用随机 UUID 作为节点身份；
        // 客户端在加入世界时以玩家 UUID 覆盖（见 ClientBootstrap）。
        ctx.setClientId(UUID.randomUUID());

        LOGGER.info("player_to_player 初始化完成: mode={}, root={}", mode.id(), paths.root());

        // 4. 按模式挂接生命周期
        switch (mode) {
            case SERVER -> {
                // 服务端主服务在 MC 服务器完全启动后再启动（需要注册表/世界目录就绪）
                ServerLifecycleEvents.SERVER_STARTED.register(P2PServerService::start);
                ServerLifecycleEvents.SERVER_STOPPING.register(server -> P2PServerService.stop());
            }
            case PROXY_SERVER -> {
                // 中转服务端同样宿主在一个 fabric 服务端实例上
                ServerLifecycleEvents.SERVER_STARTED.register(server -> ProxyServerService.start());
                ServerLifecycleEvents.SERVER_STOPPING.register(server -> ProxyServerService.stop());
            }
            case CLIENT -> {
                // 客户端侧引导（算力/NAT 探测、加入世界钩子）由 ClientBootstrap 负责，
                // 这里不做任何事，保持 src/main 不引用客户端类。
            }
        }
    }
}
