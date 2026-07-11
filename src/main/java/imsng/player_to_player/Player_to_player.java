package imsng.player_to_player;

import imsng.player_to_player.core.P2PBootstrap;
import net.fabricmc.api.ModInitializer;
import net.minecraft.resources.ResourceLocation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * player_to_player 模组主入口（服务端 + 公共初始化）。
 * <p>
 * 具体初始化流程见 {@link P2PBootstrap}；本类只负责入口挂接，保持轻薄。
 */
public class Player_to_player implements ModInitializer {
	public static final String MOD_ID = "player_to_player";

	public static final Logger LOGGER = LoggerFactory.getLogger(MOD_ID);

	@Override
	public void onInitialize() {
		// 公共引导：目录、总配置、节点模式判定、服务端/中转端生命周期挂接
		P2PBootstrap.onCommonInit();
	}

	public static ResourceLocation id(String path) {
		return new ResourceLocation(MOD_ID, path);
	}
}
