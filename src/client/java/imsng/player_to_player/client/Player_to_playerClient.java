package imsng.player_to_player.client;

import imsng.player_to_player.client.boot.ClientBootstrap;
import net.fabricmc.api.ClientModInitializer;

/**
 * player_to_player 客户端入口。
 * <p>
 * 具体客户端引导（算力检测、NAT 探测、加入世界钩子）见 {@link ClientBootstrap}；
 * 本类只负责入口挂接。
 */
public class Player_to_playerClient implements ClientModInitializer {
	@Override
	public void onInitializeClient() {
		ClientBootstrap.onClientInit();
	}
}
