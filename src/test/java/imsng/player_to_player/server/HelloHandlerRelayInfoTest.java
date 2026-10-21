package imsng.player_to_player.server;

import com.google.gson.JsonObject;
import imsng.player_to_player.config.GlobalConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * {@link HelloHandler#appendRelayInfo} 与端口同步的口径一致性：
 * 服务端把 {@code relayPort} 配成 0（系统分配临时端口）时，HELLO_ACK/P2P_USE_RELAY
 * 仍必须报告实际绑定的中转端口；外部中转与"不兼任中转"不得被端口同步值覆盖。
 */
class HelloHandlerRelayInfoTest {

    @AfterEach
    void clearPublishedPorts() {
        PortSyncServer.clear();
    }

    @Test
    void selfRelayWithEphemeralPortUsesPublishedBoundPort() {
        PortSyncServer.publish(25580, 51234);
        GlobalConfig config = new GlobalConfig();
        config.serverActsAsRelay = true;
        config.relayServerAddress = "";
        config.relayPort = 0;

        JsonObject ack = new JsonObject();
        HelloHandler.appendRelayInfo(config, ack);

        assertEquals("", ack.get("relayAddress").getAsString());
        assertEquals(51234, ack.get("relayPort").getAsInt());
    }

    @Test
    void externalRelayKeepsItsOwnAddressAndPort() {
        PortSyncServer.publish(25580, 51234); // 本机没有中转；这个值不得泄漏给外部中转
        GlobalConfig config = new GlobalConfig();
        config.serverActsAsRelay = true;
        config.relayServerAddress = "10.0.0.5:30000";
        config.relayPort = 25581;

        JsonObject ack = new JsonObject();
        HelloHandler.appendRelayInfo(config, ack);

        assertEquals("10.0.0.5", ack.get("relayAddress").getAsString());
        assertEquals(30000, ack.get("relayPort").getAsInt());
    }

    @Test
    void notActingAsRelayStaysUnavailable() {
        PortSyncServer.publish(25580, 51234);
        GlobalConfig config = new GlobalConfig();
        config.serverActsAsRelay = false;
        config.relayServerAddress = "";
        config.relayPort = 25581;

        JsonObject ack = new JsonObject();
        HelloHandler.appendRelayInfo(config, ack);

        assertEquals(0, ack.get("relayPort").getAsInt());
    }
}
