package imsng.player_to_player.runtime;

import org.junit.jupiter.api.Test;
import java.net.InetSocketAddress;
import java.util.UUID;
import static org.junit.jupiter.api.Assertions.*;

class WorkerIdentityTableTest {
    @Test void authenticatedTextureSignatureSurvivesLoginAndIsDetachedFromMutableSource() throws Exception {
        var profile = new com.mojang.authlib.GameProfile(UUID.randomUUID(), "Alice");
        profile.getProperties().put("textures", new com.mojang.authlib.properties.Property("textures", "skin-data", "signature"));
        WorkerIdentityTable table = new WorkerIdentityTable();
        table.grant(41234, profile);
        profile.getProperties().clear();
        var identity = table.consume(new InetSocketAddress("127.0.0.1", 41234), "Alice");
        assertNotNull(identity);
        var restored = identity.profile();
        var texture = restored.getProperties().get("textures").iterator().next();
        assertEquals("skin-data", texture.value());
        assertEquals("signature", texture.signature());
        assertEquals(profile.getId(), restored.getId());
    }

    @Test void loginGrantIsBoundToSocketAndConsumedOnlyOnce() throws Exception {
        Class<?> type = assertDoesNotThrow(() -> Class.forName(
                "imsng.player_to_player.runtime.WorkerIdentityTable"),
                "必须实现独立服务端的已认证套接字身份绑定");
        Object table = type.getConstructor().newInstance();
        UUID player = UUID.randomUUID();
        type.getMethod("grant", int.class, UUID.class, String.class)
                .invoke(table, 41234, player, "Alice");
        var consume = type.getMethod("consume", java.net.SocketAddress.class, String.class);
        assertNull(consume.invoke(table, new InetSocketAddress("192.0.2.1", 41234), "Alice"));
        assertNull(consume.invoke(table, new InetSocketAddress("127.0.0.1", 41235), "Alice"));
        Object identity = consume.invoke(table, new InetSocketAddress("127.0.0.1", 41234), "Alice");
        assertNotNull(identity);
        assertEquals(player, identity.getClass().getMethod("playerId").invoke(identity));
        assertNull(consume.invoke(table, new InetSocketAddress("127.0.0.1", 41234), "Alice"));
    }

    @Test void wrongNameConsumesGrantWithoutGrantingAnotherIdentity() throws Exception {
        Class<?> type = assertDoesNotThrow(() -> Class.forName(
                "imsng.player_to_player.runtime.WorkerIdentityTable"));
        Object table = type.getConstructor().newInstance();
        type.getMethod("grant", int.class, UUID.class, String.class)
                .invoke(table, 41234, UUID.randomUUID(), "Alice");
        var consume = type.getMethod("consume", java.net.SocketAddress.class, String.class);
        assertNull(consume.invoke(table, new InetSocketAddress("127.0.0.1", 41234), "Bob"));
        assertNull(consume.invoke(table, new InetSocketAddress("127.0.0.1", 41234), "Alice"));
    }
}
