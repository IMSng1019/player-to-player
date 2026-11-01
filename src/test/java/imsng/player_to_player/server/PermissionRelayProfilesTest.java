package imsng.player_to_player.server;

import com.google.gson.JsonArray;
import com.mojang.authlib.GameProfile;
import com.mojang.authlib.properties.Property;
import imsng.player_to_player.netproto.PlayerProfileWire;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/** 物理服移除原版玩家连接以后，权限下发仍必须携带该次认证的纹理及签名。 */
class PermissionRelayProfilesTest {
    @Test void permissionFramePreservesAuthenticatedPropertiesUntilControlSessionEnds() throws Exception {
        UUID playerId = UUID.randomUUID();
        GameProfile authenticated = new GameProfile(playerId, "Alice");
        authenticated.getProperties().put("textures", new Property("textures", "skin-value", "skin-signature"));
        PermissionRelay.rememberProfile(authenticated);
        authenticated.getProperties().clear();
        var build = PermissionRelay.class.getDeclaredMethod("buildMemberArray", List.class);
        build.setAccessible(true);
        try {
            var frame = (JsonArray) build.invoke(null, List.of(new PermissionRelay.MemberPermission(playerId, "Alice", 2)));
            GameProfile received = PlayerProfileWire.read(playerId, "Alice", frame.get(0).getAsJsonObject().get("properties"));
            Property texture = received.getProperties().get("textures").iterator().next();
            assertEquals("skin-value", texture.value());
            assertEquals("skin-signature", texture.signature());
            // UUID相同但名字不同不能借用旧认证资料。
            var renamed = (JsonArray) build.invoke(null, List.of(new PermissionRelay.MemberPermission(playerId, "Bob", 2)));
            assertTrue(renamed.get(0).getAsJsonObject().getAsJsonArray("properties").isEmpty());
        } finally {
            PermissionRelay.forgetProfile(playerId);
        }
        var forgotten = (JsonArray) build.invoke(null, List.of(new PermissionRelay.MemberPermission(playerId, "Alice", 2)));
        assertTrue(forgotten.get(0).getAsJsonObject().getAsJsonArray("properties").isEmpty());
    }
}
