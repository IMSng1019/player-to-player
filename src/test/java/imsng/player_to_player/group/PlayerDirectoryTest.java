package imsng.player_to_player.group;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/** 跨组目录必须整表替换，不能把已退出的玩家或上一个服务器名单留在补全中。 */
class PlayerDirectoryTest {
    @Test void combinesOtherGroupsWithVanillaNamesAndRemovesDepartedPlayers() throws Exception {
        PlayerDirectory directory = new PlayerDirectory();
        directory.update(frame("Alice", "Bob"));
        assertEquals(List.of("Alice", "Bob", "Local"), directory.namesIncluding(List.of("Local", "Alice")));
        directory.update(frame("Bob"));
        assertEquals(List.of("Bob", "Local"), directory.namesIncluding(List.of("Local")));
        directory.reset();
        assertEquals(List.of("Local"), directory.namesIncluding(List.of("Local")));
    }

    @Test void memberPermissionsDoNotMasqueradeAsGlobalOnlineDirectory() throws Exception {
        PlayerDirectory directory = new PlayerDirectory();
        directory.update(frame("Remote"));
        JsonObject legacy = new JsonObject();
        legacy.add("members", new JsonArray());
        directory.update(legacy);
        assertEquals(List.of("Remote"), directory.namesIncluding(List.of()));
        directory.update(frame());
        assertEquals(List.of(), directory.namesIncluding(List.of()));
    }

    @Test void vanillaTeleportArgumentsUseRemoteNamesForBothTargetsAndDestination() {
        net.minecraft.SharedConstants.tryDetectVersion();
        net.minecraft.server.Bootstrap.bootStrap();
        var build = net.minecraft.commands.CommandBuildContext.simple(
                net.minecraft.data.registries.VanillaRegistries.createLookup(), net.minecraft.world.flag.FeatureFlags.DEFAULT_FLAGS);
        var dispatcher = new net.minecraft.commands.Commands(net.minecraft.commands.Commands.CommandSelection.ALL, build).getDispatcher();
        PlayerDirectory directory = new PlayerDirectory();
        directory.update(frame("RemotePlayer"));
        var source = new net.minecraft.commands.CommandSourceStack(net.minecraft.commands.CommandSource.NULL,
                net.minecraft.world.phys.Vec3.ZERO, net.minecraft.world.phys.Vec2.ZERO, null, 2, "Local",
                net.minecraft.network.chat.Component.literal("Local"), null, null) {
            @Override public java.util.Collection<String> getOnlinePlayerNames() {
                return directory.namesIncluding(List.of("Local"));
            }
        };
        for (String command : List.of("tp R", "tp Local R", "teleport R")) {
            var suggestions = dispatcher.getCompletionSuggestions(dispatcher.parse(command, source)).join();
            assertTrue(suggestions.getList().stream().anyMatch(s -> s.getText().equals("RemotePlayer")), command);
        }
        directory.update(frame());
        assertFalse(dispatcher.getCompletionSuggestions(dispatcher.parse("tp R", source)).join().getList()
                .stream().anyMatch(s -> s.getText().equals("RemotePlayer")));
    }

    private static JsonObject frame(String... names) {
        JsonObject json = new JsonObject();
        JsonArray players = new JsonArray();
        for (String name : names) {
            JsonObject row = new JsonObject();
            row.addProperty("uuid", UUID.nameUUIDFromBytes(name.getBytes(java.nio.charset.StandardCharsets.UTF_8)).toString());
            row.addProperty("name", name);
            players.add(row);
        }
        json.add("onlinePlayers", players);
        return json;
    }
}
