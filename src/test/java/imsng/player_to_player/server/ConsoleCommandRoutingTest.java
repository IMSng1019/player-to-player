package imsng.player_to_player.server;

import com.mojang.brigadier.CommandDispatcher;
import imsng.player_to_player.registry.ChunkKey;
import imsng.player_to_player.registry.PlayerTable;
import net.minecraft.SharedConstants;
import net.minecraft.commands.CommandBuildContext;
import net.minecraft.commands.CommandSource;
import net.minecraft.commands.CommandSourceStack;
import net.minecraft.commands.Commands;
import net.minecraft.data.registries.VanillaRegistries;
import net.minecraft.network.chat.Component;
import net.minecraft.server.Bootstrap;
import net.minecraft.world.flag.FeatureFlags;
import net.minecraft.world.phys.Vec2;
import net.minecraft.world.phys.Vec3;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/** 对真实原版指令树测试：路由到玩家所属组，保留控制台位置，拒绝无法安全定位的执行。 */
class ConsoleCommandRoutingTest {
    private static final String DIMENSION = "minecraft:overworld";
    private static final UUID ALICE = UUID.randomUUID();
    private static final UUID BOB = UUID.randomUUID();
    private static final UUID GROUP_A = UUID.randomUUID();
    private static final UUID GROUP_B = UUID.randomUUID();
    private static CommandDispatcher<CommandSourceStack> dispatcher;

    @BeforeAll
    static void bootstrap() {
        SharedConstants.tryDetectVersion();
        Bootstrap.bootStrap();
        var build = CommandBuildContext.simple(VanillaRegistries.createLookup(), FeatureFlags.DEFAULT_FLAGS);
        dispatcher = new Commands(Commands.CommandSelection.ALL, build).getDispatcher();
    }

    private ConsoleCommandRouting.Decision plan(String command, UUID bobGroup, Map<ChunkKey, UUID> owners) {
        CommandSourceStack source = source();
        Map<String, PlayerTable.PlayerEntry> players = Map.of(
                "Alice", new PlayerTable.PlayerEntry(ALICE, "Alice", DIMENSION, 5000, 70, 5000, GROUP_A, "primary"),
                "Bob", new PlayerTable.PlayerEntry(BOB, "Bob", DIMENSION, 6000, 70, 6000, bobGroup, "secondary"));
        return ConsoleCommandRouting.plan(command, dispatcher, source, DIMENSION,
                players::get, id -> ALICE.equals(id) ? GROUP_A : bobGroup, owners::get);
    }

    private static CommandSourceStack source() {
        return new CommandSourceStack(CommandSource.NULL,
                new Vec3(100, 64, 200), Vec2.ZERO, null, 4, "Server",
                Component.literal("Server"), null, null);
    }

    @Test
    void namedGamemodeTargetsOnlyPlayersGroup() {
        var decision = plan("gamemode creative Alice", GROUP_B, Map.of());
        assertEquals(ConsoleCommandRouting.Kind.FORWARD, decision.kind());
        assertEquals(GROUP_A, decision.groupId());
        assertEquals("gamemode creative Alice", decision.command());
    }

    @Test
    void relativeTeleportUsesConsolePositionInsteadOfTargetPosition() {
        var decision = plan("tp Alice ~1 ~ ~-2", GROUP_B, Map.of());
        assertEquals(ConsoleCommandRouting.Kind.FORWARD, decision.kind());
        assertEquals(GROUP_A, decision.groupId());
        assertEquals("execute in minecraft:overworld run teleport Alice 101.0 64.0 198.0", decision.command());
    }

    @Test
    void localCoordinatesUseConsoleRotation() {
        var decision = plan("tp Alice ^ ^ ^2", GROUP_B, Map.of());
        assertEquals(ConsoleCommandRouting.Kind.FORWARD, decision.kind());
        String[] fields = decision.command().split(" ");
        assertEquals(100, Double.parseDouble(fields[6]), 0.0001);
        assertEquals(64, Double.parseDouble(fields[7]), 0.0001);
        assertEquals(202, Double.parseDouble(fields[8]), 0.0001);
    }

    @Test
    void destinationOwnedByDifferentGroupIsRejectedBeforeSending() {
        var decision = plan("tp Alice 0.5 80 0.5", GROUP_B,
                Map.of(new ChunkKey(DIMENSION, 0, 0), GROUP_B));
        assertEquals(ConsoleCommandRouting.Kind.REJECT, decision.kind());
        assertFalse(decision.message().isBlank());
    }

    @Test
    void sameGroupPlayerDestinationPreservesEntityTeleportCommand() {
        var decision = plan("tp Alice Bob", GROUP_A, Map.of());
        assertEquals(ConsoleCommandRouting.Kind.FORWARD, decision.kind());
        assertEquals("teleport Alice Bob", decision.command());
    }

    @Test
    void crossGroupPlayerDestinationIsExplicitlyRejected() {
        var decision = plan("tp Alice Bob", GROUP_B, Map.of());
        assertEquals(ConsoleCommandRouting.Kind.REJECT, decision.kind());
        assertTrue(decision.message().contains("坐标"));
    }

    @Test
    void selectorsAndUnknownPlayersAreRejectedInsteadOfBroadcast() {
        assertEquals(ConsoleCommandRouting.Kind.REJECT,
                plan("gamemode creative @a", GROUP_B, Map.of()).kind());
        assertEquals(ConsoleCommandRouting.Kind.REJECT,
                plan("gamemode creative Missing", GROUP_B, Map.of()).kind());
    }

    @Test
    void administrativeCommandsRemainVanillaLocalExecution() {
        assertEquals(ConsoleCommandRouting.Kind.LOCAL, plan("op Alice", GROUP_B, Map.of()).kind());
        assertEquals(ConsoleCommandRouting.Kind.LOCAL, plan("deop Alice", GROUP_B, Map.of()).kind());
        assertEquals(ConsoleCommandRouting.Kind.LOCAL, plan("stop", GROUP_B, Map.of()).kind());
    }

    @Test
    void safeLocalQueriesKeepTheirVanillaConsoleBehavior() {
        assertEquals(ConsoleCommandRouting.Kind.LOCAL, plan("help", GROUP_B, Map.of()).kind());
        assertEquals(ConsoleCommandRouting.Kind.LOCAL, plan("help tp", GROUP_B, Map.of()).kind());
        assertEquals(ConsoleCommandRouting.Kind.LOCAL, plan("seed", GROUP_B, Map.of()).kind());
    }

    @Test
    void unsupportedWorldCommandReportsFailureRatherThanPhysicalWorldSuccess() {
        var decision = plan("setblock 0 64 0 stone", GROUP_B, Map.of());
        assertEquals(ConsoleCommandRouting.Kind.REJECT, decision.kind());
        assertFalse(decision.message().isBlank());
    }

    @Test
    void largeCoordinatesRemainValidVanillaNumericLiterals() {
        var decision = plan("tp Alice 10000000 80 0", GROUP_B, Map.of());
        assertEquals(ConsoleCommandRouting.Kind.FORWARD, decision.kind());
        assertDoesNotThrow(() -> Commands.validateParseResults(dispatcher.parse(decision.command(), source())));
    }
}
