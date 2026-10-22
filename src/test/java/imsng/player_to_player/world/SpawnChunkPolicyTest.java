package imsng.player_to_player.world;

import imsng.player_to_player.config.GlobalConfig;
import imsng.player_to_player.core.NodeMode;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.params.provider.Arguments.arguments;

/**
 * 出生点常加载的角色边界：不启动 Minecraft，也不修改全局 NodeContext。
 * nullable 配置值表示配置尚未装载，确保启动早期不会漏掉已预置的组世界。
 */
class SpawnChunkPolicyTest {

    @ParameterizedTest(name = "{0}, managedGroup={1}, disableSpawnChunks={2} -> {3}")
    @MethodSource("spawnChunkCases")
    @DisplayName("出生点常加载禁用策略覆盖三种角色、组状态和配置缺失")
    void disablesSpawnChunksOnlyForIntendedRoles(NodeMode mode, boolean managedGroup,
                                                Boolean disableSpawnChunks, boolean expected) {
        GlobalConfig config = null;
        if (disableSpawnChunks != null) {
            config = new GlobalConfig();
            config.disableSpawnChunks = disableSpawnChunks;
        }

        assertEquals(expected, SpawnChunkPolicy.shouldDisable(mode, managedGroup, config));
    }

    /** 明确枚举预期，防止测试再次复制生产代码的角色门控遗漏。 */
    static Stream<Arguments> spawnChunkCases() {
        return Stream.of(
                arguments(NodeMode.SERVER, false, true, true),
                arguments(NodeMode.SERVER, false, false, false),
                arguments(NodeMode.SERVER, false, null, false),
                arguments(NodeMode.SERVER, true, true, true),
                arguments(NodeMode.SERVER, true, false, false),
                arguments(NodeMode.SERVER, true, null, false),
                arguments(NodeMode.PROXY_SERVER, false, true, true),
                arguments(NodeMode.PROXY_SERVER, false, false, false),
                arguments(NodeMode.PROXY_SERVER, false, null, false),
                arguments(NodeMode.PROXY_SERVER, true, true, true),
                arguments(NodeMode.PROXY_SERVER, true, false, false),
                arguments(NodeMode.PROXY_SERVER, true, null, false),
                arguments(NodeMode.CLIENT, false, true, false),
                arguments(NodeMode.CLIENT, false, false, false),
                arguments(NodeMode.CLIENT, false, null, false),
                arguments(NodeMode.CLIENT, true, true, true),
                arguments(NodeMode.CLIENT, true, false, true),
                arguments(NodeMode.CLIENT, true, null, true)
        );
    }
}
