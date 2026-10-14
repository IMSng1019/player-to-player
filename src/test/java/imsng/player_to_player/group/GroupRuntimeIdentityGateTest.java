package imsng.player_to_player.group;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 本地存档身份门控的镜像语义（{@code ChunkMapMixin} 的本地磁盘回退读前提消费它）。
 * <p>
 * 该谓词是 main 源集里的一个 volatile 镜像：校验本身在客户端编排方
 * （{@code world.LocalSaveGuard}）完成，arm 时带进组计划。它必须满足：
 * 只有"确实带入了非空身份"才算已校验；未 arm、身份为空、disarm 之后一律为未校验
 * —— 否则未校验的本地存档会被放行回退读，读到旧世界区块并上行覆盖服务端。
 */
class GroupRuntimeIdentityGateTest {

    @AfterEach
    void reset() {
        GroupRuntime.disarm();
    }

    @Test
    void notVerifiedBeforeArmAndAfterDisarm() {
        assertFalse(GroupRuntime.isLocalWorldIdentityVerified(),
                "未 arm 时不得视为已校验");

        GroupRuntime.arm(plan("abc12345"));
        assertTrue(GroupRuntime.isLocalWorldIdentityVerified(),
                "arm 带入非空身份后应视为已校验");

        GroupRuntime.disarm();
        assertFalse(GroupRuntime.isLocalWorldIdentityVerified(),
                "撤销预置后必须回到未校验（身份结论不跨接管残留）");
    }

    @Test
    void emptyOrMissingIdentityIsNotVerified() {
        GroupRuntime.arm(plan(""));
        assertFalse(GroupRuntime.isLocalWorldIdentityVerified(),
                "空身份（服务端骨架给不出身份）不得视为已校验");

        GroupRuntime.arm(plan(null));
        assertFalse(GroupRuntime.isLocalWorldIdentityVerified(),
                "null 身份不得视为已校验（否则门控会被短路放行）");
    }

    private static GroupRuntime.GroupPlan plan(String identity) {
        return new GroupRuntime.GroupPlan(UUID.randomUUID(), null, identity, 5, null);
    }
}
