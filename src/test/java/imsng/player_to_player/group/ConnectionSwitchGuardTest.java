package imsng.player_to_player.group;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;

/** 复现 Fabric 在 Netty 线程发出断连事件、原版随后重复清理连接的实际时序。 */
class ConnectionSwitchGuardTest {
    @Test void nettyDisconnectMustNotEndTheRenderThreadSwitch() {
        var guard = new ConnectionSwitchGuard();
        Object oldConnection = new Object();
        guard.run(oldConnection, () -> {
            assertTrue(guard.isSwitching());
            // ClientLevel.disconnect 关闭 channel 时，Fabric 已在 Netty 线程消费断连事件。
            CompletableFuture.runAsync(() -> {
                assertFalse(guard.isSwitching(), "旁路只属于执行切换的线程");
                assertTrue(guard.consumeDisconnect(oldConnection));
            }).join();
            assertTrue(guard.isSwitching(), "紧随其后的 Minecraft.disconnect 不能启动耐久退出");
            assertTrue(guard.isSwitching(), "ConnectScreen.startConnecting 内第二次清理也必须受保护");
        });
        assertFalse(guard.isSwitching(), "切换完成后真正退出必须重新经过耐久屏障");
        assertFalse(guard.consumeDisconnect(oldConnection), "同一旧连接事件只可消费一次");
    }

    @Test void lateOldDisconnectCannotConsumeTheNewConnectionsExit() {
        var guard = new ConnectionSwitchGuard();
        Object oldConnection = new Object();
        Object newConnection = new Object();
        guard.run(oldConnection, () -> assertTrue(guard.isSwitching()));
        assertFalse(guard.isSwitching());
        assertFalse(guard.consumeDisconnect(newConnection));
        assertTrue(guard.consumeDisconnect(oldConnection), "迟到旧事件仍需保留中心控制会话");
        assertFalse(guard.consumeDisconnect(newConnection));
    }

    @Test void connectingWithoutAWorldStillProtectsVanillaCleanup() {
        var guard = new ConnectionSwitchGuard();
        guard.run(null, () -> assertTrue(guard.isSwitching(),
                "分离后的空世界也会在 ConnectScreen.startConnecting 内调用 disconnect"));
        assertFalse(guard.isSwitching());
        assertFalse(guard.consumeDisconnect(null));
        assertFalse(guard.consumeDisconnect(new Object()));
    }

    @Test void nestedCleanupKeepsOuterScopeUntilItsOwnReturn() {
        var guard = new ConnectionSwitchGuard();
        guard.run(null, () -> {
            guard.run(null, () -> assertTrue(guard.isSwitching()));
            assertTrue(guard.isSwitching());
        });
        assertFalse(guard.isSwitching());
    }

    @Test void failedTransitionCannotLeaveAnExitBypassOrUnusedDisconnectPermit() {
        var guard = new ConnectionSwitchGuard();
        Object connection = new Object();
        assertThrows(IllegalStateException.class, () -> guard.run(connection, () -> {
            throw new IllegalStateException("关闭旧连接前失败");
        }));
        assertFalse(guard.isSwitching());
        assertFalse(guard.consumeDisconnect(connection));
    }

    @Test void sessionTeardownClearsOnlyPendingEventsNotAnExecutingScope() {
        var guard = new ConnectionSwitchGuard();
        Object connection = new Object();
        guard.run(connection, () -> {
            guard.clear();
            assertTrue(guard.isSwitching());
            assertFalse(guard.consumeDisconnect(connection));
        });
        assertFalse(guard.isSwitching());
    }

    @Test void connectionsAreComparedByIdentityAndEachOldConnectionHasItsOwnEvent() {
        var guard = new ConnectionSwitchGuard();
        Object first = new String("equal-value");
        Object second = new String("equal-value");
        guard.run(first, () -> {});
        assertFalse(guard.consumeDisconnect(second));
        guard.run(second, () -> {});
        assertTrue(guard.consumeDisconnect(first));
        assertTrue(guard.consumeDisconnect(second));
    }
}
