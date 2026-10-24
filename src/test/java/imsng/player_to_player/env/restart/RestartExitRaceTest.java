package imsng.player_to_player.env.restart;

import org.junit.jupiter.api.Test;
import java.lang.reflect.Proxy;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import static org.junit.jupiter.api.Assertions.*;

class RestartExitRaceTest {
    @Test void exitBetweenAliveCheckAndIdentityReadStillReleasesLaunchBarrier() {
        var source = new RestartTask.ProcessIdentity(123, 456);
        var checks = new AtomicInteger();
        ProcessHandle handle = handleWithMissingIdentity(() -> checks.getAndIncrement() == 0);
        assertDoesNotThrow(() -> RestartHelper.waitForSource(source, Optional.of(handle)));
    }

    @Test void liveProcessWithUnknownIdentityMustNotReleaseLaunchBarrier() {
        var source = new RestartTask.ProcessIdentity(123, 456);
        assertThrows(Exception.class, () -> RestartHelper.waitForSource(source,
                Optional.of(handleWithMissingIdentity(() -> true))));
    }

    /** 用 OS 接口替身固定复现微秒窗口，不依赖调度器碰巧赶上该竞态。 */
    private static ProcessHandle handleWithMissingIdentity(java.util.function.BooleanSupplier alive) {
        var info = (ProcessHandle.Info) Proxy.newProxyInstance(ProcessHandle.Info.class.getClassLoader(),
                new Class<?>[] {ProcessHandle.Info.class}, (proxy, method, args) -> Optional.empty());
        return (ProcessHandle) Proxy.newProxyInstance(ProcessHandle.class.getClassLoader(),
                new Class<?>[] {ProcessHandle.class}, (proxy, method, args) -> switch (method.getName()) {
                    case "isAlive" -> alive.getAsBoolean();
                    case "info" -> info;
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }
}
