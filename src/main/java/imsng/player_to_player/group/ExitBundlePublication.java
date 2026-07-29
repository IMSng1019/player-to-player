package imsng.player_to_player.group;

import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Supplier;

/** 最终 bundle 的不可取消构建与发布重试循环。 */
public final class ExitBundlePublication {

    private ExitBundlePublication() {
    }

    public static void publishUntilAccepted(
            Supplier<ExitBundle> builder,
            Consumer<ExitBundle> listener,
            Consumer<RuntimeException> failureListener,
            Runnable retryWaiter,
            Runnable afterPublished) {
        Objects.requireNonNull(builder, "builder");
        Objects.requireNonNull(listener, "listener");
        Objects.requireNonNull(failureListener, "failureListener");
        Objects.requireNonNull(retryWaiter, "retryWaiter");
        Objects.requireNonNull(afterPublished, "afterPublished");
        while (true) {
            try {
                listener.accept(builder.get());
                afterPublished.run();
                return;
            } catch (RuntimeException e) {
                failureListener.accept(e);
                retryWaiter.run();
            }
        }
    }
}
