package imsng.player_to_player.group;

import java.util.Objects;
import java.util.function.Consumer;

/**
 * 只允许一个当前值存在的可替换注册槽。
 *
 * <p>适用于监听器这类必须成对执行“注册/注销”的生命周期资源。调用
 * {@link #replace(Object)} 时会先注销旧实例再注册新实例；对同一引用重复调用保持
 * 幂等，避免同一个回调被重复挂接。所有状态变更串行化，允许不同生命周期线程安全地
 * 调用 {@link #replace(Object)} 和 {@link #clear()}。</p>
 *
 * @param <T> 被注册对象的类型
 */
public final class ReplaceableRegistration<T> {

    /** 实际执行注册和注销的底层操作。 */
    private final Consumer<T> register;
    private final Consumer<T> unregister;

    /** 当前已成功注册的实例；null 表示注册槽为空。 */
    private T current;

    public ReplaceableRegistration(Consumer<T> register, Consumer<T> unregister) {
        this.register = Objects.requireNonNull(register, "register");
        this.unregister = Objects.requireNonNull(unregister, "unregister");
    }

    /**
     * 用新实例替换当前注册；同一引用已经注册时直接返回。
     *
     * <p>状态在调用底层操作前先置空。这样即使注销或新注册抛出异常，注册槽也不会
     * 错误地声称旧实例仍处于有效状态，后续生命周期仍可重新尝试注册。</p>
     */
    public synchronized void replace(T value) {
        T next = Objects.requireNonNull(value, "value");
        if (current == next) {
            return;
        }

        T previous = current;
        current = null;
        if (previous != null) {
            unregister.accept(previous);
        }
        register.accept(next);
        current = next;
    }

    /**
     * 注销当前实例并清空注册槽；没有当前实例时为空操作。
     */
    public synchronized void clear() {
        T previous = current;
        current = null;
        if (previous != null) {
            unregister.accept(previous);
        }
    }
}
