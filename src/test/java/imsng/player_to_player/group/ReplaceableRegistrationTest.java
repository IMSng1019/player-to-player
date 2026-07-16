package imsng.player_to_player.group;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * {@link ReplaceableRegistration} 的监听器生命周期测试。
 *
 * <p>组宿主可能在同一世界会话内经历“主客户端 → 副客户端 → 再次成为主客户端”。
 * 注册槽必须先注销旧监听器再挂接新监听器，并且对同一实例重复注册保持幂等，
 * 否则同一条 P2P 会话会被多个宿主监听器并发消费。</p>
 */
class ReplaceableRegistrationTest {

    @Test
    void replacesThePreviousRegistrationInLifecycleOrder() {
        List<String> events = new ArrayList<>();
        Object first = new Object();
        Object second = new Object();
        ReplaceableRegistration<Object> registration = new ReplaceableRegistration<>(
                value -> events.add(value == first ? "add-first" : "add-second"),
                value -> events.add(value == first ? "remove-first" : "remove-second"));

        registration.replace(first);
        registration.replace(second);

        assertEquals(List.of("add-first", "remove-first", "add-second"), events);
    }

    @Test
    void registeringTheSameInstanceTwiceIsIdempotent() {
        List<String> events = new ArrayList<>();
        Object listener = new Object();
        ReplaceableRegistration<Object> registration = new ReplaceableRegistration<>(
                ignored -> events.add("add"),
                ignored -> events.add("remove"));

        registration.replace(listener);
        registration.replace(listener);

        assertEquals(List.of("add"), events);
    }

    @Test
    void clearUnregistersTheCurrentInstanceOnlyOnce() {
        List<String> events = new ArrayList<>();
        ReplaceableRegistration<Object> registration = new ReplaceableRegistration<>(
                ignored -> events.add("add"),
                ignored -> events.add("remove"));

        registration.replace(new Object());
        registration.clear();
        registration.clear();

        assertEquals(List.of("add", "remove"), events);
    }
}
