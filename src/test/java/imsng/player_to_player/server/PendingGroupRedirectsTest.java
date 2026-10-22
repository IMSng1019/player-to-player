package imsng.player_to_player.server;

import org.junit.jupiter.api.Test;

import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class PendingGroupRedirectsTest {
    private final PendingGroupRedirects redirects = new PendingGroupRedirects();
    private final UUID group = UUID.randomUUID();
    private final Set<UUID> followers = Set.of(UUID.randomUUID());

    @Test void oldTimeoutCannotRemoveNextSplitWithIdenticalGroupAndMembers() {
        var first = redirects.register(group, followers);
        assertSame(first, redirects.ready(group));
        var next = redirects.register(group, followers);

        assertNull(redirects.expire(group, first), "前次已就绪，但未取消的旧定时器不能踢出下一次等待的成员");
        assertSame(next, redirects.ready(group));
        assertEquals(followers, next.members());
    }

    @Test void replacedWaitIsNotRemovedByItsOldTimer() {
        var old = redirects.register(group, followers);
        var current = redirects.register(group, followers);
        assertNull(redirects.expire(group, old));
        assertSame(current, redirects.expire(group, current));
        assertNull(redirects.ready(group), "本轮超时只消费一次，不得再发布就绪角色");
    }

    @Test void readinessWinsAgainstItsOwnLaterTimeout() {
        var current = redirects.register(group, followers);
        assertSame(current, redirects.ready(group));
        assertNull(redirects.expire(group, current));
    }

    @Test void shutdownDoesNotLetOldTimerConsumeAReusedGroup() {
        var old = redirects.register(group, followers);
        redirects.clear();
        var current = redirects.register(group, followers);
        assertNull(redirects.expire(group, old));
        assertSame(current, redirects.ready(group));
    }
}
