package imsng.player_to_player.server;

import org.junit.jupiter.api.Test;
import java.util.UUID;
import static org.junit.jupiter.api.Assertions.*;

class PreparedGroupMergeTest {
    @Test
    void lateMemberMustNotBeMergedWithoutPreparedTransport() {
        GroupTable groups = new GroupTable();
        UUID a = UUID.randomUUID(), b = UUID.randomUUID(), late = UUID.randomUUID();
        groups.createGroup(a); groups.createGroup(b);
        var prepared = groups.membersOf(a);
        groups.addSecondary(a, late);
        assertNull(groups.mergePreparedGroups(a, b, b, a, prepared));
        assertEquals(a, groups.groupOf(late));
        assertEquals(a, groups.primaryOf(a));
        assertNotNull(groups.mergePreparedGroups(a, b, b, a, groups.membersOf(a)));
        assertEquals(b, groups.groupOf(late));
    }
}
