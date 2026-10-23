package imsng.player_to_player.server;

import org.junit.jupiter.api.Test;

import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class EnvironmentAdmissionGateTest {
    private static final String V1 = "1".repeat(64);
    private static final String V2 = "2".repeat(64);

    @Test void admitsMatchingVersionAndAllowsMembersToMerge() {
        var gate = new EnvironmentAdmissionGate();
        var groups = new GroupTable();
        UUID first = UUID.randomUUID();
        UUID second = UUID.randomUUID();
        assertEquals("", gate.admit(groups, first, V1, () -> V1, () -> groups.createGroup(first)));
        assertEquals("", gate.admit(groups, second, V1, () -> V1, () -> groups.createGroup(second)));
        assertTrue(gate.sameRuntime(first, second));
    }

    @Test void refusesNewVersionWhileAnOldGroupIsStillRunning() {
        var gate = new EnvironmentAdmissionGate();
        var groups = new GroupTable();
        UUID old = UUID.randomUUID();
        UUID next = UUID.randomUUID();
        gate.admit(groups, old, V1, () -> V1, () -> groups.createGroup(old));
        assertFalse(gate.admit(groups, next, V2, () -> V2, () -> groups.createGroup(next)).isEmpty());
        assertNull(groups.groupOf(next));
    }

    @Test void admitsNewVersionAfterAllOldGroupsHaveLeft() {
        var gate = new EnvironmentAdmissionGate();
        var groups = new GroupTable();
        UUID old = UUID.randomUUID();
        UUID next = UUID.randomUUID();
        gate.admit(groups, old, V1, () -> V1, () -> groups.createGroup(old));
        groups.removeClient(old);
        assertEquals("", gate.admit(groups, next, V2, () -> V2, () -> groups.createGroup(next)));
        assertEquals(next, groups.groupOf(next));
    }

    @Test void missingOrOutdatedRuntimeNeverRunsAssignment() {
        var gate = new EnvironmentAdmissionGate();
        var groups = new GroupTable();
        AtomicInteger changes = new AtomicInteger();
        for (String actual : new String[] { "", "invalid", V1 }) {
            assertFalse(gate.admit(groups, UUID.randomUUID(), actual, () -> V2, changes::incrementAndGet).isEmpty());
        }
        assertEquals(0, changes.get());
        assertFalse(gate.sameRuntime(UUID.randomUUID(), UUID.randomUUID()));
    }

    @Test void newHelloCannotReplaceAnExistingGroupsAdmittedVersion() {
        var gate = new EnvironmentAdmissionGate();
        var groups = new GroupTable();
        UUID client = UUID.randomUUID();
        gate.admit(groups, client, V1, () -> V1, () -> groups.createGroup(client));
        assertFalse(gate.admit(groups, client, V2, () -> V2, () -> groups.createGroup(client)).isEmpty());
        UUID oldPeer = UUID.randomUUID();
        assertEquals("", gate.admit(groups, oldPeer, V1, () -> V1, () -> groups.createGroup(oldPeer)));
        assertTrue(gate.sameRuntime(client, oldPeer));
    }

    @Test void existingGroupWithoutRuntimeEvidenceBlocksAdmission() {
        var gate = new EnvironmentAdmissionGate();
        var groups = new GroupTable();
        groups.createGroup(UUID.randomUUID());
        UUID client = UUID.randomUUID();
        assertFalse(gate.admit(groups, client, V1, () -> V1, () -> groups.createGroup(client)).isEmpty());
        assertNull(groups.groupOf(client));
    }

    @Test void secondaryKeepsAdmittedVersionWhenPromotedDuringSplit() {
        var gate = new EnvironmentAdmissionGate();
        var groups = new GroupTable();
        UUID primary = UUID.randomUUID();
        UUID secondary = UUID.randomUUID();
        gate.admit(groups, primary, V1, () -> V1, () -> groups.createGroup(primary));
        gate.admit(groups, secondary, V1, () -> V1, () -> groups.addSecondary(primary, secondary));
        groups.createGroup(secondary);
        assertTrue(gate.sameRuntime(primary, secondary));
        assertFalse(gate.admit(groups, UUID.randomUUID(), V2, () -> V2, () -> fail("版本混用")).isEmpty());
    }

    @Test void concurrentFirstAdmissionsCannotCreateDifferentRuntimeGroups() throws Exception {
        var gate = new EnvironmentAdmissionGate();
        var groups = new GroupTable();
        var executor = Executors.newFixedThreadPool(2);
        CountDownLatch start = new CountDownLatch(1);
        AtomicInteger admitted = new AtomicInteger();
        try {
            var first = executor.submit(() -> admitAfter(start, gate, groups, V1, admitted));
            var second = executor.submit(() -> admitAfter(start, gate, groups, V2, admitted));
            start.countDown();
            first.get(5, TimeUnit.SECONDS);
            second.get(5, TimeUnit.SECONDS);
            assertEquals(1, admitted.get());
            assertEquals(1, groups.primariesSnapshot().size());
        } finally { executor.shutdownNow(); }
    }

    private static void admitAfter(CountDownLatch start, EnvironmentAdmissionGate gate,
                                   GroupTable groups, String version, AtomicInteger admitted) {
        try { assertTrue(start.await(3, TimeUnit.SECONDS)); }
        catch (InterruptedException error) { throw new AssertionError(error); }
        UUID client = UUID.randomUUID();
        gate.admit(groups, client, version, () -> version, () -> {
            groups.createGroup(client);
            admitted.incrementAndGet();
        });
    }
}
