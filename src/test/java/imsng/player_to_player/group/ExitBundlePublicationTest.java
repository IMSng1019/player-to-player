package imsng.player_to_player.group;

import org.junit.jupiter.api.Test;

import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ExitBundlePublicationTest {

    @Test
    void retriesBuildAndListenerBeforeClearingCapturedState() {
        UUID id = UUID.randomUUID();
        ExitManifest manifest = new ExitManifest(
                id, id, id, Set.of(id), Set.of());
        AtomicInteger builds = new AtomicInteger();
        AtomicInteger deliveries = new AtomicInteger();
        AtomicInteger waits = new AtomicInteger();
        AtomicBoolean cleared = new AtomicBoolean();

        ExitBundlePublication.publishUntilAccepted(
                () -> {
                    if (builds.incrementAndGet() == 1) {
                        throw new IllegalStateException("player file temporarily unavailable");
                    }
                    return new ExitBundle(
                            manifest, java.util.Map.of(id, new byte[]{1}),
                            java.util.Map.of(), Set.of());
                },
                bundle -> {
                    if (deliveries.incrementAndGet() == 1) {
                        throw new IllegalStateException("listener temporarily unavailable");
                    }
                },
                ignored -> {
                },
                waits::incrementAndGet,
                () -> cleared.set(true));

        assertEquals(3, builds.get());
        assertEquals(2, deliveries.get());
        assertEquals(2, waits.get());
        assertTrue(cleared.get());
    }
}
