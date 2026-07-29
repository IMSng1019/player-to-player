package imsng.player_to_player.server;

import imsng.player_to_player.group.ExitPayloadProtocol;
import imsng.player_to_player.util.Sha256;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ExitPayloadAssemblerTest {

    private static final ExitPayloadAssembler.Key KEY = new ExitPayloadAssembler.Key(
            UUID.fromString("c768829c-9160-43c7-a4fd-ecc2290936f4"),
            "chunk", "minecraft:overworld/-7/0");

    @Test
    void fragmentsAreBoundedDeduplicatedAndVerifiedBeforeCompletion() {
        byte[] payload = new byte[ExitPayloadProtocol.FRAGMENT_BYTES * 2 + 31];
        for (int i = 0; i < payload.length; i++) {
            payload[i] = (byte) (i * 31);
        }
        ExitPayloadAssembler assembler = new ExitPayloadAssembler();
        int count = ExitPayloadProtocol.fragmentCount(payload.length);
        String sha256 = Sha256.hex(payload);

        ExitPayloadAssembler.Result first = assembler.offer(KEY, 0, count,
                payload.length, sha256, ExitPayloadProtocol.fragment(payload, 0));
        assertTrue(first.accepted());
        assertFalse(first.complete());

        ExitPayloadAssembler.Result duplicate = assembler.offer(KEY, 0, count,
                payload.length, sha256, ExitPayloadProtocol.fragment(payload, 0));
        assertTrue(duplicate.accepted());
        assertFalse(duplicate.complete());

        ExitPayloadAssembler.Result second = assembler.offer(KEY, 1, count,
                payload.length, sha256, ExitPayloadProtocol.fragment(payload, 1));
        assertTrue(second.accepted());
        assertFalse(second.complete());

        ExitPayloadAssembler.Result completed = assembler.offer(KEY, 2, count,
                payload.length, sha256, ExitPayloadProtocol.fragment(payload, 2));
        assertTrue(completed.accepted());
        assertTrue(completed.complete());
        assertArrayEquals(payload, completed.payload());
    }

    @Test
    void invalidHashAndOversizedMetadataAreRejected() {
        ExitPayloadAssembler assembler = new ExitPayloadAssembler();
        byte[] payload = new byte[]{1, 2, 3};

        ExitPayloadAssembler.Result badHash = assembler.offer(KEY, 0, 1,
                payload.length, Sha256.hex(new byte[]{9}), payload);
        assertFalse(badHash.accepted());

        ExitPayloadAssembler.Result oversized = assembler.offer(KEY, 0, 1,
                ExitPayloadProtocol.MAX_ASSEMBLED_BYTES + 1, Sha256.hex(payload), payload);
        assertFalse(oversized.accepted());
    }
}
