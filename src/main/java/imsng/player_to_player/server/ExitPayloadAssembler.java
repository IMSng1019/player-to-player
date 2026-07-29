package imsng.player_to_player.server;

import imsng.player_to_player.group.ExitPayloadProtocol;
import imsng.player_to_player.util.Sha256;

import java.io.ByteArrayOutputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

/** 服务端退出载荷的有界、去重、带摘要校验的内存重组器。 */
final class ExitPayloadAssembler {

    private static final int MAX_ACTIVE = 64;
    private static final long MAX_ACTIVE_BYTES = 256L * 1024 * 1024;
    private static final long EXPIRE_MILLIS = 5 * 60_000L;

    record Key(UUID shutdownId, String kind, String objectId) {
        Key {
            Objects.requireNonNull(shutdownId, "shutdownId");
            Objects.requireNonNull(kind, "kind");
            Objects.requireNonNull(objectId, "objectId");
        }
    }

    record Result(boolean accepted, boolean complete, byte[] payload) {
        static Result rejected() {
            return new Result(false, false, null);
        }

        static Result partial() {
            return new Result(true, false, null);
        }
    }

    private static final class Assembly {
        final int fragmentCount;
        final int totalBytes;
        final String sha256;
        final byte[][] fragments;
        int received;
        long lastTouched = System.currentTimeMillis();

        Assembly(int fragmentCount, int totalBytes, String sha256) {
            this.fragmentCount = fragmentCount;
            this.totalBytes = totalBytes;
            this.sha256 = sha256;
            this.fragments = new byte[fragmentCount][];
        }
    }

    private final Map<Key, Assembly> active = new HashMap<>();
    private long activeBytes;

    synchronized Result offer(Key key, int index, int count, int totalBytes,
                              String sha256, byte[] fragment) {
        purgeExpired();
        if (!validMetadata(index, count, totalBytes, sha256, fragment)) {
            remove(key);
            return Result.rejected();
        }
        Assembly assembly = active.get(key);
        if (assembly == null) {
            if (active.size() >= MAX_ACTIVE
                    || activeBytes + totalBytes > MAX_ACTIVE_BYTES) {
                return Result.rejected();
            }
            assembly = new Assembly(count, totalBytes, sha256);
            active.put(key, assembly);
            activeBytes += totalBytes;
        } else if (assembly.fragmentCount != count
                || assembly.totalBytes != totalBytes
                || !assembly.sha256.equals(sha256)) {
            remove(key);
            return Result.rejected();
        }
        assembly.lastTouched = System.currentTimeMillis();
        byte[] existing = assembly.fragments[index];
        if (existing != null) {
            if (Arrays.equals(existing, fragment)) {
                return Result.partial();
            }
            remove(key);
            return Result.rejected();
        }
        assembly.fragments[index] = fragment.clone();
        assembly.received++;
        if (assembly.received != count) {
            return Result.partial();
        }

        ByteArrayOutputStream output = new ByteArrayOutputStream(totalBytes);
        for (byte[] part : assembly.fragments) {
            output.writeBytes(part);
        }
        byte[] payload = output.toByteArray();
        remove(key);
        if (payload.length != totalBytes || !Sha256.hex(payload).equals(sha256)) {
            return Result.rejected();
        }
        return new Result(true, true, payload);
    }

    private static boolean validMetadata(int index, int count, int totalBytes,
                                         String sha256, byte[] fragment) {
        if (totalBytes <= 0 || totalBytes > ExitPayloadProtocol.MAX_ASSEMBLED_BYTES
                || count != ExitPayloadProtocol.fragmentCount(totalBytes)
                || index < 0 || index >= count
                || sha256 == null || !sha256.matches("[0-9a-f]{64}")
                || fragment == null || fragment.length == 0
                || fragment.length > ExitPayloadProtocol.FRAGMENT_BYTES) {
            return false;
        }
        int expected = index == count - 1
                ? totalBytes - index * ExitPayloadProtocol.FRAGMENT_BYTES
                : ExitPayloadProtocol.FRAGMENT_BYTES;
        return fragment.length == expected;
    }

    private void purgeExpired() {
        long cutoff = System.currentTimeMillis() - EXPIRE_MILLIS;
        var iterator = active.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<Key, Assembly> entry = iterator.next();
            if (entry.getValue().lastTouched < cutoff) {
                activeBytes -= entry.getValue().totalBytes;
                iterator.remove();
            }
        }
    }

    private void remove(Key key) {
        Assembly removed = active.remove(key);
        if (removed != null) {
            activeBytes -= removed.totalBytes;
        }
    }
}
