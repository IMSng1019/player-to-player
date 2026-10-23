package imsng.player_to_player.netproto;

import com.google.gson.JsonObject;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Test;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import static org.junit.jupiter.api.Assertions.*;

class EnvironmentOnlyConnectionTest {
    @Test void preflightCanReadEnvironmentButCannotClaimOrBecomePlayer() {
        AtomicInteger reads = new AtomicInteger();
        AtomicInteger authority = new AtomicInteger();
        var connection = new NettyControlConnection(Map.of(
                MessageType.ENV_PREFLIGHT, (conn, msg) -> conn.restrictToEnvironment(),
                MessageType.ENV_MANIFEST_REQUEST, (conn, msg) -> reads.incrementAndGet(),
                MessageType.HELLO, (conn, msg) -> authority.incrementAndGet(),
                MessageType.CHUNK_CLAIM_REQUEST, (conn, msg) -> authority.incrementAndGet()),
                List.of(), true, Protocol.RID_BASE_ACCEPTOR);
        EmbeddedChannel channel = new EmbeddedChannel(connection);
        try {
            channel.writeInbound(ControlMessage.of(MessageType.ENV_MANIFEST_REQUEST));
            assertEquals(0, reads.get());
            channel.writeInbound(ControlMessage.of(MessageType.ENV_PREFLIGHT, new JsonObject()));
            assertNull(connection.peerId());
            channel.writeInbound(ControlMessage.of(MessageType.ENV_MANIFEST_REQUEST));
            assertEquals(1, reads.get());
            channel.writeInbound(ControlMessage.of(MessageType.HELLO));
            channel.writeInbound(ControlMessage.of(MessageType.CHUNK_CLAIM_REQUEST));
            assertEquals(0, authority.get());
        } finally {
            channel.finishAndReleaseAll();
        }
    }
}
