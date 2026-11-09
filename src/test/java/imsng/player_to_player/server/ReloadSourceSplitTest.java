package imsng.player_to_player.server;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import imsng.player_to_player.compute.ComputeTable;
import imsng.player_to_player.config.GlobalConfig;
import imsng.player_to_player.netproto.*;
import imsng.player_to_player.registry.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.net.SocketAddress;
import java.util.*;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;

/** 全组重新加载时，角色提交与旧worker解除加载围栏是两个独立的确认阶段。 */
class ReloadSourceSplitTest {
    private final UUID source = UUID.randomUUID(), retained = UUID.randomUUID(), departing = UUID.randomUUID();
    private final UUID split = UUID.randomUUID();
    private final GroupTable groups = new GroupTable();
    private final ChunkRegistry registry = new ChunkRegistry(null, null);
    private final Map<UUID, FakeConnection> connections = new HashMap<>();
    private final SplitCoordinator coordinator;

    ReloadSourceSplitTest() {
        groups.createGroup(source);
        groups.addSecondary(source, retained);
        groups.addSecondary(source, departing);
        for (UUID id : List.of(source, retained, departing)) connections.put(id, new FakeConnection(id));
        coordinator = new SplitCoordinator(new GlobalConfig(), registry, groups, new ComputeTable(),
                new PlayerTable(), connections::get, ignored -> { }, (group, members) -> { });
    }

    @AfterEach void closeCoordinator() { coordinator.shutdown(); }

    @Test void reloadPreparationPausesTheEntireGroupIncludingSource() {
        send("prepare");
        for (UUID id : List.of(source, retained, departing)) {
            ControlMessage pause = lastOf(id, MessageType.SPLIT_PREPARE);
            assertTrue(pause.json().get("reloadSource").getAsBoolean());
            assertEquals(split.toString(), pause.json().get("splitId").getAsString());
            assertEquals(source, groups.groupOf(id));
        }
        assertTrue(lastOf(source, MessageType.SPLIT_ACK).json().get("reloadSource").getAsBoolean());
        assertTrue(roles(source).isEmpty());
    }

    @Test void commitStartsNewGroupButRetainedMembersWaitForSourceReady() {
        send("prepare");
        send("commit");
        assertEquals(departing, groups.groupOf(departing));
        assertEquals("primary", lastOf(departing, MessageType.ROLE_ASSIGN).json().get("role").getAsString());
        assertTrue(roles(source).isEmpty());
        assertTrue(roles(retained).isEmpty());
        assertTrue(coordinator.involvesGroup(source), "旧worker恢复完成前不能插入新的合并/分离");

        send("source_ready");
        ControlMessage sourceRole = lastOf(source, MessageType.ROLE_ASSIGN);
        assertEquals("primary", sourceRole.json().get("role").getAsString());
        assertTrue(sourceRole.json().get("reuseWorker").getAsBoolean());
        assertEquals("secondary", lastOf(retained, MessageType.ROLE_ASSIGN).json().get("role").getAsString());
        assertEquals(1, roles(departing).size(), "源组恢复不能把已分走成员拉回旧主");
        assertTrue(lastOf(source, MessageType.SPLIT_ACK).json().get("sourceResumed").getAsBoolean());
        assertFalse(coordinator.involvesGroup(source));
    }

    @Test void cancelledReloadWaitsForSourceReadyBeforeReconnectingAllOriginalMembers() {
        send("prepare");
        send("cancel");
        for (UUID id : List.of(source, retained, departing)) {
            assertTrue(roles(id).isEmpty());
            assertEquals(source, groups.groupOf(id));
        }
        assertTrue(coordinator.involvesGroup(source));
        send("source_ready");
        assertEquals("primary", lastOf(source, MessageType.ROLE_ASSIGN).json().get("role").getAsString());
        assertTrue(lastOf(source, MessageType.ROLE_ASSIGN).json().get("reuseWorker").getAsBoolean());
        assertEquals("secondary", lastOf(departing, MessageType.ROLE_ASSIGN).json().get("role").getAsString());
        assertEquals("secondary", lastOf(retained, MessageType.ROLE_ASSIGN).json().get("role").getAsString());
        assertEquals("cancelled", lastOf(source, MessageType.SPLIT_ACK).json().get("phase").getAsString());
    }

    @Test void repeatedSourceReadyReplaysSameRolesAndAcknowledgementWithoutChangingGroups() {
        send("prepare"); send("commit"); send("source_ready");
        JsonObject firstRole = lastOf(source, MessageType.ROLE_ASSIGN).json().deepCopy();
        send("source_ready");
        assertEquals(firstRole, lastOf(source, MessageType.ROLE_ASSIGN).json());
        assertEquals(2, roles(source).size());
        assertEquals(2, roles(retained).size());
        assertEquals(1, roles(departing).size());
        assertEquals(departing, groups.groupOf(departing));
        assertTrue(lastOf(source, MessageType.SPLIT_ACK).json().get("sourceResumed").getAsBoolean());
    }

    @Test void earlyAndWrongSourceReadyCannotResumeAnyone() {
        send("prepare"); send("source_ready");
        assertEquals(MessageType.ERROR, connections.get(source).last().type());
        send("commit");
        JsonObject wrong = request("source_ready"); wrong.addProperty("groupId", retained.toString());
        coordinator.handle(connections.get(retained), ControlMessage.of(MessageType.SPLIT_REQUEST, wrong));
        assertEquals(MessageType.ERROR, connections.get(retained).last().type());
        assertTrue(roles(source).isEmpty()); assertTrue(roles(retained).isEmpty());
    }

    @Test void sourceReadyIsNotAvailableForOrdinarySplit() {
        JsonObject ordinary = request("prepare"); ordinary.remove("reloadSource");
        coordinator.handle(connections.get(source), ControlMessage.of(MessageType.SPLIT_REQUEST, ordinary));
        send("commit"); send("source_ready");
        assertEquals(MessageType.ERROR, connections.get(source).last().type());
        assertTrue(roles(source).isEmpty());
    }

    @Test void reloadTimeoutRequestsCancellationWithoutClosingWaitingControlSessions() {
        send("prepare"); coordinator.requestTimeout(split);
        assertNotNull(lastOf(source, MessageType.SPLIT_ABORT));
        for (UUID id : List.of(source, retained, departing))
            assertTrue(connections.get(id).messages.stream().noneMatch(message -> message.type() == MessageType.SPLIT_FAILED));
        send("cancel"); send("source_ready");
        assertTrue(lastOf(source, MessageType.SPLIT_ACK).json().get("sourceResumed").getAsBoolean());
    }

    @Test void primaryExitSuppressesPreparedReloadResumeAndKeepsControlSessions() {
        send("prepare"); coordinator.supersedeForPrimaryExit(source);
        send("source_ready");
        assertEquals(MessageType.ERROR, connections.get(source).last().type());
        assertFalse(coordinator.involvesGroup(source));
        for (UUID id : List.of(source, retained, departing)) {
            assertTrue(roles(id).isEmpty());
            assertTrue(connections.get(id).messages.stream().noneMatch(message -> message.type() == MessageType.SPLIT_FAILED));
        }
    }

    @Test void primaryExitAlsoSuppressesCommittedButNotYetResumedSource() {
        send("prepare"); send("commit"); coordinator.supersedeForPrimaryExit(source);
        send("source_ready");
        assertEquals(MessageType.ERROR, connections.get(source).last().type());
        assertTrue(roles(source).isEmpty()); assertTrue(roles(retained).isEmpty());
        assertEquals(departing, groups.groupOf(departing), "不能撤销已提交的新组");
    }

    @Test void disconnectedSourceFailsAllPausedMembersAndCannotResumeLater() {
        send("prepare"); coordinator.abortInvolving(source);
        assertNotNull(lastOf(retained, MessageType.SPLIT_FAILED));
        assertNotNull(lastOf(departing, MessageType.SPLIT_FAILED));
        send("source_ready");
        assertEquals(MessageType.ERROR, connections.get(source).last().type());
        assertTrue(roles(source).isEmpty());
    }

    @Test void oldAndReplacementSourceConnectionsCannotResumeOldTransaction() {
        send("prepare"); send("commit");
        FakeConnection old = connections.get(source), replacement = new FakeConnection(source);
        connections.put(source, replacement);
        coordinator.handle(old, ControlMessage.of(MessageType.SPLIT_REQUEST, request("source_ready")));
        assertEquals(MessageType.ERROR, old.last().type());
        send("source_ready");
        assertEquals(MessageType.ERROR, replacement.last().type());
        assertTrue(roles(source).isEmpty()); assertTrue(roles(retained).isEmpty());
    }

    @Test void sourceGroupRemovedBeforeReadyCannotBeResurrected() {
        send("prepare"); send("commit"); groups.removeClient(source);
        send("source_ready");
        assertEquals(MessageType.ERROR, connections.get(source).last().type());
        assertTrue(roles(source).isEmpty()); assertTrue(roles(retained).isEmpty());
        assertEquals(departing, groups.groupOf(departing));
    }

    @Test void completedSourceReadyCannotOverrideANewerSplit() {
        send("prepare"); send("commit"); send("source_ready");
        JsonObject newer = request("prepare"); newer.addProperty("splitId", UUID.randomUUID().toString());
        JsonArray members = new JsonArray(); members.add(retained.toString()); newer.add("departingClientIds", members);
        coordinator.handle(connections.get(source), ControlMessage.of(MessageType.SPLIT_REQUEST, newer));
        int before = roles(source).size();
        send("source_ready");
        assertEquals(MessageType.ERROR, connections.get(source).last().type());
        assertEquals(before, roles(source).size());
        assertEquals(source, groups.groupOf(retained));
    }

    @Test void sourceDisconnectAfterCommitDoesNotFailTheNewGroup() {
        send("prepare"); send("commit");
        coordinator.abortInvolving(source);
        assertNotNull(lastOf(retained, MessageType.SPLIT_FAILED));
        assertTrue(connections.get(departing).messages.stream().noneMatch(message -> message.type() == MessageType.SPLIT_FAILED));
        assertEquals(departing, groups.groupOf(departing));
        send("source_ready");
        assertEquals(MessageType.ERROR, connections.get(source).last().type());
    }

    @Test void productionSplitEntryRejectsSourceReadyWhileExitOwnsTheGroup() {
        Map<MessageType, MessageHandler> handlers = new EnumMap<>(MessageType.class);
        MergeCoordinator merges = MergeCoordinator.register(handlers::put, new GlobalConfig(), registry,
                groups, new ComputeTable(), new PlayerTable(), new PermissionRelay(null, groups));
        try {
            merges.setExitHandoffGate(group -> group.equals(source));
            handlers.get(MessageType.SPLIT_REQUEST).handle(connections.get(source),
                    ControlMessage.of(MessageType.SPLIT_REQUEST, request("source_ready")));
            assertEquals(MessageType.ERROR, connections.get(source).last().type());
            assertEquals("group_exiting", connections.get(source).last().json().get("code").getAsString());
            assertTrue(roles(source).isEmpty());
        } finally {
            merges.shutdown();
        }
    }

    private void send(String phase) {
        coordinator.handle(connections.get(source), ControlMessage.of(MessageType.SPLIT_REQUEST, request(phase)));
    }

    private JsonObject request(String phase) {
        JsonObject json = new JsonObject();
        json.addProperty("phase", phase); json.addProperty("splitId", split.toString());
        json.addProperty("groupId", source.toString()); json.addProperty("reloadSource", true);
        JsonArray members = new JsonArray(); members.add(departing.toString()); json.add("departingClientIds", members);
        JsonArray regions = new JsonArray(); regions.add(new SplitRegion("minecraft:overworld", -30, 60, -30, 60).toJson());
        json.add("regions", regions);
        return json;
    }

    private List<ControlMessage> roles(UUID client) {
        return connections.get(client).messages.stream().filter(message -> message.type() == MessageType.ROLE_ASSIGN).toList();
    }

    private ControlMessage lastOf(UUID client, MessageType type) {
        return connections.get(client).messages.stream().filter(message -> message.type() == type)
                .reduce((first, second) -> second).orElseThrow(() -> new AssertionError("未收到 " + type + ": " + client));
    }

    private static final class FakeConnection implements ControlConnection {
        final UUID peer;
        final List<ControlMessage> messages = new ArrayList<>();
        FakeConnection(UUID peer) { this.peer = peer; }
        ControlMessage last() { return messages.get(messages.size() - 1); }
        public void send(ControlMessage message) { messages.add(message); }
        public CompletableFuture<ControlMessage> request(ControlMessage message) { throw new UnsupportedOperationException(); }
        public SocketAddress remoteAddress() { return null; }
        public UUID peerId() { return peer; }
        public void setPeerId(UUID value) { throw new UnsupportedOperationException(); }
        public boolean isOpen() { return true; }
        public void close() { }
    }
}
