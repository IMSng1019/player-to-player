package imsng.player_to_player.server;

import com.google.gson.JsonObject;
import imsng.player_to_player.compute.ComputeScore;
import imsng.player_to_player.compute.ComputeTable;
import imsng.player_to_player.config.GlobalConfig;
import imsng.player_to_player.group.ExitManifest;
import imsng.player_to_player.group.GroupHandoffPreparation;
import imsng.player_to_player.netproto.ControlConnection;
import imsng.player_to_player.netproto.ControlMessage;
import imsng.player_to_player.netproto.MessageType;
import imsng.player_to_player.registry.PlayerTable;
import imsng.player_to_player.registry.ChunkKey;
import imsng.player_to_player.registry.ChunkRegistry;
import imsng.player_to_player.registry.SplitRegion;
import org.junit.jupiter.api.Test;

import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/** 退出必须先让仍在线成员确认等待，再在耐久释放后发布新的唯一主客户端。 */
class PrimaryExitHandoffTest {
    private final UUID primary = UUID.randomUUID();
    private final UUID successor = UUID.randomUUID();
    private final UUID follower = UUID.randomUUID();
    private final GroupTable groups = new GroupTable();
    private final PlayerTable players = new PlayerTable();
    private final ComputeTable computes = new ComputeTable();
    private final Map<UUID, FakeConnection> connections = new HashMap<>();
    private final List<String> events = new ArrayList<>();
    private final ExitManifest manifest = new ExitManifest(UUID.randomUUID(), primary, primary,
            Set.of(primary, successor, follower), Set.of());
    private final PrimaryExitHandoff handoff;

    PrimaryExitHandoffTest() {
        groups.createGroup(primary);
        groups.addSecondary(primary, successor);
        groups.addSecondary(primary, follower);
        for (UUID id : manifest.playerIds()) {
            connections.put(id, new FakeConnection(id));
            players.declare(id, id.toString());
            players.updateGroupRole(id, primary, id.equals(primary) ? "primary" : "secondary");
        }
        computes.report(successor, new ComputeScore("strong", 200, 0, "unknown", 1L << 30, 2L << 30));
        computes.report(follower, new ComputeScore("weak", 100, 0, "unknown", 1L << 30, 2L << 30));
        handoff = new PrimaryExitHandoff(new GlobalConfig(), groups, computes, players,
                connections::get, id -> events.add("permissions"), (group, members) -> {
                    assertEquals(successor, group);
                    assertEquals(Set.of(follower), members);
                    events.add("followers");
                });
    }

    @Test void preparationWaitsForEveryMemberAcknowledgementBeforeAllowingShutdown() throws Exception {
        FakeConnection slow = connections.get(follower);
        slow.reply = new CompletableFuture<>();
        CompletableFuture<Void> prepared = CompletableFuture.runAsync(() -> handoff.prepare(manifest));
        ControlMessage request = slow.requested.get(2, TimeUnit.SECONDS);
        assertEquals(MessageType.SPLIT_PREPARE, request.type());
        assertEquals("primary_exit", request.json().get("reason").getAsString());
        assertFalse(prepared.isDone(), "副客户端尚未保护游戏断线，原主不得开始停服");
        assertEquals(primary, groups.groupOf(successor));
        slow.reply.complete(ack(request));
        prepared.get(2, TimeUnit.SECONDS);
        assertTrue(handoff.isExiting(primary));
        assertTrue(connections.get(primary).messages.isEmpty());
    }

    @Test void failedAcknowledgementKeepsOriginalGroupForBeginRetry() {
        connections.get(follower).reply = CompletableFuture.failedFuture(new IllegalStateException("lost ACK"));
        assertThrows(IllegalStateException.class, () -> handoff.prepare(manifest));
        assertEquals(primary, groups.groupOf(successor));
        assertTrue(connections.get(successor).messages.isEmpty());
        connections.get(follower).reply = null;
        assertDoesNotThrow(() -> handoff.prepare(manifest));
    }

    @Test void exitReservationWaitsForAdmissionThatAlreadyHoldsGroupMutationLock() throws Exception {
        UUID newcomer = UUID.randomUUID();
        java.util.concurrent.CountDownLatch started = new java.util.concurrent.CountDownLatch(1);
        java.util.concurrent.atomic.AtomicReference<CompletableFuture<Void>> reservation =
                new java.util.concurrent.atomic.AtomicReference<>();
        groups.withMutationLock(() -> {
            reservation.set(CompletableFuture.runAsync(() -> {
                started.countDown();
                handoff.reserve(manifest);
            }));
            try {
                assertTrue(started.await(2, TimeUnit.SECONDS));
                assertThrows(java.util.concurrent.TimeoutException.class,
                        () -> reservation.get().get(100, TimeUnit.MILLISECONDS));
                assertFalse(handoff.isExiting(primary), "通过准入检查的成员必须先完成入组");
                groups.addSecondary(primary, newcomer);
                return null;
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new AssertionError(interrupted);
            }
        });
        reservation.get().get(2, TimeUnit.SECONDS);
        assertTrue(groups.membersOf(primary).contains(newcomer));
        assertTrue(handoff.isExiting(primary));
    }

    @Test void finalDataReleaseFailureNeverPublishesOrChangesSuccessor() {
        handoff.prepare(manifest);
        assertThrows(IllegalStateException.class, () -> handoff.complete(manifest, () -> {
            throw new IllegalStateException("disk unavailable");
        }));
        assertEquals(primary, groups.groupOf(successor));
        assertTrue(connections.get(successor).messages.isEmpty());
        assertTrue(events.isEmpty());
    }

    @Test void durableCompletionElectsRemainingStrongestAndRegistersFollowersBeforePublishing() {
        handoff.prepare(manifest);
        handoff.complete(manifest, () -> events.add("durable"));
        assertNull(groups.groupOf(primary));
        assertEquals(successor, groups.groupOf(successor));
        assertEquals(successor, groups.groupOf(follower));
        assertEquals("primary", players.get(successor).role());
        assertEquals("secondary", players.get(follower).role());
        assertTrue(events.indexOf("durable") < events.indexOf("followers"));
        assertTrue(events.indexOf("followers") < events.indexOf("role"));
        assertEquals("primary", connections.get(successor).messages.get(0).json().get("role").getAsString());
        assertTrue(connections.get(follower).messages.isEmpty(), "副客户端必须等新主GROUP_WORLD_READY后才能重定向");
        assertFalse(handoff.isExiting(primary));
    }

    @Test void replayAfterPublicationDoesNotRecreateGroupOrKickNewMembers() {
        handoff.complete(manifest, () -> events.add("durable"));
        UUID joinedLater = UUID.randomUUID();
        groups.addSecondary(successor, joinedLater);
        handoff.complete(manifest, () -> fail("同事务已经耐久释放，不应重复删除后来占用"));
        assertEquals(successor, groups.groupOf(joinedLater));
        assertEquals(1, connections.get(successor).messages.size());
        assertEquals(1, events.stream().filter("followers"::equals).count());
    }

    @Test void cleanupMarkerRetryDoesNotTouchFormerPrimaryAfterItRejoins() {
        handoff.complete(manifest, () -> events.add("durable"));
        groups.createGroup(primary);
        UUID newMember = UUID.randomUUID();
        groups.addSecondary(primary, newMember);
        handoff.complete(manifest, () -> fail("旧退出回放不得释放同UUID的新一轮组占用"));
        assertEquals(primary, groups.groupOf(primary));
        assertEquals(primary, groups.groupOf(newMember));
        assertFalse(handoff.isExiting(primary));
    }

    @Test void failedRoleSendCanRetryWithoutRepeatingGroupMutation() {
        connections.get(successor).failSends = true;
        assertThrows(IllegalStateException.class, () -> handoff.complete(manifest, () -> { }));
        assertEquals(successor, groups.groupOf(follower));
        UUID joinedLater = UUID.randomUUID();
        groups.addSecondary(successor, joinedLater);
        connections.get(successor).failSends = false;
        handoff.complete(manifest, () -> fail("耐久释放成功后不重复释放"));
        assertEquals(successor, groups.groupOf(joinedLater));
        assertEquals(1, connections.get(successor).messages.size());
    }

    @Test void departedOrReassignedMembersAreNotResurrectedByOldExit() {
        groups.createGroup(successor);
        connections.get(follower).open = false;
        handoff.complete(manifest, () -> { });
        assertEquals(successor, groups.groupOf(successor));
        assertNull(groups.groupOf(follower));
        assertTrue(connections.get(successor).messages.isEmpty());
    }

    @Test void mergedGroupExitRetainsBothFormerPrimaryAndNeverPrimaryWhileSplitIsWaiting() {
        // 与现场角色一致：successor 原先独立为主，follower 从未做主；合并后三人同组。
        groups.createGroup(successor);
        assertNotNull(groups.mergeGroups(primary, successor, primary));
        var registry = new ChunkRegistry(null, null);
        var chunk = new ChunkKey("minecraft:overworld", 625, 625);
        assertTrue(registry.tryClaim(chunk, primary).granted());
        var exiting = new ExitManifest(manifest.shutdownId(), primary, primary,
                manifest.playerIds(), Set.of(chunk));
        var splits = new SplitCoordinator(new GlobalConfig(), registry, groups, computes, players,
                connections::get, ignored -> { }, (group, members) -> fail("未提交分离不得重定向"));
        UUID splitId = UUID.randomUUID();
        JsonObject prepare = new JsonObject();
        prepare.addProperty("phase", "prepare");
        prepare.addProperty("splitId", splitId.toString());
        prepare.addProperty("groupId", primary.toString());
        var departing = new com.google.gson.JsonArray(); departing.add(follower.toString());
        prepare.add("departingClientIds", departing);
        var regions = new com.google.gson.JsonArray();
        regions.add(new SplitRegion(chunk.dimension(), 620, 630, 620, 630).toJson());
        prepare.add("regions", regions);
        try {
            splits.handle(connections.get(primary), ControlMessage.of(MessageType.SPLIT_REQUEST, prepare));
            assertTrue(connections.get(follower).preparation.isPrepared());

            handoff.reserve(exiting);
            splits.supersedeForPrimaryExit(primary);
            registry.freezeOrRecoverForExit(primary, exiting.chunks(), exiting.playerIds(), false);
            handoff.prepare(exiting);
            assertTrue(connections.get(follower).isOpen(), "正在分离的纯副端必须继续参加退出选举");
            assertTrue(connections.get(successor).preparation.isPrepared());
            handoff.complete(exiting, () -> registry.completeExitDurably(primary, exiting.chunks()));

            assertEquals(successor, groups.groupOf(successor));
            assertEquals(successor, groups.groupOf(follower));
            assertNull(registry.ownerOf(chunk), "新主角色只在旧所有权耐久释放后发布");
            assertEquals("primary", players.get(successor).role());
            assertEquals("secondary", players.get(follower).role());
            assertTrue(events.contains("followers"));
            assertTrue(events.contains("role"));
            assertTrue(events.indexOf("followers") < events.indexOf("role"));
        } finally {
            splits.shutdown();
        }
    }

    private static ControlMessage ack(ControlMessage request) {
        JsonObject json = request.json().deepCopy();
        json.addProperty("accepted", true);
        return request.reply(MessageType.SPLIT_PREPARE, json, null);
    }

    private final class FakeConnection implements ControlConnection {
        final UUID peer;
        final List<ControlMessage> messages = new ArrayList<>();
        final CompletableFuture<ControlMessage> requested = new CompletableFuture<>();
        final GroupHandoffPreparation preparation = new GroupHandoffPreparation();
        CompletableFuture<ControlMessage> reply;
        boolean open = true;
        boolean failSends;
        FakeConnection(UUID peer) { this.peer = peer; }
        public void send(ControlMessage message) {
            if (failSends) throw new IllegalStateException("send failed");
            messages.add(message);
            if (message.type() == MessageType.SPLIT_PREPARE) {
                preparation.prepareSplit(UUID.fromString(message.json().get("splitId").getAsString()),
                        UUID.fromString(message.json().get("groupId").getAsString()), () -> { });
            }
            if (message.type() == MessageType.SPLIT_FAILED) open = false;
            if (message.type() == MessageType.ROLE_ASSIGN) events.add("role");
        }
        public CompletableFuture<ControlMessage> request(ControlMessage message) {
            requested.complete(message);
            if (reply != null) return reply;
            ControlMessage acknowledgement = ack(message);
            boolean prepared = preparation.prepare(UUID.fromString(message.json().get("splitId").getAsString()),
                    UUID.fromString(message.json().get("groupId").getAsString()), () -> { });
            acknowledgement.json().addProperty("accepted", prepared);
            return CompletableFuture.completedFuture(acknowledgement);
        }
        public SocketAddress remoteAddress() { return null; }
        public UUID peerId() { return peer; }
        public void setPeerId(UUID ignored) { }
        public boolean isOpen() { return open; }
        public void close() { open = false; }
    }
}
