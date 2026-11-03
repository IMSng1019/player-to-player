package imsng.player_to_player.server;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import imsng.player_to_player.compute.ComputeTable;
import imsng.player_to_player.config.GlobalConfig;
import imsng.player_to_player.netproto.*;
import imsng.player_to_player.registry.*;
import org.junit.jupiter.api.Test;
import java.net.SocketAddress;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import static org.junit.jupiter.api.Assertions.*;

class SplitCoordinatorTest {
    private final UUID source = UUID.randomUUID(), member = UUID.randomUUID(), follower = UUID.randomUUID();
    private final UUID split = UUID.randomUUID();
    private final GroupTable groups = new GroupTable();
    private final ChunkRegistry registry = new ChunkRegistry(null, null);
    private final PlayerTable players = new PlayerTable();
    private final Map<UUID, FakeConnection> connections = new HashMap<>();
    private final List<Set<UUID>> followers = new ArrayList<>();
    private boolean failPermissionPush;
    private int permissionPushAttempts;
    private final SplitCoordinator coordinator;
    private final ChunkKey chunk = new ChunkKey("minecraft:overworld", 0, 0);

    SplitCoordinatorTest() {
        groups.createGroup(source);
        groups.addSecondary(source, member);
        groups.addSecondary(source, follower);
        for (UUID id : List.of(source, member, follower)) connections.put(id, new FakeConnection(id));
        coordinator = new SplitCoordinator(new GlobalConfig(), registry, groups, new ComputeTable(),
                players, connections::get, id -> {
                    permissionPushAttempts++;
                    if (failPermissionPush) throw new IllegalStateException("模拟权限通知发送失败");
                }, (group, set) -> followers.add(set));
    }

    private void send(String phase) {
        JsonObject json = new JsonObject();
        json.addProperty("phase", phase);
        json.addProperty("splitId", split.toString());
        json.addProperty("groupId", source.toString());
        JsonArray members = new JsonArray(); members.add(member.toString()); members.add(follower.toString());
        json.add("departingClientIds", members);
        JsonArray regions = new JsonArray(); regions.add(new SplitRegion(chunk.dimension(), -2, 2, -2, 2).toJson());
        json.add("regions", regions);
        coordinator.handle(connections.get(source), ControlMessage.of(MessageType.SPLIT_REQUEST, json));
    }

    @Test void preparationPausesEveryDepartingMemberWithoutChangingOwnerOrGroups() {
        registry.tryClaim(chunk, source);
        send("prepare");
        assertEquals(source, groups.groupOf(member));
        assertEquals(source, registry.ownerOf(chunk));
        assertEquals(MessageType.SPLIT_PREPARE, connections.get(member).last().type());
        assertEquals(MessageType.SPLIT_PREPARE, connections.get(follower).last().type());
        assertFalse(registry.tryClaim(new ChunkKey(chunk.dimension(), 1, 0), source).granted());
    }

    @Test void noRoleIsPublishedUntilOldOwnershipHasBeenReleased() {
        registry.tryClaim(chunk, source); send("prepare"); send("commit");
        assertEquals(source, groups.groupOf(member));
        assertEquals(MessageType.ERROR, connections.get(source).last().type());
        registry.release(chunk, source); send("commit");
        assertNotEquals(source, groups.groupOf(member));
        assertEquals(groups.groupOf(member), groups.groupOf(follower));
        assertEquals("committed", connections.get(source).last().json().get("phase").getAsString());
        assertEquals(1, followers.size());
        send("commit");
        assertEquals(1, followers.size(), "提交应答丢失重试不能再分组/重定向");
        send("cancel");
        assertEquals("committed", connections.get(source).last().json().get("phase").getAsString());
    }

    @Test void lateMembershipChangeRejectsCommitAndRollbackRestoresOriginalRoles() {
        send("prepare"); groups.addSecondary(source, UUID.randomUUID()); send("commit");
        assertEquals(source, groups.groupOf(member));
        send("cancel");
        assertEquals("cancelled", connections.get(source).last().json().get("phase").getAsString());
        assertEquals("secondary", connections.get(member).last().json().get("role").getAsString());
        assertTrue(registry.tryClaim(chunk, source).granted());
    }

    @Test void timeoutKeepsFenceUntilSourceFinishesRollback() {
        send("prepare"); coordinator.requestTimeout(split);
        assertFalse(registry.tryClaim(chunk, source).granted());
        assertEquals(MessageType.SPLIT_ABORT, connections.get(source).last().type());
        assertEquals(MessageType.SPLIT_FAILED, connections.get(member).last().type());
        send("status");
        assertEquals("cancel_requested", connections.get(source).last().json().get("phase").getAsString());
    }

    @Test void normalPrimaryExitTakesOverPreparedSplitWithoutDisconnectingWaitingMembers() {
        registry.tryClaim(chunk, source);
        send("prepare");
        // 复现现场：离组端已退出游戏连接等待分离，原主此时开始正常退出。
        // 正常退出应继续把这些在线成员纳入接管，不能先用 SPLIT_FAILED 踢掉他们。
        coordinator.supersedeForPrimaryExit(source);

        assertEquals(MessageType.SPLIT_PREPARE, connections.get(member).last().type(),
                "正常退出必须接替分离等待，不能关闭成员控制会话");
        assertEquals(MessageType.SPLIT_PREPARE, connections.get(follower).last().type());
        assertEquals(source, groups.groupOf(member));
        assertEquals(source, registry.ownerOf(chunk), "退出耐久提交前原主仍持有区块");
        send("commit");
        assertEquals("cancelled", connections.get(source).last().json().get("phase").getAsString());
        assertEquals(source, groups.groupOf(member), "迟到分离提交不得抢先发布新组");
        coordinator.requestTimeout(split);
        coordinator.supersedeForPrimaryExit(source);
        coordinator.abortInvolving(source);
        assertEquals(MessageType.SPLIT_PREPARE, connections.get(member).last().type(),
                "旧超时、重复退出及随后原主断线都不能再给已接替的分离发失败通知");
        assertFalse(coordinator.involvesGroup(source));
        assertTrue(registry.tryClaim(new ChunkKey(chunk.dimension(), 1, 0), source).granted(),
                "旧分离预约已撤销，全部占用后续由退出事务统一冻结和释放");
    }

    @Test void actualPrimaryDisconnectStillFailsThePreparedSplit() {
        registry.tryClaim(chunk, source);
        send("prepare");
        coordinator.abortInvolving(source);
        assertEquals(MessageType.SPLIT_FAILED, connections.get(member).last().type());
        assertEquals(source, registry.ownerOf(chunk), "异常掉线的区块仍须交由耐久清理，不可直接释放");
    }

    @Test void normalExitCannotCancelASplitThatAlreadyCommitted() {
        send("prepare");
        send("commit");
        UUID newGroup = groups.groupOf(member);
        int memberNotifications = connections.get(member).messages.size();
        int followerNotifications = connections.get(follower).messages.size();

        coordinator.supersedeForPrimaryExit(source);
        coordinator.requestTimeout(split);
        send("status");

        assertEquals("committed", connections.get(source).last().json().get("phase").getAsString());
        assertEquals(newGroup, groups.groupOf(member));
        assertEquals(memberNotifications, connections.get(member).messages.size());
        assertEquals(followerNotifications, connections.get(follower).messages.size());
    }

    @Test void permissionPushFailureDoesNotReclassifyCommittedSplitOrSkipRolePublication() {
        send("prepare");
        failPermissionPush = true;

        send("commit");

        UUID primary = groups.groupOf(member);
        assertNotEquals(source, primary);
        assertEquals(MessageType.SPLIT_ACK, connections.get(source).last().type());
        assertEquals("committed", connections.get(source).last().json().get("phase").getAsString());
        assertEquals(2, permissionPushAttempts, "单个通知失败不能跳过另一组的权限通知");
        assertEquals(MessageType.ROLE_ASSIGN, connections.get(primary).last().type());
        assertEquals(1, followers.size(), "新主和跟随成员仍须进入已经提交的组");
        send("cancel");
        assertEquals("committed", connections.get(source).last().json().get("phase").getAsString());
    }

    @Test void lostPrepareAckStillPausesAllDepartingMembersAndCanBeQueried() {
        connections.get(source).failNextSends = 1;

        send("prepare");

        assertEquals(MessageType.SPLIT_PREPARE, connections.get(member).last().type());
        assertEquals(MessageType.SPLIT_PREPARE, connections.get(follower).last().type());
        assertTrue(connections.get(source).messages.stream().noneMatch(message -> message.type() == MessageType.ERROR));
        send("status");
        assertEquals("prepared", connections.get(source).last().json().get("phase").getAsString());
        assertFalse(registry.tryClaim(chunk, source).granted());
    }

    @Test void failedPauseNotificationDoesNotRejectPreparedTransactionOrSkipOtherMembers() {
        connections.get(member).failNextSends = 1;

        send("prepare");

        assertEquals(MessageType.SPLIT_ACK, connections.get(source).last().type());
        assertEquals("prepared", connections.get(source).last().json().get("phase").getAsString());
        assertEquals(MessageType.SPLIT_PREPARE, connections.get(follower).last().type());
        assertFalse(registry.tryClaim(chunk, source).granted());
    }

    private static final class FakeConnection implements ControlConnection {
        final List<ControlMessage> messages = new ArrayList<>(); UUID peer;
        int failNextSends;
        FakeConnection(UUID peer) { this.peer = peer; }
        ControlMessage last() { assertFalse(messages.isEmpty()); return messages.get(messages.size()-1); }
        public void send(ControlMessage message) {
            if (failNextSends > 0) {
                failNextSends--;
                throw new IllegalStateException("模拟发送失败");
            }
            messages.add(message);
        }
        public CompletableFuture<ControlMessage> request(ControlMessage message) { throw new UnsupportedOperationException(); }
        public SocketAddress remoteAddress() { return null; }
        public UUID peerId() { return peer; }
        public void setPeerId(UUID value) { peer = value; }
        public boolean isOpen() { return true; }
        public void close() { }
    }
}
