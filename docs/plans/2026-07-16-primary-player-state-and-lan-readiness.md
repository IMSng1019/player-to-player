# Authoritative Primary Player State Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Make every primary-client launch use the physical server's validated live player NBT, fail closed before role assignment, and publish the integrated LAN endpoint only after the local player is ready.

**Architecture:** Add common, unit-tested helpers for player NBT validation/installation, the existing control-protocol request, and one-shot publication readiness. Keep Minecraft lifecycle classes as thin orchestration: fetch and prepare before `ROLE_REQUEST`, then launch an already-prepared world; defer `publishServer` to a client-tick condition gate.

**Tech Stack:** Java 17, Fabric API 1.20.4, Mojang mappings, Minecraft `CompoundTag`/`NbtIo`, JUnit 5, Gradle Wrapper.

---

Before Task 1, use `superpowers:using-git-worktrees` to create an isolated worktree from the commit containing this plan. Do not implement directly on `main`.

### Task 1: Validate and atomically install authoritative player NBT

**Files:**
- Create: `src/main/java/imsng/player_to_player/group/PlayerStateNbt.java`
- Create: `src/main/java/imsng/player_to_player/group/PlayerStateFiles.java`
- Create: `src/test/java/imsng/player_to_player/group/PlayerStateNbtTest.java`
- Create: `src/test/java/imsng/player_to_player/group/PlayerStateFilesTest.java`

**Step 1: Write the failing NBT validation tests**

Create `PlayerStateNbtTest` with real gzip NBT payloads. Cover a valid vanilla-shaped player tag and the exact invalid states that must stop switching:

```java
package imsng.player_to_player.group;

import net.minecraft.nbt.CompoundTag;
import net.minecraft.nbt.ListTag;
import net.minecraft.nbt.NbtIo;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class PlayerStateNbtTest {

    @Test
    void decodesValidPhysicalServerPlayerState() throws Exception {
        CompoundTag tag = validPlayer(-92.0, 119.0, 8.0);

        CompoundTag decoded = PlayerStateNbt.decodeValidated(gzip(tag));

        assertEquals("minecraft:overworld", decoded.getString("Dimension"));
        assertEquals(-92.0, decoded.getList("Pos", 6).getDouble(0));
    }

    @Test
    void rejectsMissingDimension() throws Exception {
        CompoundTag tag = validPlayer(1.0, 64.0, 1.0);
        tag.remove("Dimension");

        assertThrows(IOException.class, () -> PlayerStateNbt.decodeValidated(gzip(tag)));
    }

    @Test
    void rejectsNonFinitePosition() throws Exception {
        CompoundTag tag = validPlayer(Double.NaN, 64.0, 1.0);

        assertThrows(IOException.class, () -> PlayerStateNbt.decodeValidated(gzip(tag)));
    }

    private static CompoundTag validPlayer(double x, double y, double z) {
        CompoundTag tag = new CompoundTag();
        tag.putString("Dimension", "minecraft:overworld");
        ListTag pos = new ListTag();
        pos.add(net.minecraft.nbt.DoubleTag.valueOf(x));
        pos.add(net.minecraft.nbt.DoubleTag.valueOf(y));
        pos.add(net.minecraft.nbt.DoubleTag.valueOf(z));
        tag.put("Pos", pos);
        return tag;
    }

    private static byte[] gzip(CompoundTag tag) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        NbtIo.writeCompressed(tag, out);
        return out.toByteArray();
    }
}
```

Also add cases for an empty payload, a non-string `Dimension`, a `Pos` list with fewer than three doubles, and an over-limit decompressed payload.

**Step 2: Run the test to verify RED**

Run:

```powershell
.\gradlew.bat test --tests imsng.player_to_player.group.PlayerStateNbtTest
```

Expected: compilation fails because `PlayerStateNbt` does not exist.

**Step 3: Implement the minimal codec**

Create `PlayerStateNbt` with this public surface:

```java
public final class PlayerStateNbt {
    public static final long MAX_PLAYER_NBT_BYTES = 16L * 1024 * 1024;

    public static CompoundTag decodeValidated(byte[] gzipNbt) throws IOException;

    public static byte[] encode(CompoundTag tag) throws IOException;
}
```

`decodeValidated` must:

- Reject null/empty input.
- Parse with `NbtIo.readCompressed` and `NbtAccounter.create(MAX_PLAYER_NBT_BYTES)`.
- Require a non-blank string `Dimension` using `Tag.TAG_STRING`.
- Require a `Tag.TAG_LIST` named `Pos`, exactly three `Tag.TAG_DOUBLE` entries.
- Reject NaN and infinite coordinates with `Double.isFinite`.
- Wrap parse/type failures in an `IOException` whose message identifies the invalid field but never prints NBT contents.

`encode` must reject null and return canonical gzip NBT bytes using `NbtIo.writeCompressed`.

**Step 4: Verify the codec test is GREEN**

Run the targeted test again. Expected: all `PlayerStateNbtTest` cases pass.

**Step 5: Write the failing local-file regression tests**

Create `PlayerStateFilesTest` using `@TempDir`. Reproduce the field evidence from the real run:

```java
@Test
void authoritativeStateReplacesPoisonedPlayerFileAndRemovesEmbeddedPlayer() throws Exception {
    Path save = tempDir.resolve("world");
    Files.createDirectories(save.resolve("playerdata"));
    UUID playerId = UUID.fromString("9207ce8d-2b04-4fd8-a9f7-a437ce1211a0");

    writeLevel(save.resolve("level.dat"), playerAt(0.0, 0.0, 0.0), 1234L);
    writeLevel(save.resolve("level.dat_old"), new CompoundTag(), 1234L);
    NbtIo.writeCompressed(playerAt(0.0, 0.0, 0.0),
            save.resolve("playerdata").resolve(playerId + ".dat"));

    PlayerStateFiles.installAuthoritative(save, playerId,
            playerAt(-107.0, 116.0, 10.0));

    CompoundTag installed = read(save.resolve("playerdata").resolve(playerId + ".dat"));
    assertEquals(-107.0, installed.getList("Pos", 6).getDouble(0));
    assertFalse(read(save.resolve("level.dat")).getCompound("Data").contains("Player"));
    assertFalse(read(save.resolve("level.dat_old")).getCompound("Data").contains("Player"));
    assertEquals(1234L, read(save.resolve("level.dat")).getCompound("Data").getLong("DayTime"));
}
```

Add a second test proving malformed/missing `level.dat` throws before replacing an existing player file.

**Step 6: Run the file test to verify RED**

Run:

```powershell
.\gradlew.bat test --tests imsng.player_to_player.group.PlayerStateFilesTest
```

Expected: compilation fails because `PlayerStateFiles` does not exist.

**Step 7: Implement atomic local installation**

Create `PlayerStateFiles` with:

```java
public final class PlayerStateFiles {
    public static void installAuthoritative(
            Path saveDir, UUID playerId, CompoundTag playerState) throws IOException;
}
```

Implementation order:

1. Read and validate `level.dat`; read `level.dat_old` only when present.
2. Require root `Data` compounds before touching destination files.
3. Remove `Player` from both in-memory `Data` compounds.
4. Write the player state and rewritten level files to same-directory `.p2p-tmp` paths.
5. Replace destinations using `ATOMIC_MOVE`; catch `AtomicMoveNotSupportedException` and retry with `REPLACE_EXISTING` only.
6. Delete remaining temp files in `finally`.

Prepare every temporary file before moving the first destination so parse/write failures leave the existing save unchanged.

**Step 8: Verify Task 1 tests**

Run:

```powershell
.\gradlew.bat test --tests "imsng.player_to_player.group.PlayerState*Test"
```

Expected: all tests pass.

**Step 9: Commit Task 1**

```powershell
git add src/main/java/imsng/player_to_player/group/PlayerStateNbt.java src/main/java/imsng/player_to_player/group/PlayerStateFiles.java src/test/java/imsng/player_to_player/group/PlayerStateNbtTest.java src/test/java/imsng/player_to_player/group/PlayerStateFilesTest.java
git commit -m "fix: install authoritative primary player state"
```

### Task 2: Fetch a validated physical-server snapshot before role assignment

**Files:**
- Create: `src/main/java/imsng/player_to_player/group/PlayerDataClient.java`
- Create: `src/test/java/imsng/player_to_player/group/PlayerDataClientTest.java`
- Modify: `src/main/java/imsng/player_to_player/server/PlayerDataHandlers.java:110`

**Step 1: Write the failing control-protocol tests**

Use a small fake `ControlConnection` whose `request` returns a completed future. Cover:

- A valid `PLAYER_DATA` response returns a validated `CompoundTag`.
- `exists=false`, `ERROR`, empty binary, invalid NBT, and timeout all throw and never return stale state.
- The request JSON contains the expected `playerUuid` and uses `MessageType.PLAYER_DATA_REQUEST`.

Target API:

```java
CompoundTag state = PlayerDataClient.requestAuthoritative(connection, playerId);
```

**Step 2: Run the test to verify RED**

```powershell
.\gradlew.bat test --tests imsng.player_to_player.group.PlayerDataClientTest
```

Expected: compilation fails because `PlayerDataClient` does not exist.

**Step 3: Implement `PlayerDataClient`**

The method must build the existing request, wait only on the caller's IO thread with `Protocol.REQUEST_TIMEOUT_MILLIS`, verify `PLAYER_DATA`, `exists=true`, and non-empty binary, then delegate to `PlayerStateNbt.decodeValidated`.

Do not catch and downgrade failures in this helper; callers need an exception to enforce fail-closed switching.

**Step 4: Verify the client helper test is GREEN**

Run the targeted test. Expected: all cases pass.

**Step 5: Change the server response to prefer the live player**

In `PlayerDataHandlers.handleRequest`:

```java
server.execute(() -> {
    ServerPlayer online = server.getPlayerList().getPlayer(playerId);
    if (online != null) {
        try {
            sendPlayerData(conn, msg, playerId,
                    PlayerStateNbt.encode(online.saveWithoutId(new CompoundTag())), "online");
        } catch (Exception e) {
            sendReadError(conn, msg, playerId, e);
        }
        return;
    }
    ThreadPools.io().execute(() -> sendDiskPlayerData(conn, msg, playerId));
});
```

Keep entity serialization on the server thread. Keep disk IO on `ThreadPools.io()`. Extract response construction so online and disk paths return identical JSON. Update the class Javadoc to document “online snapshot first, disk fallback”.

**Step 6: Run focused and full tests**

```powershell
.\gradlew.bat test --tests imsng.player_to_player.group.PlayerDataClientTest
.\gradlew.bat test
```

Expected: both commands pass.

**Step 7: Commit Task 2**

```powershell
git add src/main/java/imsng/player_to_player/group/PlayerDataClient.java src/main/java/imsng/player_to_player/server/PlayerDataHandlers.java src/test/java/imsng/player_to_player/group/PlayerDataClientTest.java
git commit -m "fix: snapshot live player data before role switch"
```

### Task 3: Prepare the local world before `ROLE_REQUEST`

**Files:**
- Modify: `src/client/java/imsng/player_to_player/client/group/LocalWorldLauncher.java:54`
- Modify: `src/client/java/imsng/player_to_player/client/session/WorldSession.java:429`

**Step 1: Split local preparation from launch**

Add a value record and two methods to `LocalWorldLauncher`:

```java
public record PreparedWorld(Path savesRoot, String saveName) {
}

public static PreparedWorld prepare(
        Path worldFolder, String worldName, UUID playerId, CompoundTag playerState)
        throws IOException;

public static void launchPrepared(
        Minecraft minecraft, ControlConnection conn, PreparedWorld world, UUID groupId);
```

`prepare` must run on the IO thread, call the existing skeleton copy, then call `PlayerStateFiles.installAuthoritative`. `launchPrepared` must perform no disk IO; it only arms `GroupRuntime` and schedules `WorldSwitcher.switchToLocalWorld`.

Update comments that currently say existing local player state has priority; physical-server state now has priority at every primary startup.

**Step 2: Replace the best-effort request in `WorldSession`**

Remove `pullOwnPlayerData`. Add:

```java
private static LocalWorldLauncher.PreparedWorld prepareAuthoritativeWorld(
        ControlConnection conn, Path worldFolder, String worldName) throws Exception {
    NodeContext ctx = NodeContext.get();
    CompoundTag state = PlayerDataClient.requestAuthoritative(conn, ctx.clientId());
    return LocalWorldLauncher.prepare(
            worldFolder, worldName, ctx.clientId(), state);
}
```

**Step 3: Enforce the initial fail-closed order**

At the start of `requestRoleAndSwitch`:

1. Fetch and validate the physical-server snapshot.
2. Prepare and sanitize the local primary save.
3. Recheck `myGeneration`.
4. Only then send `ROLE_REQUEST`.

For `primary`, set role/group and call `launchPrepared`. For `secondary`, keep the prepared cache and use the existing `SecondaryJoiner` path.

On snapshot or file failure:

- Do not send `ROLE_REQUEST`.
- Leave role `UNASSIGNED` and group null.
- Log the failing stage.
- Schedule a client-thread chat message:

```text
无法取得或准备物理服务器玩家状态，已取消主客户端切换，请稍后重新进入服务器
```

**Step 4: Reuse strict preparation for unsolicited promotion**

In `handleUnsolicitedRoleAssign`, do not set `PRIMARY` before preparation. Fetch, validate, and prepare first; only then update `NodeContext` and launch. On failure, leave the current world connected and log that the promotion could not be applied. Do not silently use stale local files.

**Step 5: Compile and run all tests**

```powershell
.\gradlew.bat test
.\gradlew.bat build
```

Expected: compilation and tests pass. No protocol version or resource metadata changes appear in the diff.

**Step 6: Commit Task 3**

```powershell
git add src/client/java/imsng/player_to_player/client/group/LocalWorldLauncher.java src/client/java/imsng/player_to_player/client/session/WorldSession.java
git commit -m "fix: prepare primary state before role assignment"
```

### Task 4: Publish LAN once the local client is actually ready

**Files:**
- Create: `src/main/java/imsng/player_to_player/group/GroupPublicationGate.java`
- Create: `src/test/java/imsng/player_to_player/group/GroupPublicationGateTest.java`
- Modify: `src/client/java/imsng/player_to_player/client/group/GroupHost.java:66`
- Modify: `src/client/java/imsng/player_to_player/client/boot/ClientBootstrap.java:109`
- Modify: `src/client/java/imsng/player_to_player/client/session/WorldSession.java:148`

**Step 1: Write the failing one-shot gate tests**

Target API:

```java
GroupPublicationGate<Object> gate = new GroupPublicationGate<>();
gate.arm(server);

assertNull(gate.takeIfReady(server, false, true, ignored -> true));
assertSame(server, gate.takeIfReady(server, true, true, ignored -> true));
assertNull(gate.takeIfReady(server, true, true, ignored -> true));
```

Also test stale current-server identity, inactive runtime, and `reset()`.

**Step 2: Run the test to verify RED**

```powershell
.\gradlew.bat test --tests imsng.player_to_player.group.GroupPublicationGateTest
```

Expected: compilation fails because `GroupPublicationGate` does not exist.

**Step 3: Implement the minimal thread-safe gate**

```java
public final class GroupPublicationGate<T> {
    private volatile T pending;

    public void arm(T value);

    public T takeIfReady(
            T current, boolean playerReady, boolean connectionReady, Predicate<T> active);

    public void reset();
}
```

Use identity comparison (`current == pending`) because an integrated server instance is a lifecycle identity. Synchronize the final check-and-clear so publication is consumed exactly once.

**Step 4: Verify the gate test is GREEN**

Run the targeted test. Expected: all cases pass.

**Step 5: Wire condition-based publication**

Change `GroupHost.start` to reset `lanPort`, arm the pending integrated server, and register the existing P2P listener without calling `publishServer`.

Add `GroupHost.onClientTick(Minecraft client)`:

```java
IntegratedServer ready = PUBLICATION_GATE.takeIfReady(
        client.getSingleplayerServer(),
        client.player != null,
        client.getConnection() != null,
        GroupRuntime::isManagedServer);
if (ready == null) {
    return;
}
publish(ready);
```

`publish` must run on the client thread, publish only when not already published, record `ready.getPort()`, and send `GROUP_WORLD_READY`. If another path already partially published the server, adopt its existing port instead of opening a second listener.

Add `GroupHost.reset()` to clear the gate and LAN port. Call it from `WorldSession.teardownSession` and before arming a new host.

Register the tick callback once in `ClientBootstrap`:

```java
ClientTickEvents.END_CLIENT_TICK.register(GroupHost::onClientTick);
```

Do not use sleeps or scheduled fixed delays.

**Step 6: Run tests and build**

```powershell
.\gradlew.bat test --tests imsng.player_to_player.group.GroupPublicationGateTest
.\gradlew.bat test
.\gradlew.bat build
```

Expected: all commands pass.

**Step 7: Commit Task 4**

```powershell
git add src/main/java/imsng/player_to_player/group/GroupPublicationGate.java src/test/java/imsng/player_to_player/group/GroupPublicationGateTest.java src/client/java/imsng/player_to_player/client/group/GroupHost.java src/client/java/imsng/player_to_player/client/boot/ClientBootstrap.java src/client/java/imsng/player_to_player/client/session/WorldSession.java
git commit -m "fix: defer LAN publication until client join"
```

### Task 5: Final verification and real client/server smoke test

**Files:**
- Modify only if verification reveals an issue; do not add unrelated refactors.

**Step 1: Run clean automated verification**

```powershell
.\gradlew.bat test
.\gradlew.bat build
git diff --check
git status --short
```

Expected: tests and build pass, `git diff --check` is empty, and status contains only intentional work.

**Step 2: Inspect compatibility-sensitive files**

```powershell
git diff 61ab182..HEAD -- src/main/java/imsng/player_to_player/netproto/Protocol.java src/main/java/imsng/player_to_player/netproto/MessageType.java src/main/resources/fabric.mod.json src/main/resources/player_to_player.mixins.json
```

Expected: no changes.

**Step 3: Run the physical-server-to-primary smoke scenario**

Use the same physical server and client roles as the reported test:

1. Record physical-server dimension, coordinates, health, selected hotbar item, and one distinctive inventory item.
2. Join and wait for primary assignment.
3. Confirm the client remains on the physical server until player-state preparation and role assignment complete.
4. After local-world switch, confirm all recorded state matches exactly.
5. Confirm the local log contains the authoritative-state installation and successful LAN port record.
6. Confirm neither log contains `Not a string`, `Error executing task on Client`, or the `Minecraft.player` NPE.

**Step 4: Run the fail-closed scenario**

Temporarily make `PLAYER_DATA_REQUEST` fail in a controlled development setup, or stop the control endpoint after environment sync but before the request. Confirm:

- The client does not send/complete `ROLE_REQUEST`.
- The physical server does not create a group.
- The player remains in the physical world.
- The retry message is visible.
- No local player or level file is partially replaced when validation fails.

Restore the test setup immediately afterward; do not commit runtime configuration or endpoints.

**Step 5: Review the final commits**

Use `superpowers:requesting-code-review`, address any concrete findings, rerun Task 5 Step 1, then use `superpowers:verification-before-completion` before reporting success.
