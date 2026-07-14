# Versioned Environment Snapshots Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Make every environment manifest and file download use the same immutable, versioned snapshot while rebuilding snapshots in near real time after source changes.

**Architecture:** Add a content-addressed snapshot repository under `player-to-player/environment-snapshots`, publish immutable `EnvironmentSnapshot` objects atomically, and require `snapshotId` on every environment file request. A manager uses recursive `WatchService` events with one-second debounce plus a 60-second reconciliation scan, retaining older snapshots long enough for in-flight downloads.

**Tech Stack:** Java 17, Fabric Loader 1.20.4, Gradle/Loom, Java NIO, Gson, JUnit Jupiter 5.

---

## Constraints

- Preserve the current environment inclusion and exclusion semantics.
- Keep all blocking file work off Minecraft and Netty threads.
- Add detailed Chinese comments to new production code.
- Raise `Protocol.VERSION` from 1 to 2 and treat the change as compatibility-sensitive.
- Do not run `git add`, `git commit`, push, or create a PR; the user handles commits.

### Task 1: Add Executable Test Support

**Files:**
- Modify: `build.gradle`
- Create: `src/test/java/imsng/player_to_player/env/EnvironmentManifestTest.java`

**Step 1: Add a failing smoke test**

Create a JUnit Jupiter test which builds two manifests with the same entries in different insertion orders and asserts that their global hashes match. This verifies the test source set can load production classes.

```java
@Test
void globalHashIsIndependentOfInsertionOrder() {
    Map<String, EnvironmentManifest.Entry> first = new LinkedHashMap<>();
    first.put("b.txt", new EnvironmentManifest.Entry("bb", 2));
    first.put("a.txt", new EnvironmentManifest.Entry("aa", 1));

    Map<String, EnvironmentManifest.Entry> second = new LinkedHashMap<>();
    second.put("a.txt", new EnvironmentManifest.Entry("aa", 1));
    second.put("b.txt", new EnvironmentManifest.Entry("bb", 2));

    assertEquals(new EnvironmentManifest(first).globalHash(),
            new EnvironmentManifest(second).globalHash());
}
```

**Step 2: Run the test and verify the missing JUnit configuration**

Run: `.\gradlew.bat test --tests imsng.player_to_player.env.EnvironmentManifestTest`

Expected before configuration: test compilation fails because JUnit Jupiter is unavailable or the platform is not enabled.

**Step 3: Configure JUnit Jupiter**

Add:

```groovy
dependencies {
    testImplementation platform("org.junit:junit-bom:5.10.2")
    testImplementation "org.junit.jupiter:junit-jupiter"
}

test {
    useJUnitPlatform()
}
```

**Step 4: Run the smoke test**

Run: `.\gradlew.bat test --tests imsng.player_to_player.env.EnvironmentManifestTest`

Expected: PASS.

**Step 5: User commit checkpoint**

Report Task 1 files and test result; do not commit.

### Task 2: Extract Reusable Environment Path Rules

**Files:**
- Create: `src/main/java/imsng/player_to_player/env/EnvironmentPathPolicy.java`
- Modify: `src/main/java/imsng/player_to_player/env/EnvironmentScanner.java`
- Test: `src/test/java/imsng/player_to_player/env/EnvironmentPathPolicyTest.java`

**Step 1: Write failing path-policy tests**

Cover:

- Built-in exclusions such as `logs/latest.log`, `player-to-player/config.json`, and `world/region/r.0.0.mca`.
- Included files such as `mods/example.jar` and `world/level.dat`.
- Case-insensitive configured exclusions on Windows-style paths.
- `.tmp` files.

The API should be:

```java
EnvironmentPathPolicy policy = EnvironmentPathPolicy.create(extraExclusions);
assertTrue(policy.includes("world/level.dat"));
assertFalse(policy.includes("world/region/r.0.0.mca"));
```

**Step 2: Run tests and verify failure**

Run: `.\gradlew.bat test --tests imsng.player_to_player.env.EnvironmentPathPolicyTest`

Expected: FAIL because `EnvironmentPathPolicy` does not exist.

**Step 3: Implement the policy**

Move normalization, built-in exclusions, configured exclusions, and `.tmp` handling from `EnvironmentScanner` into an immutable policy object. Expose:

```java
public static EnvironmentPathPolicy create(List<String> extraExclusions);
public boolean includes(String relativePath);
public String normalize(String path);
```

Keep path comparisons case-insensitive while preserving original case for manifest keys.

**Step 4: Refactor the scanner**

Make `EnvironmentScanner.scan` create one policy and call `policy.includes(relative)` without changing scan output.

**Step 5: Run focused tests**

Run: `.\gradlew.bat test --tests 'imsng.player_to_player.env.Environment*Test'`

Expected: PASS.

**Step 6: User commit checkpoint**

Report the refactor and tests; do not commit.

### Task 3: Build the Content-Addressed Snapshot Repository

**Files:**
- Modify: `src/main/java/imsng/player_to_player/config/P2PPaths.java`
- Create: `src/main/java/imsng/player_to_player/env/EnvironmentSnapshot.java`
- Create: `src/main/java/imsng/player_to_player/env/EnvironmentSnapshotStore.java`
- Test: `src/test/java/imsng/player_to_player/env/EnvironmentSnapshotStoreTest.java`

**Step 1: Write the mutation regression test**

Use `@TempDir` to create a source directory and repository. Build snapshot A from `world/level.dat = "old"`, mutate the live file to `"new-content"`, then read A through the store and assert the bytes remain `"old"`.

Also build snapshot B and assert:

- A and B have different snapshot IDs.
- A still serves old bytes.
- B serves new bytes.
- An unchanged large file maps to the same Blob path in both snapshots.

**Step 2: Run and verify failure**

Run: `.\gradlew.bat test --tests imsng.player_to_player.env.EnvironmentSnapshotStoreTest`

Expected: FAIL because snapshot classes do not exist.

**Step 3: Add snapshot paths**

Add to `P2PPaths`:

```java
public Path environmentSnapshotsDir();
public Path environmentSnapshotBlobsDir();
public Path environmentSnapshotManifestsDir();
public Path environmentSnapshotStagingDir();
```

**Step 4: Implement `EnvironmentSnapshot`**

Use an immutable class containing `snapshotId`, `EnvironmentManifest`, creation time, and an atomic last-access timestamp. `snapshotId` equals the complete manifest global hash.

**Step 5: Implement stable Blob materialization**

For every included source file:

1. Read `BasicFileAttributes` before copying.
2. Copy to a staging file while calculating SHA-256 and byte count.
3. Read attributes again.
4. Retry up to three times if size, modification time, or file key changed.
5. Atomically move the staging file to `blobs/<sha256>`, or reuse an existing Blob.

Build the manifest exclusively from the staged Blob result, never from source metadata captured separately.

**Step 6: Persist and load manifests atomically**

Store snapshot JSON in `manifests/<snapshotId>.json`. On startup, load valid manifests whose referenced Blobs exist. Ignore malformed or incomplete manifests with a warning.

**Step 7: Run snapshot tests**

Run: `.\gradlew.bat test --tests imsng.player_to_player.env.EnvironmentSnapshotStoreTest`

Expected: PASS.

**Step 8: User commit checkpoint**

Report Task 3 files and tests; do not commit.

### Task 4: Add Real-Time Snapshot Management and Retention

**Files:**
- Modify: `src/main/java/imsng/player_to_player/config/GlobalConfig.java`
- Create: `src/main/java/imsng/player_to_player/env/EnvironmentSnapshotManager.java`
- Test: `src/test/java/imsng/player_to_player/env/EnvironmentSnapshotManagerTest.java`

**Step 1: Write failing manager tests**

Use injectable debounce/reconciliation durations in a package-private constructor so tests do not sleep for production intervals. Cover:

- Initial snapshot publication.
- Multiple rapid file events causing one rebuild.
- A change during a running build causing a follow-up build.
- Reconciliation detecting a change even when no watch event is delivered.
- Failed rebuild retaining the previous current snapshot.
- Old snapshots retained while recently accessed and removed after expiry.

**Step 2: Run and verify failure**

Run: `.\gradlew.bat test --tests imsng.player_to_player.env.EnvironmentSnapshotManagerTest`

Expected: FAIL because the manager does not exist.

**Step 3: Add retention configuration**

Add:

```java
public int envSnapshotRetentionMinutes = 120;
```

Clamp values below 10 minutes to 10 in `sanitize()` and document the disk/slow-client trade-off in Chinese.

**Step 4: Implement single-flight publication**

The manager must expose:

```java
public void start();
public EnvironmentSnapshot current();
public EnvironmentSnapshot find(String snapshotId);
public void requestRefresh();
public void stop();
```

Use one dirty flag and one building flag. Publish with a volatile reference only after store construction completes. If the built snapshot ID equals current, update the source fingerprint without publishing a duplicate.

**Step 5: Implement recursive watching**

Register all included directories with `WatchService`; register new directories on create. Ignore excluded paths. Handle `OVERFLOW` by requesting a full refresh. Watch callbacks only mark dirty and schedule the one-second debounce.

**Step 6: Implement 60-second reconciliation**

Calculate a lightweight deterministic fingerprint from included relative paths, sizes, and last-modified timestamps. If it differs from the last published source fingerprint, request a refresh.

**Step 7: Implement cleanup**

Keep current snapshot unconditionally. Remove non-current manifests after `envSnapshotRetentionMinutes` since last access, then delete unreferenced Blobs. Clean abandoned staging files during startup and periodic cleanup.

**Step 8: Run manager tests**

Run: `.\gradlew.bat test --tests imsng.player_to_player.env.EnvironmentSnapshotManagerTest`

Expected: PASS.

**Step 9: User commit checkpoint**

Report Task 4 files and tests; do not commit.

### Task 5: Bind the Environment Protocol to Snapshot IDs

**Files:**
- Modify: `src/main/java/imsng/player_to_player/netproto/Protocol.java`
- Modify: `src/main/java/imsng/player_to_player/env/EnvSyncServerHandlers.java`
- Modify: `src/main/java/imsng/player_to_player/env/EnvSyncClient.java`
- Test: `src/test/java/imsng/player_to_player/env/EnvSyncSnapshotProtocolTest.java`

**Step 1: Write failing protocol tests**

Create a small fake `ControlConnection` that routes requests to registered handlers. Cover:

- Manifest response includes full `snapshotId` even when target filtering changes `filteredHash`.
- Every file request must include `snapshotId`.
- File response echoes `snapshotId`, path, and offset.
- Updating current snapshot between chunks does not change bytes returned for the old ID.
- Missing snapshot returns `ERROR` with `code=snapshot_not_found`.

**Step 2: Run and verify failure**

Run: `.\gradlew.bat test --tests imsng.player_to_player.env.EnvSyncSnapshotProtocolTest`

Expected: FAIL because the existing protocol has no snapshot binding.

**Step 3: Raise the protocol version**

Change `Protocol.VERSION` from 1 to 2 and update its compatibility-sensitive comment.

**Step 4: Change server handler registration**

Replace `(serverRoot, manifestSupplier)` with an `EnvironmentSnapshotManager` or a narrow snapshot lookup interface. Manifest requests use `manager.current()`. File requests require a nonblank `snapshotId`, resolve that exact snapshot, validate path membership, and read `blobs/<sha256>`.

**Step 5: Change client synchronization**

Capture `snapshotId` from `ENV_MANIFEST`. Pass it through `downloadWithRetry` and `downloadOne`, include it in every chunk request, and validate the echoed value.

Introduce a dedicated internal exception for `snapshot_not_found`. `doSync` catches it and restarts manifest acquisition with a small bounded retry count rather than continuing a partially mixed download.

**Step 6: Run protocol tests**

Run: `.\gradlew.bat test --tests imsng.player_to_player.env.EnvSyncSnapshotProtocolTest`

Expected: PASS.

**Step 7: Run all environment tests**

Run: `.\gradlew.bat test --tests 'imsng.player_to_player.env.*Test'`

Expected: PASS.

**Step 8: User commit checkpoint**

Report the protocol version change explicitly; do not commit.

### Task 6: Integrate Server, HELLO, and Proxy Lifecycles

**Files:**
- Modify: `src/main/java/imsng/player_to_player/server/P2PServerService.java`
- Modify: `src/main/java/imsng/player_to_player/server/HelloHandler.java`
- Modify: `src/main/java/imsng/player_to_player/proxy/ProxyEnvService.java`
- Modify: `src/main/java/imsng/player_to_player/config/P2PPaths.java` if separate proxy snapshot roots are needed
- Test: `src/test/java/imsng/player_to_player/env/EnvironmentSnapshotLifecycleTest.java`

**Step 1: Write failing lifecycle tests**

Cover manager startup/stop idempotence and verify `envReady`/`envHash` are taken from the manager's current snapshot. Verify proxy distribution keeps serving the previous snapshot while its cache directory is being updated.

**Step 2: Run and verify failure**

Run: `.\gradlew.bat test --tests imsng.player_to_player.env.EnvironmentSnapshotLifecycleTest`

Expected: FAIL because services still use a raw manifest supplier.

**Step 3: Integrate the physical server**

In `P2PServerService.start`:

- Build the existing dynamic exclusion list.
- Construct and start `EnvironmentSnapshotManager` rooted at the server game directory.
- Pass it to `HelloHandler` and `EnvSyncServerHandlers`.
- Store it in service state.

In `stop`, stop the manager after closing the network entry and before clearing service state.

**Step 4: Update HELLO metadata**

`HelloHandler` reads `manager.current()`. It sends `envReady=false` when null, otherwise uses the snapshot ID as `envHash`.

**Step 5: Integrate the proxy**

Create a snapshot manager rooted at `proxyEnvDir()`. Start it before exposing environment handlers. Upstream `EnvSyncClient` updates the mutable cache; the manager publishes a new immutable downstream version after debounce/build. Stop the manager with `ProxyEnvService.stop()`.

**Step 6: Run lifecycle tests**

Run: `.\gradlew.bat test --tests imsng.player_to_player.env.EnvironmentSnapshotLifecycleTest`

Expected: PASS.

**Step 7: Run all tests**

Run: `.\gradlew.bat test`

Expected: PASS with zero failed tests.

**Step 8: User commit checkpoint**

Report lifecycle integration and test result; do not commit.

### Task 7: Full Verification and Runtime Evidence

**Files:**
- Modify documentation only if implementation details differ from the approved design.

**Step 1: Run formatting and whitespace checks**

Run: `git diff --check`

Expected: no output, exit code 0.

**Step 2: Run the complete test suite**

Run: `.\gradlew.bat test`

Expected: BUILD SUCCESSFUL and zero failed tests.

**Step 3: Run the production build**

Run: `.\gradlew.bat build`

Expected: BUILD SUCCESSFUL and remapped JAR in `build/libs/`.

**Step 4: Inspect the compatibility-sensitive artifact**

Confirm:

- `Protocol.VERSION == 2`.
- `fabric.mod.json` version expansion remains correct.
- Snapshot classes and tests are included in the expected source sets.
- No generated worlds, runtime configs, IP addresses, or credentials are tracked.

**Step 5: Runtime smoke scenario**

Run a dedicated server and client when the environment is available:

1. Let the server publish snapshot A.
2. Join with a client and begin environment download.
3. Cause `world/level.dat` to update while downloading.
4. Verify the client completes snapshot A without size/hash mismatch.
5. Verify the server publishes snapshot B afterward.
6. Rejoin and verify the client receives B and reaches role assignment/local group-world startup.

If a runtime server cannot be launched in the current environment, report this test as not run rather than inferring success from unit tests.

**Step 6: Final user handoff**

Summarize changed files, tests, protocol migration, residual risks, and the exact runtime scenario still required. Do not commit; the user handles repository history.
