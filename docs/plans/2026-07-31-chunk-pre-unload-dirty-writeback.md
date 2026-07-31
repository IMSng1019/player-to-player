# Chunk Pre-Unload Dirty Writeback Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.
>
> **仓库约束：** 按 `superpowers:test-driven-development` 执行；直接修改当前 `main`，不建 worktree、不 commit。

**Goal:** 让区块在物理服确认最终脏数据并释放所有权前绝不本地卸载，并让正常退出只传输未确认脏区块。

**Architecture:** 使用 `ChunkHolder` save dependency 作为不可绕过的预卸载屏障；客户端分别跟踪服务端数据基线、最新未确认快照和预卸载 future。退出事务保留完整 ownership manifest，但只暂存和应用显式声明的 written chunks，干净区块隐式保留服务端现状。

**Tech Stack:** Java 17、Fabric/Mixin 1.20.4、Minecraft ChunkMap/ChunkHolder/NBT、Netty 控制协议、JUnit 5、Gradle Wrapper。

---

### Task 1: 让实时 ACK 精确维护未确认脏快照

**Files:**
- Modify: `src/test/java/imsng/player_to_player/group/ChunkUploadServiceTest.java`
- Modify: `src/main/java/imsng/player_to_player/group/ChunkUploadService.java`

1. 新增失败测试：当前版本收到 `written=true` 后 `latestSnapshot(key)` 必须为 null。
2. 新增失败测试：发送旧版本期间同键入队新版本，旧 ACK 后必须仍保留新版本。
3. 新增失败测试：异常或负 ACK 始终保留最新版本。
4. 运行：`.\gradlew.bat test --tests "imsng.player_to_player.group.ChunkUploadServiceTest"`，确认因 ACK 后仍保留 `latestSnapshots` 失败。
5. 让成功路径在同一锁内只删除仍与已发送对象相同的 `queue` 和 `latestSnapshots` 项；新增
   `hasUnacknowledged`、`latestUnacknowledged`、`forget` 和退出冻结 API。
6. 重跑定向测试，确认通过。

### Task 2: 跟踪服务端数据基线并耐久确认区块释放

**Files:**
- Create: `src/test/java/imsng/player_to_player/group/ChunkClaimClientReleaseTest.java`
- Modify: `src/test/java/imsng/player_to_player/group/ChunkClaimClientExitTest.java`
- Modify: `src/main/java/imsng/player_to_player/group/ChunkClaimClient.java`

1. 编写失败测试：claim 的 `hasServerData=false` 会使 `requiresInitialWrite(key)` 为 true；调用
   `markServerDataKnown` 后为 false。
2. 编写失败测试：`releaseWhenConfirmed` 在请求异常、错误类型、`released=false` 时不移除 claimed，
   并按注入的 retry scheduler 重试。
3. 编写失败测试：携带数据时必须同时验证 `finalDataWritten=true`；只有正 ACK 才移除 claimed。
4. 编写失败测试：`beginExitPreparation` 拒绝新 claim 但允许在途 release；`freezeForExit` 才禁止普通 release。
5. 运行新测试并确认所需 API 缺失导致红灯。
6. 用 `ConcurrentHashMap<ChunkKey, Boolean>` 保存服务端数据基线；所有 forget/shutdown/成功 release
   同步清理。
7. 将普通释放改为返回 `CompletableFuture<Void>` 的无限确认重试，不在请求前摘除 claimed。
8. 为同一释放的所有重试生成稳定 `releaseId`；服务端用单条 WAL 原子记录 ownership 删除与
   成功回执，重启后仍可重放原 ACK。客户端收到正 ACK 后立即开放卸载，后台确认回收回执。
9. 重跑两个 ChunkClaimClient 测试类，并覆盖 ACK 丢失、跨重启重放、最终数据仅写一次。

### Task 3: 建立可测试的预卸载状态机

**Files:**
- Create: `src/main/java/imsng/player_to_player/group/ChunkPreUnloadService.java`
- Create: `src/test/java/imsng/player_to_player/group/ChunkPreUnloadServiceTest.java`

1. 先测试同键重复 begin 返回同一未决 future，不能发送两次 release。
2. 测试决策矩阵：`isUnsaved || unacknowledged || requiresInitialWrite` 任一成立就生成最终数据；
   三者都为 false 时 release payload 为 null。
3. 测试 release future 未完成时屏障未完成；release 成功后调用上传状态 `forget` 并完成屏障。
4. 测试 `beginExitPreparation` 后可等待所有现有屏障 quiescent，退出冻结后不创建新屏障。
5. 运行测试确认类/API 不存在而失败。
6. 实现按 ChunkKey 合并的 future 表、主线程快照回调、后台压缩/释放以及成功清理；所有失败由
   ChunkClaimClient 内部持续重试，屏障本身不降级成功。
7. 重跑测试。

### Task 4: 把屏障接入原版 ChunkHolder 卸载依赖

**Files:**
- Create: `src/main/java/imsng/player_to_player/mixin/ChunkHolderSaveDependencyAccess.java`
- Modify: `src/main/java/imsng/player_to_player/mixin/ChunkMapMixin.java`
- Modify: `src/main/resources/player_to_player.mixins.json`
- Modify: `src/main/java/imsng/player_to_player/group/GroupRuntime.java`
- Modify: `src/main/java/imsng/player_to_player/group/GroupServerHooks.java`

1. 在 accessor 中用 `@Invoker("addSaveDependency")` 暴露受保护的 save dependency API。
2. 在 `ChunkMap.scheduleUnload(long, ChunkHolder)` HEAD 注入：仅托管维度取得原始
   `holder.getChunkToSave()`，创建/复用 P2P 屏障并添加为 save dependency；不取消原方法。
3. GroupRuntime attach/detach 创建和销毁 `ChunkPreUnloadService`，提供 Mixin 使用的静态入口。
4. 将 Fabric `CHUNK_UNLOAD` 处理器降为防御性兜底；正常路径因所有权已释放必须直接返回，不能再次发送。
5. 运行 `.\gradlew.bat compileJava`，确认 Mixin 目标和 invoker 签名可解析。
6. 运行全部 group 测试。

### Task 5: 让全局退出只收集最终未确认区块

**Files:**
- Modify: `src/main/java/imsng/player_to_player/group/GroupRuntime.java`
- Modify: `src/main/java/imsng/player_to_player/group/GroupServerHooks.java`
- Modify: `src/main/java/imsng/player_to_player/group/ExitBundle.java`
- Modify: `src/test/java/imsng/player_to_player/group/ExitBundleTest.java`
- Modify: `src/main/java/imsng/player_to_player/group/ExitBundlePublication.java`（仅在 API 变化需要时）

1. 修改 ExitBundle 测试：ownership manifest 可以包含干净区块，bundle 只要求 written 是 manifest
   子集，不再要求 `written ∪ preserve == manifest`。
2. 新增测试：退出等待预卸载 quiescent 后才冻结 claims；该协调逻辑尽量落在纯 Java service 中测试。
3. 运行测试确认旧“每个区块必须二选一”不变量导致失败。
4. `beginExitDrain` 先进入 exit preparation，异步等待预卸载完成，再在服务器线程固定 manifest。
5. 冻结时停止普通实时发送但继续记录停服 save 的脏快照。
6. 删除 `captureExitStopping` 对全部 manifest chunks 的无条件扫描；detach 只从
   `latestUnacknowledged` 构建 written map。
7. 简化 ExitBundle 为 players + written chunks；干净区块由 manifest 隐式表示。
8. 重跑 ExitBundle、ChunkUpload、ChunkClaim、预卸载测试。

### Task 6: 改造退出客户端为“只传 written，commit 声明计划”

**Files:**
- Modify: `src/test/java/imsng/player_to_player/group/ExitTransactionClientTest.java`
- Modify: `src/main/java/imsng/player_to_player/group/ExitTransactionClient.java`
- Modify: `src/main/java/imsng/player_to_player/netproto/Protocol.java`

1. 新增失败测试：manifest 有大量干净区块而 written 为空时，客户端不得发送任何
   `EXIT_CHUNK_DATA`，只发送玩家数据和 `EXIT_COMMIT`。
2. 新增失败测试：commit JSON 的 `writtenChunks` 精确等于实际上传集合且稳定排序。
3. 保留连接中断后从 `EXIT_BEGIN` 重放玩家和 written chunks 的测试。
4. 运行定向测试并确认当前 preserve 循环导致失败。
5. 删除 preserve 循环；commit 附带 written chunk key 数组。
6. 将 `Protocol.VERSION` 从 3 提升到 4，并添加详细兼容性中文注释。
7. 重跑 ExitTransactionClientTest。

### Task 7: 服务端按声明的 written 集合提交并耐久恢复

**Files:**
- Modify: `src/test/java/imsng/player_to_player/server/ExitTransactionLedgerTest.java`
- Modify: `src/test/java/imsng/player_to_player/server/ExitTransactionStoreTest.java`
- Modify: `src/test/java/imsng/player_to_player/server/ExitTransactionHandlersTest.java`
- Modify: `src/main/java/imsng/player_to_player/server/ExitTransactionRecord.java`
- Modify: `src/main/java/imsng/player_to_player/server/ExitTransactionStore.java`
- Modify: `src/main/java/imsng/player_to_player/server/ExitTransactionLedger.java`
- Modify: `src/main/java/imsng/player_to_player/server/ExitTransactionHandlers.java`

1. 测试 commit 声明集合必须是 manifest 子集，且所有声明项已收到；未声明的干净区块不阻止提交。
2. 测试收到但未在 commit 声明中的区块、声明但未收到的区块均拒绝提交。
3. 测试 committed 记录保存并恢复精确 written 集合；pending/abandoned 状态保持现有语义。
4. 测试旧 JSON 缺少 written 字段时标记为 legacy disposition，仍可恢复旧 data/marker 暂存。
5. 运行三个测试类确认旧 ledger 要求全部 manifest chunks 而失败。
6. 在 RuntimeTransaction/ExitTransactionRecord 中持久化 written 集合及是否显式声明；所有
   markDataApplied/markCleanupComplete 状态转换保留该计划。
7. EXIT_COMMIT 解析、校验 writtenChunks，原子提交后再应用。
8. 重跑测试。

### Task 8: 移除新事务的 preserve marker 与逐项干净读盘

**Files:**
- Modify: `src/test/java/imsng/player_to_player/server/ExitPayloadStoreTest.java`
- Modify: `src/main/java/imsng/player_to_player/server/ExitPayloadStore.java`
- Modify: `src/main/java/imsng/player_to_player/server/ExitTransactionHandlers.java`
- Modify: `src/main/java/imsng/player_to_player/server/ChunkDataHandlers.java`
- Modify: `src/main/java/imsng/player_to_player/server/P2PServerService.java`

1. 新增失败测试：新事务 apply 只调用 written chunks 的 validate/write，manifest 中干净区块零调用。
2. 新增测试：written 暂存缺失必须失败；一次 apply 完成后只 flush 一次。
3. 保留测试：legacy committed 事务仍识别 data 与 preserve marker 并完成恢复。
4. 运行 ExitPayloadStoreTest 确认当前逐 manifest 扫描失败。
5. 新事务 stageChunk 禁止 `preserveExisting=true`；apply 接收持久化计划，只读取和写入 written 文件。
6. legacy 分支保留旧 marker 读取逻辑；新路径不调用 `existingChunkIsReadable`。
7. 重跑 ExitPayloadStore、Handlers、Ledger 测试。

### Task 9: 文档、日志与完整验证

**Files:**
- Modify: `docs/DESIGN.md`
- Modify: `src/main/java/imsng/player_to_player/group/GroupRuntime.java`
- Modify: `src/main/java/imsng/player_to_player/client/session/ClientExitCoordinator.java`（如需统计日志）

1. 更新 DESIGN.md：预卸载屏障、数据基线、脏/未确认语义、增量退出和协议 v4。
2. 增加退出统计日志：`ownedChunks`、`writtenChunks`、压缩总字节；增加预卸载重试与成功日志，
   不在每次轮询刷屏。
3. 运行所有新增/修改的定向测试，确认每个红灯都经历了 red-green。
4. 运行：`.\gradlew.bat test --no-daemon`。
5. 运行：`.\gradlew.bat build --no-daemon`。
6. 运行：`git diff --check`、`git status --short`、`git diff --stat`。
7. 检查 remap JAR 包含新增 Mixin，且 mixin JSON 名称正确。
8. 不 commit。报告兼容性敏感变化：`Protocol.VERSION 3 → 4`、
   `EXIT_COMMIT.writtenChunks`、`CHUNK_RELEASE.releaseId`、消息类型 29。
9. 实机待验证场景：移动触发 clean/dirty 卸载、世界生成 ProtoChunk 回收、卸载中断网后恢复、
   退出前在途预卸载、退出事务中服务端重启、2400 ownership 下 written 很少时的耗时。
