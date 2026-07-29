# Durable Primary Exit Implementation Plan

> **执行约束：** 按 `superpowers:test-driven-development` 实施；本仓库要求不 commit，所有提交步骤省略。

**Goal:** 修复主客户端固定进入 `(0,0,0)`，并实现可跨断线和服务端重启恢复、提交前无限阻止客户端关闭的耐久退出事务。

**Architecture:** 权威玩家 NBT 同时安装到原版单人所有者内嵌状态与 UUID 文件。客户端退出采用“先确认保护意图，再停本地服并重放最终数据，最后原子提交”的两阶段协议；服务端持久化事务元数据并在 pending 期间跳过断线释放。

**Tech Stack:** Java 17、Fabric API 1.20.4、Minecraft NBT/ChunkStorage、Netty 控制协议、JUnit 5、Gradle Wrapper。

---

### Task 1: 修复单人世界所有者玩家状态来源

**Files:**
- Modify: `src/main/java/imsng/player_to_player/group/PlayerStateFiles.java`
- Modify: `src/test/java/imsng/player_to_player/group/PlayerStateFilesTest.java`

1. 修改现有回归测试，要求权威 NBT 同时存在于 UUID 玩家文件、`level.dat/Data.Player` 和
   `level.dat_old/Data.Player`，先运行测试并确认因当前删除行为失败。
2. 修改 `PlayerStateFiles.installAuthoritative`，向两个 level 文件写入玩家 NBT 副本，不再删除。
3. 运行 `PlayerStateFilesTest` 和全部现有玩家状态测试。

### Task 2: 建立纯 Java 的退出事务模型与持久化

**Files:**
- Create: `src/main/java/imsng/player_to_player/group/ExitManifest.java`
- Create: `src/main/java/imsng/player_to_player/group/ExitBundle.java`
- Create: `src/main/java/imsng/player_to_player/server/ExitTransactionLedger.java`
- Create: `src/main/java/imsng/player_to_player/server/ExitTransactionStore.java`
- Create: `src/test/java/imsng/player_to_player/server/ExitTransactionLedgerTest.java`
- Create: `src/test/java/imsng/player_to_player/server/ExitTransactionStoreTest.java`
- Modify: `src/main/java/imsng/player_to_player/config/P2PPaths.java`

1. 测试 pending/committed、重复 begin、冲突 begin、数据接收集合、未齐全不可提交。
2. 测试事务 JSON 原子落盘、pending 和 committed 重启恢复。
3. 实现不可变 manifest/bundle、ledger 和 store；事务文件不包含玩家/区块 NBT，只保存身份、清单和状态。

### Task 3: 新增退出协议与服务端处理器

**Files:**
- Modify: `src/main/java/imsng/player_to_player/netproto/MessageType.java`
- Modify: `src/main/java/imsng/player_to_player/netproto/Protocol.java`
- Create: `src/main/java/imsng/player_to_player/server/ExitTransactionHandlers.java`
- Modify: `src/main/java/imsng/player_to_player/server/ChunkDataHandlers.java`
- Modify: `src/main/java/imsng/player_to_player/server/PlayerDataHandlers.java`
- Modify: `src/main/java/imsng/player_to_player/server/P2PServerService.java`
- Modify: `src/main/java/imsng/player_to_player/server/GroupTable.java`
- Modify: `src/main/java/imsng/player_to_player/registry/ChunkRegistry.java`
- Create: `src/test/java/imsng/player_to_player/server/ExitTransactionHandlersTest.java`

1. 为 `EXIT_BEGIN/PLAYER/CHUNK/COMMIT` 编写假连接、假写回测试。
2. 新增只增不改号的消息类型并把协议版本提升到 3。
3. `EXIT_BEGIN` 校验主客户端和清单并持久化 pending。
4. 玩家消息验证 NBT 后原子写玩家文件；区块消息复用现有校验与 ChunkStorage 写入。
5. `EXIT_COMMIT` 仅在本轮数据齐全时刷新全部维度、释放并持久化注册表、解散组、持久化 committed。
6. 断连监听在 pending 事务存在时跳过玩家/算力以外的组和区块清理。
7. pending 重启时从事务清单恢复冻结；committed 通过 cleanupComplete 保证清理只执行一次。
8. 强杀后同客户端正常 ROLE_REQUEST 会以最后实时确认数据放弃旧 pending，再进入角色指派。

### Task 4: 让实时玩家与区块上行得到确认

**Files:**
- Create: `src/main/java/imsng/player_to_player/group/PlayerStateUploadService.java`
- Create: `src/test/java/imsng/player_to_player/group/PlayerStateUploadServiceTest.java`
- Modify: `src/main/java/imsng/player_to_player/group/ChunkUploadService.java`
- Modify: `src/main/java/imsng/player_to_player/group/GroupRuntime.java`
- Modify: `src/main/java/imsng/player_to_player/group/GroupServerHooks.java`
- Modify: `src/main/java/imsng/player_to_player/server/ChunkDataHandlers.java`

1. 测试玩家状态同内容去重、单在途上传、失败保留最新状态、确认后更新基线。
2. 每秒在服务器线程捕获玩家 NBT，后台请求 `PLAYER_DATA_UPLOAD` 并等待落盘确认。
3. 区块实时上行改用 `request`；失败不丢弃同区块最新版本，成功确认后才移除。
4. GroupRuntime attach/detach 管理两个上传服务生命周期。

### Task 5: 捕获完整最终状态并冻结新区块申请

**Files:**
- Modify: `src/main/java/imsng/player_to_player/group/ChunkClaimClient.java`
- Modify: `src/main/java/imsng/player_to_player/group/ChunkUploadService.java`
- Modify: `src/main/java/imsng/player_to_player/group/GroupRuntime.java`
- Modify: `src/main/java/imsng/player_to_player/group/GroupServerHooks.java`
- Create: `src/test/java/imsng/player_to_player/group/ExitBundleTest.java`

1. 测试 manifest 固定后新区块申请被拒绝，bundle 只能包含 manifest 中的玩家/区块。
2. `beginExitDrain` 在集成服线程固定清单并捕获玩家状态。
3. `SERVER_STOPPING` 序列化 manifest 中仍加载的全部区块；缺失实例使用最近捕获数据或“保留服务端已有数据”标记。
4. `SERVER_STOPPED` 发布不可变 ExitBundle，退出路径不调用普通 teardown。
5. 最终玩家文件优先于 EXIT_BEGIN 时的早期内存快照；大 NBT 经 512 KB 分片传输并校验 SHA-256。

### Task 6: 实现可无限重连重放的客户端事务发送器

**Files:**
- Create: `src/main/java/imsng/player_to_player/group/ExitTransactionClient.java`
- Create: `src/test/java/imsng/player_to_player/group/ExitTransactionClientTest.java`
- Modify: `src/client/java/imsng/player_to_player/client/session/WorldSession.java`

1. 用假连接测试 begin/上传/commit 任一步失败后重新连接并从 begin 重放，成功前不返回。
2. 实现带可注入等待器的重试循环；生产等待采用 1 秒起、最高 30 秒的封顶退避，无最大次数。
3. WorldSession 保存物理服端点和握手身份，提供退出专用“复用现有连接或重新 HELLO”的连接工厂。
4. 普通 teardown 关闭重连产生的替换连接。

### Task 7: 拦截游戏内退出和窗口关闭

**Files:**
- Create: `src/client/java/imsng/player_to_player/client/session/ClientExitCoordinator.java`
- Create: `src/client/java/imsng/player_to_player/client/mixin/MinecraftExitMixin.java`
- Modify: `src/client/resources/player_to_player.client.mixins.json`
- Modify: `src/client/java/imsng/player_to_player/client/group/WorldSwitcher.java`
- Modify: `src/client/java/imsng/player_to_player/client/boot/ClientBootstrap.java`
- Modify: `src/client/java/imsng/player_to_player/client/session/WorldSession.java`

1. Mixin 在 `Minecraft.disconnect(Screen)` 与 `Minecraft.stop()` HEAD 可取消注入。
2. 编排性切换和旁路重入直接放行；普通本地组退出取消原调用并启动一次退出流程。
3. 显示原版 `ProgressScreen`，异步等待 `EXIT_BEGIN`；确认后在客户端线程旁路调用原版 disconnect。
4. 收到最终 bundle 后无限重连提交；committed 前窗口 stop 一直被取消。
5. committed 后 teardown；游戏内退出进入原目标界面，窗口关闭重新旁路调用 stop。

### Task 8: 验证

1. 运行全部针对性测试。
2. 运行 `./gradlew.bat test`。
3. 运行 `./gradlew.bat build`。
4. 运行 `git diff --check` 和 `git status --short`，确认没有提交、没有无关改动。
5. 检查兼容性敏感差异，明确报告 `Protocol.VERSION` 和消息类型变化。
6. 实机验证：首次加入、再次加入坐标；游戏内退出；窗口关闭；退出前断网；提交中服务端重启；任务管理器强杀后的最后确认数据。

### Task 9: 最终载荷耐久暂存与提交后应用

**Files:**
- Create: `src/main/java/imsng/player_to_player/server/ExitPayloadStore.java`
- Create: `src/test/java/imsng/player_to_player/server/ExitPayloadStoreTest.java`
- Modify: `src/main/java/imsng/player_to_player/server/ExitTransactionHandlers.java`
- Modify: `src/main/java/imsng/player_to_player/server/ExitTransactionLedger.java`
- Modify: `src/main/java/imsng/player_to_player/server/P2PServerService.java`
- Modify: `src/main/java/imsng/player_to_player/config/P2PPaths.java`

1. 先测试 pending 玩家/区块只进入 `shutdownId` 暂存区，正式 sink 不被调用。
2. 测试部分暂存后 abandon 会删除暂存，正式 sink 保持最后实时数据。
3. 测试数据齐全后先耐久 committed，再幂等应用全部暂存、flush、清理并写 cleanupComplete。
4. 测试 committed 后服务端重启会继续应用；应用中途失败可重试且不会降级 abandon。
5. 测试 cleanupComplete 后删除暂存目录，删除失败不影响已经完成的数据事务。

### Task 10: 终态迟到载荷与跨会话写入门控

**Files:**
- Modify: `src/main/java/imsng/player_to_player/server/ExitTransactionLedger.java`
- Modify: `src/main/java/imsng/player_to_player/server/ExitTransactionHandlers.java`
- Modify: `src/main/java/imsng/player_to_player/server/HelloHandler.java`
- Modify: `src/main/java/imsng/player_to_player/server/PlayerDataHandlers.java`
- Modify: `src/main/java/imsng/player_to_player/server/ChunkDataHandlers.java`

1. 测试“pending 校验 + 暂存 + received 标记”与 commit/abandon 在 ledger 锁内原子互斥。
2. 测试 committed/abandoned 后迟到玩家或区块载荷不调用任何 sink。
3. 测试旧连接任务在新 HELLO 发布后被拒绝；已经开始的旧写入完成前新会话不能发布。
4. 实时正式写入在 EXIT_BEGIN 屏障后执行原子未保护检查，避免最终数据被旧任务覆盖。

### Task 11: 实时区块落盘确认与异常断连释放

**Files:**
- Modify: `src/main/java/imsng/player_to_player/server/ChunkDataHandlers.java`
- Modify: `src/main/java/imsng/player_to_player/server/P2PServerService.java`
- Modify: `src/main/java/imsng/player_to_player/registry/ChunkRegistry.java`
- Modify: `src/test/java/imsng/player_to_player/registry/ChunkRegistryExitFreezeTest.java`

1. 测试实时上传只有底层写入和 flush 完成后才返回 `written=true`。
2. 按客户端跟踪已接收的区块写任务；断连先冻结，再等待任务和 worker flush。
3. 新增耐久整组释放；保存失败恢复内存占用并保持冻结，后台持续重试。
4. 把退出区块校验的压缩载荷上限改为 `ExitPayloadProtocol.MAX_ASSEMBLED_BYTES`。

### Task 12: bundle 重建与注册表变更日志

**Files:**
- Modify: `src/main/java/imsng/player_to_player/group/GroupRuntime.java`
- Create: `src/main/java/imsng/player_to_player/registry/RegistryMutationJournal.java`
- Create: `src/test/java/imsng/player_to_player/registry/RegistryMutationJournalTest.java`
- Modify: `src/main/java/imsng/player_to_player/registry/ChunkRegistry.java`
- Modify: `src/main/java/imsng/player_to_player/server/P2PServerService.java`

注册表 WAL 使用长度与 SHA-256 帧，只截断明确损坏的最后一帧；checkpoint 必须先追加并
force 完整 `replace_all` 状态，再写快照并清日志，覆盖快照已写但 WAL 未清的崩溃窗口。

1. bundle 读取、压缩或发布失败时保留 manifest/快照并无限重试；压缩失败不得转 preserve。
2. 为 claim/release/releaseAll/migrate/退出恢复写先行日志并在 ACK 前 force。
3. 启动时加载注册表快照后重放日志；checkpoint 先 force `replace_all`，再保存快照并清日志。
4. 孤儿 EXIT_BEGIN 只接受耐久注册表中属于请求组的精确清单，并检查四邻冲突。

### Task 13: 最终验证

1. 运行每个新增失败测试，确认红灯原因与目标缺失行为一致。
2. 运行全部退出、玩家状态、区块注册表定向测试。
3. 运行 `.\\gradlew.bat test --no-daemon`、`.\\gradlew.bat build --no-daemon`。
4. 统计测试总数，检查 `git diff --check`、`git status --short` 和 remap JAR 条目。
5. 不 commit；记录仍需实机执行的断网、服务端重启和任务管理器强杀场景。
