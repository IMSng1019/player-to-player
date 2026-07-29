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
