# 主客户端两秒耐久退出实施计划

> **For Codex:** 按 `superpowers:executing-plans` 和 `superpowers:test-driven-development` 逐项实施；用户已明确要求直接在 `main` 修改且不要 commit。

**Goal:** 正常网络下让主客户端退出的最终耐久 ACK 不超过两秒，断网时无限等待且可恢复。

**Architecture:** 运行期批次进入世界级 WAL；退出数据先做可重建暂存，最终由一个同时包含玩家数据、区块尾部、事务终态和 ownership release 的逻辑 WAL 事务提交。事务由多条有界数据片和一个终结标记组成，全部写完后只 force 一次。正式玩家文件、MCA、JSON 快照和清理状态全部在 ACK 后异步物化。

**Tech Stack:** Java 17、Fabric 1.20.4、Netty 控制协议、Minecraft NBT、FileChannel WAL、JUnit 5、Gradle。

---

### Task 1: 分段复合 WAL 事务

**Files:**
- Modify: `src/main/java/imsng/player_to_player/server/DurableChunkJournal.java`
- Test: `src/test/java/imsng/player_to_player/server/DurableChunkJournalTest.java`
- Test: `src/test/java/imsng/player_to_player/server/DurableExitCheckpointScaleTest.java`

**Steps:**
1. 先写失败测试：退出记录包含 manifest、玩家数据和最终区块；一次 force 后重启可恢复 committed 状态与最新区块索引。
2. 写失败测试：同一 `shutdownId` 同内容重放返回原 epoch，不同内容重用必须拒绝。
3. 实现带记录类型的 WAL 编解码、全局 epoch、复合提交索引和严格恢复校验。
4. 将 1654 区块回归阈值改为完整复合提交 `< 2_000 ms` 且 force 次数为 1。

### Task 2: 可重建退出暂存

**Files:**
- Modify: `src/main/java/imsng/player_to_player/server/ExitPayloadStore.java`
- Modify: `src/main/java/imsng/player_to_player/server/ExitTransactionHandlers.java`
- Test: `src/test/java/imsng/player_to_player/server/ExitPayloadStoreTest.java`

**Steps:**
1. 写失败测试：玩家/批次暂存不调用 durable force，服务端重启前丢失暂存时客户端可重放。
2. 增加 `prepareCommit`，从暂存区构造不可变复合提交载荷并校验集合完全一致。
3. committed 后的 apply 只读取 WAL 权威载荷并异步物化，不再次追加区块 WAL。

### Task 3: 最终 ACK 单次 force

**Files:**
- Modify: `src/main/java/imsng/player_to_player/server/ExitTransactionLedger.java`
- Modify: `src/main/java/imsng/player_to_player/server/ExitTransactionHandlers.java`
- Modify: `src/main/java/imsng/player_to_player/server/P2PServerService.java`
- Test: `src/test/java/imsng/player_to_player/server/ExitTransactionHandlersTest.java`

**Steps:**
1. 写失败测试：`EXIT_COMMIT` 的 ACK 在复合 WAL force 后发送，但在玩家/MCA/JSON 物化前即可返回。
2. 增加 ledger 的“外部已耐久后发布 committed”入口，避免 commit 再同步写 JSON。
3. ACK 后调度幂等物化与清理；失败持续重试，状态查询仍返回 committed。
4. 启动恢复从 WAL 合并 pending/committed 事务，再开放控制端口。

### Task 4: ownership 与断连恢复

**Files:**
- Modify: `src/main/java/imsng/player_to_player/registry/ChunkRegistry.java`
- Modify: `src/main/java/imsng/player_to_player/registry/RegistryMutationJournal.java`
- Test: `src/test/java/imsng/player_to_player/registry/ChunkRegistryExitFreezeTest.java`

**Steps:**
1. 写失败测试：孤儿恢复只能使用服务端权威断连凭证，任意客户端自报清单必须拒绝。
2. 增加“复合 commit 已 durable 后仅更新内存”的 ownership release，不同步 checkpoint。
3. 启动时按 WAL committed 记录覆盖旧注册表快照，正常重进消费恢复凭证。

### Task 5: 实时批次幂等与客户端恢复

**Files:**
- Modify: `src/main/java/imsng/player_to_player/group/ChunkUploadService.java`
- Modify: `src/main/java/imsng/player_to_player/group/ExitTransactionClient.java`
- Modify: `src/main/java/imsng/player_to_player/server/ChunkDataHandlers.java`
- Test: `src/test/java/imsng/player_to_player/group/ChunkUploadServiceTest.java`
- Test: `src/test/java/imsng/player_to_player/group/ExitTransactionClientTest.java`
- Test: `src/test/java/imsng/player_to_player/server/ChunkDataHandlersWalTest.java`

**Steps:**
1. 写失败测试：已 durable 批次在其中区块释放后仍能重放原 ACK。
2. 写失败测试：`forget` 在途键会取消整批及悬挂 ACK 等待、推进 sequence，并立即重发其余键。
3. 写失败测试：最终 WAL 写入与 ownership 提交之间不得插入迟到运行期批次。
4. 客户端重连先查询 `EXIT_STATUS`；超出 4 MiB 的单区块自动回退到现有分片消息。

### Task 6: 世界隔离、日志生命周期与验证

**Files:**
- Modify: `src/main/java/imsng/player_to_player/config/P2PPaths.java`
- Modify: `src/main/java/imsng/player_to_player/server/P2PServerService.java`
- Test: add focused world-path/recovery tests under `src/test/java/imsng/player_to_player/`

**Steps:**
1. 将 WAL 目录绑定规范化世界身份，验证两个世界不共享记录。
2. 增加 checkpoint/分段清理边界，至少保证日志和幂等索引可被有界回收。
3. 覆盖“durable 前缀 + 无 marker 的完整孤儿 parts”恢复，确认只截断未确认尾部并允许事务重放。
3. 运行所有定向测试并使用 `--rerun-tasks` 确认真实执行。
4. 运行 `./gradlew.bat test` 与 `./gradlew.bat build`。
5. 检查 `git diff --check`、协议版本、日志字段和未提交文件；不执行 commit。
