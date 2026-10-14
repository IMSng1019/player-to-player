# 区块传输链路诊断与优化报告（退出 / 合并 / 分离）

- 日期：2026-10-14
- 范围：**只诊断，不改代码**。本文档给出证据、结论与分层修复方案，供后续排期。
- 主导方向：`player_to_player-prompt.txt`（规范）。任何优化建议以"不破坏规范语义与既有耐久性不变量"为前提。
- 可信度标注：`[已核实]` = 直接读到代码/字节码/日志；`[推断]` = 由已核实事实推导，未实测；`[待实测]` = 需要一次实测才能定论。

---

## 0. 结论速览（TL;DR）

| 链路 | 判定 | 一句话 |
|------|------|--------|
| 退出上行（主客户端 → 物理服务端） | **有缺陷**（收敛性 + CPU 重复劳动） | 两个真问题：4 MiB 单帧挤 15 秒超时 + 失败整事务重放；退出时对同一批 NBT 做了**两遍全量 gzip**（第一遍还在集成服务端主线程上） |
| 预同步（合并 A → B，P2P） | **机制正确、实现低效** | 传输栈（分片 + 256 帧窗口 + 累积 ACK）是对的，不是逐区块 stop-and-wait；但发送端单线程串行、同一内容双份 gzip + 双份 WAN 流量、尾包无内容去重、`awaitAck` 无超时会让冻结窗口无上界 |
| 分离 | **有正确性缺陷（会丢数据）** | 原主在 `forgetLocal` 前不存盘、不等未确认 ACK；放手后三条写回路径全部拒绝，最多丢一个原版自动存盘周期的修改。此外新主逐区块 2 次往返从服务端拉取 |

**关于"30~40 秒"**：现有实测（57 秒 / 85~87 秒）都发生在批量化提交 `fa7b26c` **之前**，是逐区块串行时代的记录，不能用于解释当前版本。当前版本的候选机制有三个，判定方法见 §7（用已有埋点，5 分钟内可定论）：

1. **退出时两遍全量 gzip + 每区块三次深拷贝**（CPU 密集，与带宽无关）——证据最充分，且能同时解释"上行 >10 Mbps 却仍要 30~40 秒"；
2. **服务端 EXIT_COMMIT 处理超过 15 秒** → 客户端超时 → 从 `EXIT_BEGIN` 整事务重放（+1~30 秒退避）；
3. **单帧 15 秒悬崖**：上行 <约 280 KB/s 时首批必然超时且永不收敛（你的带宽下不该触发，但会表现为"偶发几十秒"）。

**关于"原版怎么做到的"**：原版不是"传得快"，而是**几乎不传**——只写脏区块（`isUnsaved()==false` 直接跳过），写盘异步成批、最后只排空一次，且存档留在本地磁盘。模组把"世界最终数据必须落到远端服务端"变成了硬约束，所以必须靠"少传 + 摊开传 + 只压一遍"来逼近原版体感。详见 §4。

**关于"要不要多线程"**：要的不是更多线程，而是四件事——①压缩/哈希离开 MC 主线程并并行；②同一份内容只压一遍；③网络侧用"批量 + 窗口 + 断点续传"取代单帧 stop-and-wait；④保持每维度单写者顺序（对齐原版 `IOWorker`）。详见 §5。

---

## 1. 症状与实测证据

### 1.1 症状

- 主客户端退出世界需等待 30~40 秒；日志显示耗时集中在区块传输。
- 对照组：同一个世界、同一时刻，**原版自己的停服存盘只用了不到 1 秒**。
  `test_log/26.8.5/cilent/latest.log:152-159`：`13:48:58 Saving chunks for level ...` 与
  `13:48:58 ThreadedAnvilChunkStorage (world): All chunks are saved` 同一秒内完成。`[已核实]`

### 1.2 仓库内两份慢退出实测

**A. 2026-08-05 13:48（`test_log/26.8.5/cilent/latest.log`）**

```
:145  13:48:58 物理服务端已确认退出保护意图: ownedChunks=2137 players=1
:162  13:48:59 主客户端退出最终数据已收集: ownedChunks=2137 writtenChunks=1654 compressedBytes=7278191
:163  13:49:56 主客户端退出事务已提交
:165  13:50:03 Stopping!
```

- `:162 → :163` = **57 秒**，全部在"提交事务"这一段；`:163 → :165` 另有 **7 秒**会话清理。`[已核实]`
- 1654 区块 / 7.28 MB ⇒ 57 秒 ≈ **29 区块/秒、1 Mbps**。`[已核实]`
- 该时段**没有任何重试/超时告警**，服务端日志同期静默（`test_log/26.8.5/server/latest (1).log` 13:49:00→13:49:55）。
  57 秒 ÷ 1654 ≈ **34.5 ms/区块**——与"每区块一次请求-应答的单程往返"量级吻合。`[推断]`

**B. 2026-07-31（`test_log/26 .7.31/cilent/latest.log`，见 `docs/plans/2026-07-31-...-design.md:5-9`）**

- 退出清单 2443 / 2363 区块，约 **28 区块/秒**，耗时 **85~87 秒**（≈35 ms/区块，与 A 同量级）。`[已核实]`

### 1.3 版本时间线核实（重要：不要拿旧日志解释新症状）

- HEAD 已实现批量化：`netproto/MessageType.java:281-285` 定义 `CHUNK_DATA_BATCH(104)`、
  `EXIT_CHUNK_BATCH(105)`、`EXIT_STATUS(106)`；`group/ChunkDeltaBatch.java`、
  `ExitTransactionClient.sendChunkBatches`（`:162-201`）在位。`[已核实]`
- 批量化的引入提交 `fa7b26c`（1.0.4.3）时间戳 **2026-08-05 19:57**；
  `compactForExit` / `durableHashes` 同在这一次提交。`[已核实]`
- 上面 A 组日志时间是 **2026-08-05 13:48**，**早于批量提交 6 小时**。因此 A 组的 57 秒是
  "逐区块串行往返"时代的记录。B 组同理更早。`[已核实]`

> 结论：**当前版本的 30~40 秒是另一回事**，必须用现版本日志重新判定（§7）。

### 1.4 现版本的规模数据（回归夹具）

`logs/latest.log:1282-1283`（集成测试夹具，与 A 组同规模）：

```
退出区块批次已暂存: sequence=0 chunks=960 bytes=4198597 exit.stage_ms=20
退出区块批次已暂存: sequence=1 chunks=694 bytes=3035246 exit.stage_ms=9
```

- 1654 区块切成 **2 批**（4.20 MB + 3.04 MB），服务端暂存共 **29 ms**。`[已核实]`
- 即服务端磁盘不是瓶颈；瓶颈在客户端侧的准备/压缩与链路的往返语义。`[推断]`

---

## 2. 三条链路逐条体检

### 2.1 退出上行（主客户端 → 物理服务端，直连控制连接）

**时序**（`src/client/.../session/ClientExitCoordinator.java`，行号为核实值）：`[已核实]`

| 阶段 | 线程 | 关键动作 | 埋点 |
|------|------|----------|------|
| 1 | Render | 拦截 `PauseScreen.onDisconnect` / `Minecraft.disconnect` / `stop`，切到 `ProgressScreen` | — |
| 2 | p2p-io | `GroupRuntime.beginExitDrain`：等预卸载屏障全部完成 → 冻结所有权清单 | `exit.prepare_ms` |
| 3 | p2p-io | `EXIT_BEGIN` → 服务端冻结注册表 | `exit.begin_ms` |
| 4 | Render+io | `minecraft.disconnect()` + `joined()`：真正关停集成服务端（原版 saveAll 在此发生） | — |
| 5 | **集成服务端线程** | `GroupRuntime.detach`：`compactForExit()`（**对每个未确认快照 gzip 一遍，只为算摘要**）+ 逐区块两次深拷贝 | `exit.save_ms` |
| 6 | p2p-io | `buildExitBundle`：**再 gzip 一遍**并保留；发布重试会重新构建 | `exit.save_ms` |
| 7 | p2p-io | `commitUntilAccepted`：玩家数据 + 全部区块批次 + `EXIT_COMMIT`，**串行** | `exit.commit_ms` |
| 8 | p2p-io | `teardownSession` 拆会话 | `exit.teardown_ms` |

**代码位置**：`GroupRuntime.java:164-265`（detach、`compactForExit` 调用在 `:182`、逐区块拷贝在 `:195-201`、
发布在 `:234-253`）、`GroupRuntime.java:583-618`（`buildExitBundle`，第二遍 gzip 在 `:607`）、
`ChunkUploadService.java:534-546`（`compactForExit` 逐块 gzip）、`ChunkUploadService.java:618-627`
（`compress` = `NbtIo.writeCompressed`）、`ExitTransactionClient.java:82-201`。

**发现问题**：

1. **重复劳动（CPU 密集，且与带宽无关）** `[已核实]`
   - 同一批区块 NBT 在退出时被 **gzip 两遍**：第一遍只为算摘要后丢弃（`compactForExit`），第二遍才保留
     （`buildExitBundle`）。第二遍还会在**每次发布重试**时重跑（`publishUntilAccepted` 的 supplier 每次重新求值）。
   - 每区块另有 **三次深拷贝**：`latestUnacknowledged` 内一次 `tag.copy()`（`ChunkUploadService.java:558-563`）
     + `GroupRuntime.java:199` 再一次 `copy()` + 发送时 `ExitBundle.chunkData` 一次克隆。
   - `compactForExit()` 跑在**集成服务端主线程**上（`detach` 的调用线程），即世界已停、玩家在进度界面等它。
   - 压缩实现是 `GZIPOutputStream` 默认 512 字节缓冲 + level 6、单线程串行。
   - `[待实测]` 1654 区块 × 2 遍 gzip 的具体耗时；`exit.save_ms` 会直接暴露它。
2. **单帧 15 秒悬崖 + 整事务重放** `[已核实]`
   - `BATCH_RECORD_BYTES = 4 MiB`（`ExitTransactionClient.java:34`），而
     `NettyControlConnection.request()` 在 `send()` **之前**就排好 15 秒调度超时（`:107-122`，常量
     `Protocol.java:33`）。两端都必须在收齐**整帧**后才能解码，所以 ACK 不可能早于整帧到达。
   - 上行低于约 `4 MiB / 15 s ≈ 280 KB/s` 时首批**必然超时**；而 `commitUntilAccepted` 失败后
     **从 `EXIT_BEGIN` 重放整个事务**（玩家数据 + 全部批次重传），无断点续传（`:82-130`）。
   - 服务端其实已有"整批已收则跳过"的捷径，但它同样要等整帧收完并做整帧校验之后才生效，省不掉重传。
3. **客户端无法续传** `[已核实]`：`MessageType.EXIT_STATUS(106)` 已存在且服务端有 `handleStatus`，
   但客户端从未使用——属"已批准未落地"项（`docs/plans/2026-08-05-durable-exit-checkpoint.md:78`）。
4. **提交阶段的 15 秒同样适用于 `EXIT_COMMIT`** `[推断]`：服务端该请求在 IO 池上执行
   （`ExitTransactionHandlers.java:533` `executor.execute`），但需要重读 7.2 MB 暂存 + 全量解码 + 应用 + flush；
   一旦超过 15 秒，客户端同样整事务重放。

### 2.2 预同步（合并：让出方 A → 接管方 B，P2P）

**机制** `[已核实]`（`group/PresyncSender.java`、`PresyncProtocol.java`、`p2p/ReliableChannel.java`）：

```
快照（占用区块全量 NBT）→ SNAPSHOT_DONE/ACK → 增量追赶循环至吃空
→ 冻结 tick + 主线程 saveAllChunks → 尾部增量 + 玩家 NBT → TAIL_DONE/ACK
```

- 记录格式：`type + UTF key + int len + gzip(NBT)`，每区块一条记录（无应用层批量）。
- 底层 `ReliableChannel`：1200 字节分片、**发送窗 256 帧（≈300 KiB 在途）**、累积 ACK、
  接收侧 4 MB 流控、RTO 400 ms→4 s。**不是**逐区块 stop-and-wait。`[已核实]`
- 全程在 io 线程；序列化经 `server.execute` 回主线程**逐区块**往返；主线程无阻塞等待。`[已核实]`

**问题** `[已核实]`：

1. **单线程生产者**：序列化 → gzip → 写通道严格串行，一个区块一次主线程往返；gzip 未使用
   `ThreadPools.compute()`，窗口基本填不满，吞吐由生产者而非网络决定。
2. **同一内容双份压缩 + 双份 WAN 流量**：`ChunkStorageMixin` 捕获的同一份 NBT 既被实时上行链路 gzip
   发往服务端，又被预同步 gzip 发往 B；B 载入后再次写盘上行，等于第三份。合并场景下这是常态。
3. **尾包无内容去重**：尾部（`saveAllChunks` 触发）按 ChunkKey 发送，无哈希/版本水位；快照阶段已发过的
   同内容区块可能被整包重发，**冻结窗口因此随数据量线性增长**，300 ms 目标无法保证。`[推断，需实测]`
4. **`awaitAck` 无超时**（`PresyncSender.java:231-237`）：B 若无响应或链路半死，A 的世界保持冻结，
   直到服务端 300 秒合并超时才解冻——这是最严重的用户可见故障模式。
   （`ACK_TIMEOUT_MILLIS` 常量只用于 `future.get`，不覆盖该读操作。）

### 2.3 分离

**机制**：分离**没有客户端间区块传输**——离组者成为新主，通过 `CHUNK_CLAIM_REQUEST` +
`CHUNK_DATA_REQUEST` 从**服务端**逐区块拉取（视距 12 约 1250 区块 ≈ 2.7 MB 下行，每区块 2 次串行往返）。

**缺陷（高置信）**：

1. **会丢数据** `[已核实]`：`SplitMonitor.java:277` 发 `SPLIT_REQUEST`、`:282-290` granted 后立即
   `claims.forgetLocal(key)`；此前**不存盘、不 flush、不等未确认快照的 ACK**。放手之后：
   `ChunkPreUnloadService.java:132-136`（无 claim 直接返回已完成 future）、
   `ChunkUploadService.java:257-260`（`!claimedPredicate` 即丢弃）、
   `GroupServerHooks.java:81-83`（`!claims.isClaimed` 直接 return）——三条最终写回路径全部拒绝。
   ⇒ 最多丢失**一个原版自动存盘周期**（默认 6000 tick ≈ 5 分钟）的修改。合并路径有冻结 + `saveAllChunks`
   保护，分离路径没有对应保护。
2. **新主首载较慢** `[推断]`：逐区块 2 次往返（先 claim 再 data）。协议层是异步的，可并发，不是 stop-and-wait，
   但每区块两次串行往返仍把首载时间抬高到"本地世界加载 + 千级往返"。

### 2.4 附带成本：逐区块卸载屏障（运行期，非退出专有）

`ChunkMapMixin.java:99-110` 把可靠 `CHUNK_RELEASE` future 挂进原版卸载链；而客户端
`ChunkClaimClient.releaseWhenConfirmed` 是**一区块一次请求**，服务端
`ChunkDataHandlers.appendDurableSingle` 构造**单区块批**并 `durableJournal.append`
→ `channel.force(true)`（`DurableChunkJournal.java:290-316`）。`[已核实]`
即**每卸载一个区块 = 1 次网络往返 + 服务端 1~2 次 fsync + 一次回执确认往返**。
`[待实测]` 该项在移动/传送场景下的实际占比（退出前的 `awaitQuiescent` 要等它们全部完成）。

---

## 3. "这个传输逻辑是对的吗？"

- **预同步（合并）**：传输栈本身**是对的**——分片、窗口、累积 ACK、有序、去重、无逐区块 stop-and-wait、
  主线程无阻塞。问题都在"生产者与内容"上：单线程串行、重复压缩、重复传输、尾包不去重、无超时上界。
- **退出上行**：**不正确**。三个层面的问题叠加——①同一内容压两遍、拷三遍（其中一遍在主线程）；
  ②单帧过大 + 固定 15 秒超时 + 失败整事务重放（不是增量补发）；③释放路径是逐区块往返 + 逐区块 fsync。
  这些都不是"慢一点"，而是"在特定链路条件下不收敛"。
- **分离**：**有正确性缺陷**（丢数据），且新主首载是逐区块双向往返。

---

## 4. 原版是怎么实现的（性能基线）与差距表

### 4.1 原版存档路径（对 1.20.4 字节码核对）`[已核实]`

1. `ChunkMap.save(ChunkAccess)` 开头判断 `isUnsaved()`，**为 false 直接 `return false`**——不序列化、不落盘。
   （对应 Yarn `ThreadedAnvilChunkStorage.save`；整条 `saveAllChunks` 也走它。）
2. 脏区块 → `ChunkSerializer.serialize` → `ChunkStorage.write(pos, tag)` → `StorageIoWorker.setResult`
   **异步入队**（后台工作线程成批写入 region 文件）。
3. `saveAllChunks` 末尾只做一次 `flushWorker()` = `worker.synchronize(true)` + `join()`——
   **唯一一次阻塞，且没有按区块的网络往返**。

⇒ 原版"保存 2000+ 区块"是：主线程只做"脏区块筛选 + 序列化入队"，落盘在后台并行成批，末尾排空一次。
这就是它与模组当前实现的最大差距。

### 4.2 原版把区块发给玩家 `[已核实]`

`PlayerChunkSender`（Yarn `ChunkDataSender`）：

- 批量发送，用 `chunk_batch_start` / `chunk_batch_finished` 标记批次边界；
- 客户端回 `chunk_batch_received(chunksPerTick)`，服务端据此**自适应批量**；
- 首次确认前暂停发送，之后**允许最多 10 个未确认批次**（流水线窗口）。

⇒ 原版从不在"一个请求等一个应答"上等待，也不做逐区块往返。

### 4.3 差距表

| 维度 | 原版 | 本模组当前 | 差距性质 |
|------|------|-----------|---------|
| 是否需要写 | 只写脏区块（`isUnsaved` 过滤） | 依赖 `durableHashes` 摘要去重；首次写回强制 | 语义不同（服务端要持有世界数据），但可加"规范化判等"逼近 |
| 序列化/压缩线程 | 主线程只入队（1.21 起连序列化也进 worker） | 退出时主线程 gzip 一遍 + io 线程再 gzip 一遍 | **可直接对齐** |
| 落盘 | 后台成批 + 末尾一次排空 | 服务端 WAL 一次 force + 异步 MCA 物化（已对齐）；客户端每区块卸载却要 2 次 fsync | 卸载路径需批量化 |
| 网络语义 | 批量 + 10 批窗口 + 自适应 | 单帧（最大 4 MiB）stop-and-wait + 固定 15 秒超时 + 整事务重放 | **可直接对齐** |
| 失败恢复 | 无（本地磁盘） | 有 WAL 与幂等重放，但**无增量续传** | 需补 STATUS-first 续传 |

---

## 5. 多线程结论（回答"需不需要考虑多线程"）

结论：**需要，但不是"加线程"这种加法**。四条具体判断：

1. **压缩/哈希必须离开 MC 主线程，并且并行**。当前 `compactForExit()` 在集成服务端线程上串行全量 gzip，
   这正是"世界已停、玩家干等"的直接来源。原版对应位置是把活交给后台执行器（`Util.backgroundExecutor`）。
   本仓库已有 `ThreadPools.compute()`（核数−1）专门给 CPU 密集任务用，退出尾部压缩应改走它。
   *注意*：`ThreadPools.io()` 是无界 cached pool（`util/ThreadPools.java:26-27`，见 `docs/review-findings.md` 的
   unbounded pool 条目），不适合当并行压缩池，需要**有界**执行器。
2. **同一内容只压一遍**：摘要用轻量哈希（或规范化哈希）算，压缩结果缓存复用，发布重试不重跑。
   这是"消除重复劳动"，不是并行化。
3. **网络侧要的是流水线而不是更多线程**：把"单帧 4 MiB 等 ACK"换成"小帧 + 窗口 + 断点续传"，
   单线程即可跑满带宽；原版用 10 个未确认批次就是这个意思。
4. **顺序性不能并行掉**：服务端每维度只能有一个写者（原版 `IOWorker` 单写者模型），
   客户端的批次必须保持 `(batchId, sequence)` 字节确定性，否则暂存文件按 `hash(batchId:sequence)` 命名会互相覆盖。

---

## 6. 分层修复方案（本轮不实施，供后续排期）

排序原则：**先"不收敛/会丢数据"，再"字节"，最后"CPU/线程"**。每阶段可独立验收。

### 阶段 1：退出传输收敛（含运行期同源路径）

| 项 | 内容 |
|----|------|
| 1.1 | `ExitTransactionClient.java:34` `BATCH_RECORD_BYTES` 4 MiB → **512 KiB**；`ChunkUploadService.java:62` 同步改 512 KiB |
| 1.2 | `Protocol.java` 增 `requestTimeoutMillis(msg) = 15000 + payloadBytes*1000/8192`（上限 600 s）；`ControlConnection` 增带预算的重载，`NettyControlConnection.java:107-122` 用预算排程并**补超时告警日志** |
| 1.3 | `commitUntilAccepted` 改为 **STATUS → BEGIN → 增量补发 → COMMIT**：重连先查 `EXIT_STATUS`，已暂存批次跳过；批次切分保持确定性，同 `(batchId, sequence)` 必须逐位同字节 |
| 1.4 | 服务端 `handleStatus` 回带 `receivedPlayers` / `receivedChunkKeys`；`prepareCommit` 失败时清空 pending received 自愈，防死循环 |
| 1.5 | 运行期 `ChunkUploadService.java:327-328/:419-420` 的外层超时改用同一预算（否则运行期同样有 4 MiB/15 s 悬崖，是退出尾部越滚越大的隐性来源） |
| 验收 | 夹具注入 60 KB/s 慢链路：旧组合首批必超时（4 MiB@60 KB/s≈68 s>15 s），新组合单遍收敛；中途断线第二轮只补缺失批。观察 `upload_ms`、`exit.commit_ms`、重放告警次数 |

### 阶段 2：分离最终写回屏障（防丢数据）

| 项 | 内容 |
|----|------|
| 2.1 | `SplitMonitor.java:277` 发 `SPLIT_REQUEST` **之前**，对迁移集合中"服务端无基线或仍有未确认快照"的键截取最终 NBT 并补传，**等耐久 ACK 后再 `forgetLocal`**（`:289`） |
| 2.2 | 等待设确定性上限（建议 ≤30 s，与抑制窗口对齐），超时即中止本次分离走既有重试路径——**宁可暂不分离，不可带未确认数据放手** |
| 2.3 | 新增日志"分离前置补传: N 个区块 / N 字节 / wait_ms"；同步修正 `SplitMonitor.java:63-66` 与 `MergeCoordinator.java:403-409` 的陈旧注释口径 |
| 验收 | 集成测试：迁移集含 1 个未确认快照键 → 断言 `forgetLocal` 之前该键已获正 ACK；ACK 不回 → 分离被中止。手工：离组玩家视距内改一块方块 → 分离完成后重载，修改仍在 |

### 阶段 3：降字节（先测量，后收益）

| 项 | 内容 |
|----|------|
| 3.1 | 规范化判等：忽略根级 `LastUpdate` / `InhabitedTime`（只置零不改字节）后算哈希，仅当**已有服务端耐久基线**且内容等价时才免传；不改实际发送字节 |
| 3.2 | 统一判等入口：`ChunkUploadService` 的 `durableHashes` / `compactForExit` / 预卸载释放三处共用同一判等函数；判等命中即**完全不 gzip** |
| 3.3 | **运行期检查点（已获你同意）**：新增低频扫描器，只处理"服务端无数据 且 未确认 且 未冻结"的键，每秒 ≤8 个区块、30 s 一轮、tick 超时熔断；把"首次写回债"前移出退出窗口（字节总量不变，属前移） |
| 3.4 | 预同步尾包去重：发送端维护"已发送规范化哈希"，命中即跳过并计数 |
| 3.5 | 埋点：聚合日志 `canonical_hit / eval / sent`；命中率低时再决定是否扩容易变字段，**不预设收益** |
| 验收 | 无修改长会话退出：`writtenChunks` / `compressedBytes` 显著下降，`canonical_hit/eval → ≈100%`；预同步"去重跳过"计数与冻结窗口 ms |

### 阶段 4：降 CPU / 内存 / 线程（镜像原版异步分工）

| 项 | 内容 |
|----|------|
| 4.1 | `compactForExit` 移出集成服务端线程；退出尾部改为"捕获引用级快照 → 在有界池上**单遍并行** gzip 并缓存结果" |
| 4.2 | 消除多余深拷贝（`latestUnacknowledged` + `GroupRuntime.java:199` + `ExitBundle.chunkData` 三处至少去掉一处） |
| 4.3 | 发布重试不重建 bundle（结果记忆化，失败仍走既有重试语义） |
| 4.4 | 服务端提交阶段用进程内暂存缓存避免 7.2 MB 级重读重解（缓存不完整时回退磁盘路径） |
| 4.5 | 文档同步：`docs/DESIGN.md:177/:260` 的"VERSION=4"漂移修为 v5（代码为 `Protocol.java:21` = 5）；§11.2.1 补 STATUS 续传/超时预算/跳批与新埋点 |
| 验收 | `exit.save_ms` 显著下降；`exit.commit_ms` 在缓存命中时不再包含 7.2 MB 读+解码；`exit.stage_ms` 维持夹具量级（20+9 ms） |

---

## 7. 五分钟判定法：把"30~40 秒"落到具体阶段

跑一次退出，然后在客户端 `latest.log` 上执行：

```bash
grep -nE "exit\.(prepare_ms|begin_ms|save_ms|commit_ms|total_to_durable_ms)" latest.log
grep -n "退出区块尾部批次提交完成" latest.log        # batches / bytes / upload_ms
grep -nE "退出最终提交第 [0-9]+ 次尝试失败" latest.log  # 是否发生整事务重放
grep -nE "EXIT_BEGIN 第 [0-9]+ 次尝试失败" latest.log
```

判据表：

| 观察 | 结论 | 对应阶段 |
|------|------|---------|
| `exit.save_ms` 占大头（收集/构建耗时） | 双遍 gzip + 深拷贝拖住退出（其中一半在集成服务端线程） | 阶段 4（+阶段 1.5 顺带） |
| `upload_ms ≈ 尾包字节 ÷ 上行带宽`（>10 Mbps 时 7.2 MB ≈ 5~6 秒） | 纯传输，接近带宽下限 | 阶段 3 减字节 + 运行期检查点 |
| 出现"退出最终提交第 N 次尝试失败，将从头重放" | 15 秒悬崖触发（帧太大或服务端 commit 太慢） | 阶段 1（1.1/1.2/1.3） |
| `exit.commit_ms` 大但无重放告警 | 服务端 commit 处理慢（重读+解码+应用+flush） | 阶段 4.4 |
| `exit.prepare_ms` 大 | 预卸载屏障/卸载往返在等 | 阶段 1（并考虑卸载批量化） |
| `exit.teardown_ms` 有秒级 | 会话清理（P2P 关闭 linger 等） | 单独跟进 |

---

## 8. 不做清单与残留风险

**本方案明确不做**（避免过度设计）：

- 不做自适应批量/自适应超时（固定 512 KiB + 线性预算已可证明收敛，自适应会破坏"重发同批次逐位同字节"的前提）。
- 不做 10 批大窗口流水线（512 KiB × 14~16 批下 RTT 合计不足 1 秒，收益不抵 16 MiB 在途内存与复杂度）。
- 不做"种子纯净区块免传"（与仓库既定语义冲突：`ChunkClaimClient.java:233`、`docs/DESIGN.md:225` 要求
  新生成区块首次释放必须携带数据；误判等于把已修改区块当纯种子丢弃）。
- 不升协议版本、不改消息编号（`EXIT_STATUS` 与附加数组均为加性字段，旧端缺字段时按空集退回全量重发）。
- 本轮不改退出 UI 进度/取消通道（残留风险：链路低于约 6.5 KiB/s 时退出仍会长时间等待，但每次重试都有日志、不丢数据）。

**残留风险**：

1. 若阶段 3 的 `canonical_hit` 实测很低，字节下降不及预期——用键名差异日志收敛，而不是省略耐久或跳过首次写回。
2. 预同步 `awaitAck` 无超时在阶段 1 不改，B 侧假死仍可能长时间冻结 A（有服务端 300 s 合并超时兜底）。
3. 分离的"新主逐区块拉取"属延迟问题，阶段 2 不修，记录为后续独立优化。
4. 逐区块卸载屏障（1 往返 + 服务端 2 次 fsync/区块）在高频移动场景下的开销未实测，可能成为下一个瓶颈。

---

## 9. 引用索引（本文档用到的关键位置）

代码：

- `src/main/java/imsng/player_to_player/group/GroupRuntime.java:164-265`（detach / compactForExit 调用 / 深拷贝 / 发布）
- `src/main/java/imsng/player_to_player/group/GroupRuntime.java:583-618`（buildExitBundle，第二遍 gzip）
- `src/main/java/imsng/player_to_player/group/ChunkUploadService.java:54-66`（常量）、`:255-262`（claim 丢失即拒）、`:534-546`（compactForExit）、`:558-563`、`:618-627`
- `src/main/java/imsng/player_to_player/group/ExitTransactionClient.java:31-34`、`:82-130`、`:162-201`
- `src/main/java/imsng/player_to_player/netproto/NettyControlConnection.java:107-122`；`netproto/Protocol.java:21/24/33`；`netproto/MessageType.java:281-285`
- `src/main/java/imsng/player_to_player/server/ExitTransactionHandlers.java:533`、`:585`；`server/DurableChunkJournal.java:290-316`；`server/ChunkDataHandlers.java:265-281`
- `src/main/java/imsng/player_to_player/group/SplitMonitor.java:76`、`:277`、`:282-290`；`group/ChunkPreUnloadService.java:132-136`；`group/GroupServerHooks.java:81-83`
- `src/main/java/imsng/player_to_player/group/PresyncSender.java:99-157`、`:231-237`；`p2p/ReliableChannel.java:60-71`
- `src/main/java/imsng/player_to_player/mixin/ChunkMapMixin.java:99-110`、`:151-163`；`mixin/ChunkStorageMixin.java:39-54`

日志与文档：

- `test_log/26.8.5/cilent/latest.log:145/152-159/162-165`
- `logs/latest.log:1282-1283`（夹具：2 批，`exit.stage_ms=20/9`）
- `docs/plans/2026-07-31-chunk-pre-unload-dirty-writeback-design.md:5-9`
- `docs/plans/2026-08-05-durable-exit-checkpoint-design.md:5-7/65-77`
- `docs/DESIGN.md:174-180`（兼容性纪律）、`:205-208`（退出时序）、`:218-245`（数据面与写回）、`:326-340`（预同步）
- `player_to_player-prompt.txt:26/31/57/61/62`（预同步、加载/卸载规则、主客户端离开）

---

## 10. 追加实测（2026-10-14 晚，`test_log/26.10.14/cilent/latest.log`）

现版本一次真实退出（2591 区块 / 8.25 MB 尾包）：

```
exit.prepare_ms=3  exit.begin_ms=243  exit.save_ms=860
退出区块尾部批次提交完成: batches=2, bytes=8249096, upload_ms=3556
exit.commit_ms=4024  exit.total_to_durable_ms=7106  exit.teardown_ms=0
```

- **退出全程约 7 秒**，其中 3.6 秒是把 8.25 MB 传完（≈18 Mbps），**无超时重放**。
- ⇒ 本次场景下 §0 的三个候选机制均不成立；瓶颈回到"字节量"（98% 的区块被整体上行，
  原因见 `2026-10-14-world-identity-mismatch-corruption.md` §3）。
- 同一份日志另有两项实测：运行期上行**已触发** `CHUNK_DATA_BATCH` 15 秒超时（§6.2）、
  服务端 io 池**线程耗尽**致连接被断（§6.1）——均在该缺陷报告中，修法与本报告阶段 1 同源。
