# 世界身份错配：旧本地世界覆盖新服务端（缺陷报告）

- 日期：2026-10-14
- 触发：新搭服务端 + 新种子；老客户端装了新模组进服 → **看到的是本地保留的旧世界**（旧建筑），
  退出时又把旧世界数据上行，**覆盖了服务端的 MCA**
- 证据：`test_log/26.10.14/cilent/latest.log`、`test_log/26.10.14/server/latest (1).log`、
  客户端磁盘上的两份 `level.dat`（下文第 1 节）
- 可信度标注同前一份报告：`[已核实]` / `[推断]` / `[待实测]`

---

## 0. 结论速览

世界文件夹的身份键只有 **`IP+世界名`**（`P2PPaths.java:115-116`，规范第 25 行的设计），
而**没有任何"这个世界是不是同一个世界"的校验**。于是：

1. 新服务端同 IP、同世界名 → 客户端沿用**同一个本地 data 目录**（`LocalWorldLauncher.java:99-102`）；
2. 该目录里是旧世界的 `level.dat` + `region/*.mca`（本地磁盘实证：种子不同）；
3. 区块门控在"服务端没有该区块数据"时**回退到本地磁盘读**（`ChunkMapMixin.java:130-135`）
   → 玩家直接看到旧世界的地形与建筑；
4. 退出时本地世界被整体上行（本次 `writtenChunks=2591 / ownedChunks=2637`），
   而物理服务端的自身存盘已被取消（`ChunkMapMixin.java:151-163`，"MCA 唯一写入来源=客户端上行"）
   → **服务端 MCA 被旧世界覆盖，且没有任何机制能挡住**。

这不是"配置问题"，是**数据完整性缺陷**：任何"服务端换了世界/重置了世界/从备份还原"的情形
都会静默地双向污染。修法必须同时覆盖"客户端本地存档身份校验"与"服务端拒绝陈旧上行"。

---

## 1. 证据

### 1.1 日志（客户端）

```
:132  本地存档已存在，延续既有状态: F:\p2p_mc_test\player-to-player\play.simpfun.cn+world\primary\data\world
:136  编排性断开物理服务端，打开本地组世界: F:\p2p_mc_test\player-to-player\play.simpfun.cn+world\primary\data/world
:114  [PRIMARY_CLIENT] 环境同步进度: 10/11 文件 — world/level.dat      ← 服务端的新 level.dat 只进了 environment/
:174  主客户端退出最终数据已收集: ownedChunks=2637 writtenChunks=2591 compressedBytes=8060724
:175  退出区块尾部批次提交完成: batches=2, bytes=8249096, upload_ms=3556
:176  主客户端退出事务已提交: exit.commit_ms=4024 exit.total_to_durable_ms=7106
```

`:132`/`:136` 说明：本地 data 目录被直接复用，且集成服务端打开的就是它。`[已核实]`
`:114` 说明：服务端的 `world/level.dat` 作为**环境文件**下载到了 `primary/environment/world/`，
但**从未被应用到 data 目录**（`prepareSave` 见到 data 目录已有 level.dat 就提前返回）。`[已核实]`

### 1.2 磁盘实证：两份 level.dat 的种子不同 `[已核实]`

解析客户端磁盘上两份 NBT：

| 文件 | 来源 | RandomSeed |
|------|------|-----------|
| `play.simpfun.cn+world/primary/environment/world/level.dat` | 服务端环境快照（19:47:12 下载） | **7462156823368393586** |
| `play.simpfun.cn+world/primary/data/world/level.dat` | 本地旧世界（`region/` 等文件日期为 **7 月 16 日**） | **3614799650234757351** |

⇒ 客户端在用**旧种子的旧世界**，服务端的新种子从未生效。

### 1.3 目录实证 `[已核实]`

- `primary/data/world/`（7 月 16 日创建）含 `region/r.0.0.mca`（6.0 MB）、`r.-1.-1.mca`（5.9 MB）等
  旧世界存档——**这就是旧建筑所在**。
- `primary/environment/world/` 只有 level.dat / playerdata / stats / advancements / data（无 region）——
  环境同步按设计排除了世界数据（`DESIGN.md` 第 11 节）。
- `primary/data/` 下**没有任何世界身份标记文件**（不存在 worldId/worldHash 之类），
  也没有记录"上一次加入时的服务端世界身份"。

---

## 2. 失效链（每一步都有代码依据）

| # | 事实 | 位置 |
|---|------|------|
| 1 | 世界文件夹键 = `sanitize(IP) + "+" + sanitize(世界名)`，与世界的实际身份无关 | `config/P2PPaths.java:115-116` |
| 2 | 本地 data 目录已存在 `level.dat` 时直接"延续既有状态"，**不比对种子/WorldGenSettings** | `client/group/LocalWorldLauncher.java:98-102` |
| 3 | 集成服务端打开的就是该本地存档（旧种子的 level.dat + 旧 region） | `LocalWorldLauncher.java:84-89`、`client/group/WorldSwitcher.java` |
| 4 | 门控：服务端说"无数据" → **回退 `localRead`（本地磁盘读）** → 返回旧世界区块；`PresyncStore` 未命中同样落到它 | `mixin/ChunkMapMixin.java:130-135` |
| 5 | 退出上行：本次 2591/2637 区块（≈98%）被上行；服务端按"占用组 + xPos/zPos + DataVersion"校验后**照单写入 MCA** | `group/ChunkUploadService.java`、`server/ChunkDataHandlers.java:265-281`、`docs/DESIGN.md:240-245` |

第 5 步的校验（`docs/DESIGN.md:242-243`）只验证"这块数据属于这个坐标、这个占用组"，
**不验证"它是不是这个世界的区块"**——换世界后同一坐标的旧数据完全合法。`[已核实]`

---

## 3. 现有机制为什么挡不住

- **环境哈希**只用于"文件是否一致 → 是否要下载"，不参与世界身份的判断；而且它**还会抖动**
  （见 §6.3），本身不可作为身份锚点。
- **`requiresInitialWrite`**（`ChunkClaimClient.java:236-238`：claim 时服务端无数据的区块必须至少写回一次）
  在换世界场景下**是帮凶**：新服务端对每个区块都"无数据"，于是旧世界的每一块都被判定"必须首次写回"，
  正好构成"整体覆盖服务端"。
- **物理服务端自身存盘被取消**（`ChunkMapMixin.java:151-163`）是正确设计（防陈旧内存态覆盖），
  但它同时意味着**服务端没有任何其它数据来源**去纠错。
- **规范化/摘要去重**（`ChunkUploadService` 的 `durableHashes`）在换世界时全部 miss——
  服务端基线与本地内容天然不同，去重救不了。

---

## 4. 影响与现在该怎么做（恢复步骤）

**影响**：双向静默数据损坏。服务端的世界被旧世界区块覆盖；玩家以为自己在玩新服，实际在玩本地旧存档，
并且这些旧区块被"合法地"写进了服务端。

**立刻可做的止血（无需改代码）**：

1. **客户端**：把 `F:\p2p_mc_test\player-to-player\play.simpfun.cn+world\primary\data\world`
   整体改名（如 `world.stale-20261014`）——**不要删**，那是你 7 月的旧世界，可能还要用。
2. **服务端**：本次已被写入旧数据，按你的目标二选一：
   - 想保留新世界：从备份恢复 `world/region/`（服务端 MCA 只被客户端上行写过，没有其它写入源）；
   - 新世界本来就还没内容：直接删掉 `world/region/`（以及必要时 `world/poi/`、`world/entities/`），
     让它按新种子重新生成——**注意只删客户端已经污染过的目录**，`level.dat`（新种子）保留。
3. **服务端**：`player-to-player/` 下的 `chunk-journal/`、`registry/`、`exit-transactions/`、
   `exit-payloads/` **不随世界名分割**，里面是旧世界的区块 WAL 与占用记录。旧 WAL 会被
   `RegionFileProbe` 判为"服务端已有数据"，把旧世界区块直接下发给新世界的客户端——
   **只清 `world/region/` 不够**。把这四个目录整体改名隔离（勿删）。
   *（新版代码会在启动时按世界身份标记自动隔离；标记是本次新增的，所以现存部署需要先手工做一次。）*
4. 以上都做完后再进服：客户端本地已无旧存档 → `prepareSave` 走"从环境骨架构建"分支
   （`LocalWorldLauncher.java`），用服务端的新 level.dat 建新世界，服务端无数据的区块走
   `localRead`（此时为空）→ 按种子生成。`[推断，需实测确认]`

> 顺序很重要：**只做客户端清理不做服务端清理，服务端里的旧区块会被当成"有数据"重新下发**，
> 玩家还是回到旧世界。

---

## 5. 修复方案

> **实施状态（2026-10-14 第二轮）**
> - §5 P0（客户端身份校验 + 旧存档隔离 + 门控前提）：**已实施**。新增
>   `world.WorldIdentity`（生成身份子集哈希）、`world.WorldIdentityMarker`、
>   `util.Quarantine`；落点 `client/group/LocalWorldLauncher.prepareSave`、
>   `mixin/ChunkMapMixin.player_to_player$localReadOrEmpty`、`group/GroupRuntime.GroupPlan`；
>   结果记入世界级 `config.json` 的 `worldIdentityHash`。测试：
>   `WorldIdentityTest`（10 例）、`QuarantineTest`（5 例）。
> - **额外补的一项（原报告未列，但属同一缺陷闭环）**：服务端侧。运行期状态目录
>   （`chunk-journal/`、`registry/`、`exit-transactions/`、`exit-payloads/`）不随世界名
>   分割，换世界后旧 WAL 会被 `RegionFileProbe` 当作"服务端已有数据"，把**旧世界区块
>   下发给新世界**——客户端修好了也白搭。现由 `P2PServerService.quarantineStaleWorldState`
>   在世界身份标记变化时整体隔离这些目录。
> - §5 P1（协议层 `worldId`）：**未实施**，仍在队列。已知缝隙：worldId 只加在帧 JSON
>   时不进 WAL，重启恢复后旧 WAL 记录仍会物化写 MCA；要彻底闭环需进 `ChunkDeltaBatch`
>   格式（会破坏既有 WAL 可重放性，需单独设计）。
> - 身份口径偏离原建议一处：**不把 `Data.DataVersion` 算进身份**。它随版本升级变化，
>   而版本升级后本地 region 数据仍属同一世界（DataFixer 会升级），算进去会造成无谓隔离。



### P0 —— 客户端：世界身份校验 + 旧存档隔离（最小闭环，不需要协议改动）

1. 在 `LocalWorldLauncher.prepareSave` 之前加一道校验：比较
   - 环境目录 `environment/.../world/level.dat`（服务端的），
   - 本地 `data/<世界名>/level.dat`（本地的），
   取**世界生成身份子集**做哈希：`Data.WorldGenSettings`（seed / dimensions / generator settings /
   generate_features）+ `Data.DataVersion`；缺失 `WorldGenSettings` 的旧存档回退用 `Data.RandomSeed`。
   *为什么是这个子集*：`level.dat` 里还有 `LastPlayed`/`Time`/内嵌玩家等每局都变的字段，
   而 `PlayerStateFiles.installAuthoritative` 每次加入都会改写它（`PlayerStateFiles.java:32-69`，
   只替换内嵌玩家、不动 WorldGenSettings），按整文件比对必然误报。
2. 不一致时**隔离而非删除**：把本地存档改名（`world.stale-<worldHash8>-<时间戳>`），
   然后按首次启动从环境骨架重建；日志用 WARN 明确打出"服务端世界身份已变化，已隔离旧本地存档"。
3. 校验结果落盘一份身份标记。**推荐直接写进世界级 `config.json`**
   （`P2PPaths.worldConfig`，实测已有 `serverIp` / `worldName` / `lastEnvironmentHash` /
   `lastJoinedEpochMillis` / `lastRole` 五个字段）：加 `worldIdentityHash` 即可，
   不用新造文件。注意 `lastEnvironmentHash` **不能**直接当身份用——环境哈希把
   `world/level.dat`、`playerdata`、`stats` 这些每局都变的文件算在内（见 §6.3），
   会持续抖动。
4. **同时**给门控加前提约束：`localRead` 回退只在"本地存档身份已校验通过"时可走；
   校验不通过（无法比对、缺失文件等）时按"无本地缓存"处理，宁可走种子生成。
   （规范第 24 行"继承"要的是复用同一世界的既有文件，不是复用任意旧文件。）

### P1 —— 协议层：世界身份进握手（防旧客户端/误配置继续污染）

- 服务端在 `HELLO_ACK`（现有 `世界=world` 字段旁）下发 `worldId`：世界生成身份哈希 + 一个
  **每次世界重建就变化**的实例 UUID（持久化在服务端世界目录里，随世界文件夹一起被删/重建而重新生成）。
- 客户端把 `worldId` 带上行帧（`CHUNK_DATA_UPLOAD` / `CHUNK_DATA_BATCH` / `EXIT_*`），
  服务端不匹配即**拒绝并告警**（不写 MCA）。这是防"客户端跑着旧构建/用户手工清了客户端目录"的最后一道闸。
- 属于**加性字段**：旧端缺字段时服务端可配置为"宽松放行（记 WARN）"或"严格拒绝"，不升 `Protocol.VERSION`
  （与 `MessageType`"只增不改号"纪律一致）。

### P2 —— 服务端：上传内容与本地世界的一致性抽查（可选）

对上行 NBT 做低成本抽检（如 `DataVersion` 与生成器设置相关字段），发现与自身世界明显不符时拒绝该批。
纯防御，收益有限，优先级最低。

---

## 6. 同一份日志里发现的另外两个严重问题

### 6.1 服务端 native 线程耗尽 → 连接被打断（`[已核实]`，严重）

```
[19:48:43] [p2p-netty-worker-2/WARN]: 连接 /<客户端IP>:<端口> 异常，断开:
           java.lang.OutOfMemoryError: unable to create native thread: possibly out of memory
           or process/resource limits reached
[19:48:43] 组 ... 已解散（主客户端 ... 离线，1 名成员）
[19:48:43] 客户端断开清理完成: ...，耐久释放 1696 个区块
[19:48:48] LXW1019 lost connection: Timed out
```

- 该次会话服务端共创建了 **至少 921 个 `p2p-io-*` 线程**（日志中出现过 `p2p-io-921`），
  客户端侧同期只有 20 个。`[已核实]`
- 根因组合：`ThreadPools.IO = Executors.newCachedThreadPool()`（**无界**，`util/ThreadPools.java:26-27`）
  + 服务端把**逐区块**任务丢进该池（`ChunkDataHandlers` 的 `ThreadPools.io().execute(...)`：
  解压/校验/物化/释放，`:208/:224/:304/:396`；`ExitTransactionHandlers` 同样 `executor.execute(...)`）
  + 每条任务都要在全局锁/`channel.force(true)` 上阻塞（逐区块一次 fsync，见前一份报告 §2.4，`[已核实]`）。
  任务阻塞 → cached pool 继续建线程 → 触顶 native 线程上限。
- 后果：连接被异常断开、组被解散、1696 个区块走断连清理释放；客户端随后进入
  "区块上行失败，保留最新状态等待重试"的长时间重试。
- 修法方向：把 io 池换成**有界**池（或按 CPU/磁盘并发度设上限），逐区块任务改为**成批任务**
  （一个任务处理一批区块，天然合并 fsync），并在池满时用队列而不是新建线程。

### 6.2 运行期上行的「4 MiB / 15 秒悬崖」已被实测触发 `[已核实]`

客户端日志（19:51:50–19:52:58）：

```
区块上行失败，保留最新状态等待重试: minecraft:overworld;X;Z
java.util.concurrent.TimeoutException: 请求超时: CHUNK_DATA_BATCH _rid=29606
    at ...NettyControlConnection.lambda$request$0(NettyControlConnection.java:117)
    at ...ChunkUploadService.sendBatch(ChunkUploadService.java:420)
```

重试间隔约 17 秒（15 秒超时 + 2 秒退避），与前一报告 §2.1 的推断完全一致。
即：**不只退出路径，运行期上行同样受 4 MiB 单帧 + 15 秒固定超时支配**；这次是服务端线程耗尽导致
响应变慢触发的，但机制同源——修法见前一份报告阶段 1（512 KiB 分片 + 按载荷线性超时预算 + 断点续传）。

### 6.3 环境快照每 ~2 秒换一次哈希（`[已核实]`，次要但影响面广）

服务端日志 19:47:20→19:47:34 连续发布 8 次新环境快照 id（`文件数=99→100→103`）。
原因是 `world/level.dat`、`world/playerdata/*`、`world/stats/*` 这些**每局都变**的世界元数据
被算进了环境全局哈希。后果：客户端每次加入都要重新同步这几个文件（本次 6 个 / 5138 字节），
且哈希不能作为世界身份锚点。
建议：把这几个易变路径声明为**非环境文件**（规范第 23 行明确允许服务端指定），
世界种子/身份改由 §5 的 `worldId` 显式下发。

---

## 7. 顺带更新：上一份报告的实测数据

本次会话的退出耗时（现版本）：`exit.prepare_ms=3`、`exit.begin_ms=243`、`exit.save_ms=860`、
`upload_ms=3556`（8.25 MB）、`exit.commit_ms=4024`、**`exit.total_to_durable_ms=7106`**、`exit.teardown_ms=0`。

即：**退出全程约 7 秒，其中约 3.6 秒是把 8.25 MB 传完**（≈18 Mbps，与你的上行相符），
无任何超时重放。上一份报告里"30~40 秒"的三种候选机制，在本次场景下**都不成立**
（若你手上有触发 30~40 秒的日志，仍按那份报告的 §7 判定表定位）。
真正把退出时间压下来的方向也随之明确：**减少退出尾包字节**（本次 98% 的区块被整体上行，
正是 §3 所说的"被污染世界的首次写回债"被全额触发的表现）。

---

## 8. 回归测试建议

1. **世界身份不匹配用例**（最重要）：造两份 `level.dat`（不同 `WorldGenSettings`）+
   一份本地 `region/`，断言 `prepareSave` 走隔离分支、新存档从环境骨架构建、
   且**没有任何区块被上行**（可用 `ChunkUploadService` 的假连接断言 zero-upload）。
2. **同世界正常路径回归**：身份一致时不得触发隔离，既有 `LocalWorldLauncher` 行为不变。
3. **门控前提**：身份未校验通过时，`ChunkMapMixin` 的 `localRead` 回退必须被禁用（走种子生成）。
4. **服务端拒绝陈旧上行**：伪造 `worldId` 不匹配的上行帧，断言服务端拒绝且未调用 `chunkMap.write`。
5. **线程上限**：以 2000 个逐区块上行任务压服务端 io 池，断言线程数有上界、无 native 线程 OOM。
