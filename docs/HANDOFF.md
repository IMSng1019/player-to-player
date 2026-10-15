# 交接状态板（给下一个会话）

最后更新：2026-10-14。**这是入口文档**：先读这里，再按需展开两份报告与 `docs/DESIGN.md`。

优先级顺序：`player_to_player-prompt.txt`（规范，主导方向；内容同 `AGENTS.md` 第 35-112 行）
> `docs/DESIGN.md`（已实现架构）> 本文档。三者冲突时按此顺序取舍。

---

## 0. 五分钟上手

1. `player_to_player-prompt.txt` —— 规范。任何优化不得破坏它的语义（区块唯一归属、
   主客户端负责其加载区块、服务端持有最终数据、退出前完成算力交接）。
2. `docs/DESIGN.md` —— 已实现到什么程度、既有不变量（尤其 §9 线程模型、§11 数据面、§12 合并/分离）。
3. 本文档 §1 队列，挑一项开工。
4. 需要细节再看两份报告（§1 表里有链接）。

---

## 1. 未完成工作队列

| 优先级 | 事项 | 关键位置 | 验收 | 状态 |
|--------|------|----------|------|------|
| **P0** | **服务端 native 线程耗尽**：`ThreadPools.io()` 是无界 cached pool，服务端把**逐区块**任务丢进去（解压/校验/物化/释放），每条任务还要在全局锁与逐区块 `channel.force(true)` 上阻塞 → 本次会话创建 ≥921 个线程后 `OutOfMemoryError: unable to create native thread`，连接被掐断、组解散 | `util/ThreadPools.java:26-27`、`server/ChunkDataHandlers.java:208/224/304/396`、`server/ExitTransactionHandlers.java`（`executor.execute`）、`server/DurableChunkJournal.java:290-316` | 以 2000 个逐区块任务压测，线程数有上界、无 OOM | **✅ 已实施**（io 池改固定线程数 8~32 + 无界队列；长驻任务迁 `ThreadPools.startBlocking`；`ThreadPoolsTest` 以 2000 任务断言上界**并**断言长驻线程不占池。§9.2/§9.3：BEGIN 屏障任务因死锁必须走专用线程；"fsync 合并"经论证后**未做**并撤回了无效的客户端滞留窗口，理由见 DESIGN §9.3） |
| **P0** | **世界身份错配**：世界文件夹键只有 `IP+世界名`，客户端见到本地 `level.dat` 就"延续既有状态"，不比对种子 → 换世界后继续用旧本地世界，退出再覆盖服务端 MCA（双向污染，双向都要清） | `config/P2PPaths.java:115-116`、`client/group/LocalWorldLauncher.java:98-102`、`mixin/ChunkMapMixin.java:130-135`、`server/ChunkDataHandlers.java:265-281` | 身份不一致时隔离旧存档并从环境骨架重建；门控 `localRead` 回退加"身份已校验"前提 | **✅ 已实施**（`world.WorldIdentity` + `world.LocalSaveGuard`（可测）+ 门控 + 服务端 `quarantineStaleWorldState`；同一世界判定含"同种子"放宽以免版本升级误隔离。**残留**：同种子重建世界识别不了，见本文档 §3 止血说明；待你实测） |
| P1 | 协议层 `worldId`：`HELLO_ACK` 下发世界身份，上行帧携带，服务端不匹配即拒写 MCA（加性字段，不升 `Protocol.VERSION`） | `netproto/HelloHandler.java`、`server/ChunkDataHandlers.java` | 伪造不匹配 worldId 的上行被拒且未调用 `chunkMap.write` | 未开始 |
| P1 | 退出/运行期传输收敛：4 MiB 单帧挤 15 秒固定超时 + 失败整事务重放 → 512 KiB 分片 + 按载荷线性超时预算（`15000 + bytes/8KiB`）+ 重连先查 `EXIT_STATUS` 断点续传 | `group/ExitTransactionClient.java:34/82-130/162-201`、`netproto/NettyControlConnection.java:107-122`、`group/ChunkUploadService.java:62/327-328/419-420` | 60 KB/s 慢链路夹具下旧组合必超时、新组合单遍收敛；断线后只补缺失批 | 未开始（运行期已实测触发超时：`CHUNK_DATA_BATCH _rid=29606`） |
| P1 | 分离丢数据：`forgetLocal` 前不落盘、不等未确认 ACK，放手后三条写回路径全拒（最多丢一个自动存盘周期） | `group/SplitMonitor.java:277/282-290`、`group/ChunkPreUnloadService.java:132-136`、`group/ChunkUploadService.java:257-260`、`group/GroupServerHooks.java:81-83` | 迁移集含未确认快照时，`forgetLocal` 前必须拿到正 ACK；ACK 不回则中止分离 | 未开始 |
| P2 | 降字节：规范化判等（忽略 `LastUpdate`/`InhabitedTime`）+ 运行期检查点（**用户已同意，低速限流**）＋预同步尾包去重 | `group/ChunkUploadService.java:91/512-528/534-546`、`group/PresyncSender.java:99-152` | 无修改长会话退出 `canonical_hit/eval→≈100%`；实测命中率决定收益，不预设 | 未开始 |
| P2 | 降 CPU/线程：`compactForExit` 移出集成服务端主线程、单遍并行 gzip、发布重试不重建 bundle、服务端提交用进程内缓存 | `group/GroupRuntime.java:182/195-201/234-253/607`、`server/ExitPayloadStore.java` | `exit.save_ms` 显著下降；`exit.commit_ms` 在缓存命中时不含 7.2 MB 读+解码 | 未开始 |
| P3 | 环境快照抖动：`world/level.dat`、`playerdata`、`stats` 等易变文件在环境哈希里 → 服务端每 ~2 秒换一次快照 id（实测 15 秒内 8 次） | `env/EnvironmentScanner.java`、`env/EnvSyncServerHandlers.java`、`config/EnvironmentPathPolicy`（服务端可指定非环境文件） | 静置世界时环境 id 稳定；世界身份改由 P1 的 `worldId` 承载 | 未开始 |
| P3 | 预同步健壮性：`awaitAck` 无超时（B 假死会让 A 冻结到服务端 300 秒超时）、发送端单线程串行、同一内容双份 gzip + 双份 WAN 流量 | `group/PresyncSender.java:99-157/231-237`、`group/ChunkStorageMixin.java:39-54` | `TAIL_DONE/ACK` 有上界；合并期间不再重复压缩同一份 NBT | 未开始 |

详细方案（取舍理由、不做清单、残留风险）：

- 传输延迟：`docs/plans/2026-10-14-chunk-transfer-latency-diagnosis.md`
- 世界身份：`docs/plans/2026-10-14-world-identity-mismatch-corruption.md`

---

## 2. 已确认结论（不必重新论证，除非有反证）

1. **原版基线**（对 1.20.4 字节码核对）：`ChunkMap.save(ChunkAccess)` 开头 `isUnsaved()==false`
   直接 `return false`（只写脏区块）；主线程只序列化入队，落盘在后台成批，`saveAllChunks` 末尾
   一次 `flushWorker()` 排空。向客户端发区块走 `PlayerChunkSender`：批量 + 最多 10 个未确认批次 + 自适应。
2. **批量化早在 2026-08-05 19:57 就落地**（`fa7b26c`）；仓库里 2026-08-05 13:48 与 07-31 的慢退出
   日志都**早于它**，是逐区块串行时代（≈35 ms/区块）的记录，**不要拿来解释当前症状**。
3. **现版本退出实测 7.1 秒**（`test_log/26.10.14/cilent/latest.log`：`exit.prepare_ms=3`、
   `begin_ms=243`、`save_ms=860`、`upload_ms=3556`（8.25 MB）、`commit_ms=4024`、
   `total_to_durable_ms=7106`、`teardown_ms=0`），无超时重放。
4. **世界身份错配已被铁证确认**：环境目录里服务端 `level.dat` 的 `seed=7462156823368393586`，
   本地 `data/world/level.dat` 的 `seed=3614799650234757351`；本地 `region/*.mca` 是 7 月 16 日的旧世界。
5. **`io-921` 不是"上限"**（第二轮复核）：OOM 发生在 19:48:43，而 `io-921` 首次出现在
   19:48:51 —— 那 8 秒里线程还成功创建了。`p2p-io-N` 只是**创建序号**（`AtomicInteger` 递增、
   不复用），OOM 是瞬态资源压力。不要把 921 当阈值写进压测判据。
6. **日志编码各不相同**：客户端日志是 **GBK**，服务端日志是 **UTF-8**（上一版 HANDOFF 统称
   GBK 是错的；按 GBK 强转服务端日志会得乱码）。
7. **逐区块 fsync 的真正杠杆不是 journal group commit**：三条上行路径都在
   `durableBatchLock` 内完成 append+force，彼此天然串行，没有可合并的重叠窗口（论证见
   `DESIGN.md` §9.3）。客户端"单区块滞留窗口"曾实现后被<b>撤除</b>（卸载的 enqueue 会被
   claim 过滤、自动存盘本就是同 tick 批量入队，滞留只省下每个洪峰的首帧却给孤立区块加延迟）。
   要降只有"减少临界区个数"：服务端逐区块帧改成队列 + 成批工作线程（**未做**）。
8. **线程池/日志排查口径变化**：io 线程仍是 `p2p-io-N`（N = 创建序号）但有上界；
   长驻任务现在是 `p2p-blocking-<类别>-N`。判断"线程是否耗尽"要看**存活线程数**与
   队列深度告警，不要再用"出现过 `p2p-io-921`"这类**累计创建序号**当阈值。

---

## 3. 用户工作方式（必须遵守）

- **先诊断报告、确认后再改代码**；报告落到 `docs/plans/YYYY-MM-DD-*.md`。
- **不要 commit**；直接在 `main` 上改，由用户自己提交（`AGENTS.md:33`）。
- 用中文回答；代码中文注释。
- 凡是仓库/ git 能核实的事实（版本、提交时间、常量、某机制是否已实现）**先查再问**，
  用户会明确要求"你先检查一下再问我"。
- 证据分级：`[已核实]`（读到代码/字节码/日志）/ `[推断]` / `[待实测]`——报告里要标出来。

---

## 4. 已知陷阱

- 中文日志是 **GBK**：`iconv -f GBK -t UTF-8`；测试日志目录名拼写为 **`cilent`**（不是 client）。
- `logs/latest.log`（仓库根）是 **JUnit 夹具日志**，不是真实会话；真实会话在
  `test_log/<日期>/` 与测试环境 `F:\p2p_mc_test\logs\`。
- `gradle.properties` 的版本号**恒为 1.0.2**，日志里的 `player_to_player 1.0.2` 不能用来判断构建；
  要判断某机制是否在跑，查代码 + 对照提交时间。
- 批次切分必须**逐位确定**（`batchId=shutdownId`、`sequence`、同一 key 同一原子顺序）：
  暂存文件名是 `hash(batchId:sequence)`，重切分会导致覆盖碰撞。
- `EXIT_STATUS(106)` 已存在且服务端有 `handleStatus`，但**客户端从未使用**——属"已批准未落地"，用它做断点续传不算新语义。
- 分离路径 `forgetLocal` 之后，三条最终写回路径全部拒绝（见队列 P1）。
- `test_log/` 含真实 IP/端口，**不要提交**（`AGENTS.md:31`）。

---

## 5. 常用命令

```bash
# 读客户端日志（GBK）
iconv -f GBK -t UTF-8 test_log/<日期>/cilent/latest.log > /tmp/c.log && grep -nE "exit\.|退出|区块" /tmp/c.log

# 退出耗时/是否发生整事务重放
grep -nE "exit\.(prepare_ms|begin_ms|save_ms|commit_ms|total_to_durable_ms)" /tmp/c.log
grep -nE "退出最终提交第 [0-9]+ 次尝试失败" /tmp/c.log
grep -n "退出区块尾部批次提交完成" /tmp/c.log

# 世界身份核对（服务端种子应等于本地 data 里那份）
cd "F:\p2p_mc_test\player-to-player\play.simpfun.cn+world"
python -c "import gzip,struct
for p in ['primary/environment/world/level.dat','primary/data/world/level.dat']:
    d=gzip.open(p,'rb').read(); j=d.find(b'\x04\x00\x04seed'); print(p, struct.unpack('>q',d[j+7:j+15])[0])"
```

---

## 6. 消费注意（修订：上一版此处的"不要 commit"已被用户新指令覆盖）

用户在 2026-10-14 第二轮明确指示"你可以自己 commit"。提交纪律改为：

- 代码改动由我提交（每次一个独立提交；`main` 分支）；
- `test_log/` 仍**不得提交**（真实 IP/端口）；
- `docs/plans/2026-10-14-world-identity-*.md:165` 的客户端 IP 已脱敏为 `<客户端IP>:<端口>`。

### 本轮已完成的提交

1. **世界身份错配 P0**（`44ef8f4`）：`world.WorldIdentity` / `world.WorldIdentityMarker` /
   `util.Quarantine` + 客户端 `LocalWorldLauncher`（校验→隔离→重建）+ `ChunkMapMixin`
   门控前提 + 服务端 `P2PServerService.quarantineStaleWorldState`。
2. **服务端线程上界 P0**（`d823776`）：`ThreadPools.io()` 改固定线程数（8~32）+ 无界队列；
   长驻阻塞任务迁到 `ThreadPools.startBlocking`；`ThreadPoolsTest` 以 2000 个逐区块任务
   断言"线程数有上界 + 长驻线程不占 io 池"。
3. **第一轮对抗式复查修正**（`07158b6`，5 路复查 + 37 个子代理对抗校验）：见下"复查发现"。
4. **第二轮定向复查修正**（`6807732`，4 路复查修正本身）：BEGIN 发布/入队原子化（我上一版
   引入的死锁）、身份判据按端分设（客户端严格、服务端放宽）、入站会话限额。
5. 校验方式：`./gradlew test`（全量 277 例）+ `./gradlew build`。
   **注意**：两轮复查都发现了"上一轮修正自身"的问题 —— 再动线程模型/身份判定前请先读
   `DESIGN.md` §9.2 与本文档的"复查发现"。

### 2026-10-15/16 本轮：指令路由改造（口径 = fix-plan 附录 P / P.2 定稿）

6. **玩家表/身份绑定/本地权限位/`_rid` 号段**（`47226ab`）：`server.ConnectionIdentity`
   （上报身份必须是本连接所在组成员）、`PlayerTable.declare`（HELLO 是唯一建行入口，
   修掉复查 G-09 的"表恒空"）、`server.PermissionRelay` → `group.LocalPermissions`
   （按 ops 名单把真实等级落到主客户端集成服：F3+F4、指令补全、未拦截路径）、
   控制连接 `_rid` 号段按端隔离（同连接双向 request 互吞，P.0 必修前置）。
7. **全量上链 + 服务端裁定 + 分组执行**（`3edbbaf`）：`client.mixin.CommandDispatchMixin`
   （在 `performChatCommand` 本地执行那次调用点拦截）、`group.CommandRouting`
   （形状规则 + 归属预检的分类器，从解析树判定、不维护指令表）、重写的
   `group.CommandRelayClient` / `server.CommandChatHandlers`、`server.CommandAggregator`
   （回执聚合）。原 `mixin.CommandsMixin`（本地失败才上送的探针）已删除。
   口径与边界见 `DESIGN.md` §13.2 / §13.2.1 / §13.2.2；本轮的静态证据、对抗校验与
   否证用例见 `docs/plans/2026-10-15-command-routing-compliance-review.md` 与
   `docs/plans/2026-10-15-command-routing-fix-plan.md`（均已提交）。
8. 校验：`./gradlew test`（全量 326 例）+ `./gradlew build`（Loom 重映射已把 mixin 的
   `@Inject` 目标解析为 intermediary：`method_45010` / `Commands.method_9249`）。
9. **对抗式复查修正**（5 路审查 + 逐条反驳校验，共 23 条发现）：
   - **身份判据配对化**（critical）：`ConnectionIdentity.canonicalMemberId` —— 上报的
     UUID 与名字必须来自**同一个成员**，按名字兜底时还要求上报 UUID 恰是该名字的
     离线派生值。修掉"组内成员用外部 op 的 UUID + 自己的名字换取 3/4 级执行"；
   - **一人一行**（important）：位置上报归一到成员档案 UUID（集成服里副客户端的实体
     UUID 是离线派生值，原先会一人两行 ⇒ `/list` 重复计数、`/tp` 落到 0,0,0 幽灵行、
     `/msg` 随机答"不在线"）；
   - **迟到应答不再引发 ERROR 帧无限回弹**（important）：`NettyControlConnection`
     绝不对 ERROR 帧回 ERROR + 服务端对上送形状不符的帧静默忽略；
   - **切换窗口的回执不丢**（important）：集成服已交出去时回执进暂存队列，
     玩家重新进组世界（JOIN）时补投；
   - **归属预检补三类漏判**（critical）：包围盒覆盖 `/fill` 内部区块；无主且
     **侧邻属他组**的死区块（对全网都不可认领）一律拒绝；`contextBound` 形态
     （`~/^`、`/execute positioned` 包着的绝对坐标）同样过预演检查 ——
     越界就干净拒绝，而不是让发起组就地冻结；
   - **`/forceload add <from>` 不再被误判为上下文指令**（important）：缺省参数
     "由同指令另一个同类型实参补齐"（to=from）与"取执行者位置"要区分开；
   - **`/spreadplayers` 的中心点计入坐标**（important，`Vec2Argument` 纳为位置类）；
   - **维度核对**（important）：发起组执行前比对玩家此刻所在维度与上链时自报的维度；
   - **混版本能力位 `commandRouting`**（important）：双向加性声明，老客户端/老服务端
     各自保持老行为，杜绝"重复执行/静默漏执行"；
   - **权限等级快照**（minor）：指令上送的权限裁定改读 `PermissionRelay` 在服务器
     主线程维护的快照（原先从 Netty 线程直读非并发 ops 表，属数据竞争）；
   - **权限名单在 ROLE_REQUEST 后强制重推**（minor）：客户端注册处理器是在 HELLO_ACK
     之后，重连时的那次推送必然落空且被去重签名抑制 ⇒ 本地权限位停在旧值（F3+F4 又不可用）。
     改为在客户端已就绪的 ROLE_REQUEST 之后清签名重推（ACK 路径因会在 Netty 线程读
     非并发 ops 表而弃用）；
   - **CHAT_RELAY 也做身份校验**（minor）：聊天帧的 playerName 不能再是任意自报值；
   - 测试补齐：`CommandOwnershipRouterTest`（含死区块/预演否决）、
     `CommandOwnershipVerifyTest`、`LocalPermissionsParseTest`、`TokenBucketTest`、
     `RidSegmentTest`，`ConnectionIdentityTest` 增配对判据与能力位用例。
   已知残余（未修，记在 `DESIGN.md` §13.2.1 表 10-12）：`/spreadplayers` 的散点半径、
   `/place jigsaw` 的向外延伸无法从参数统一定界；控制信道无玩家级认证（半可信模型的根，
   "被改造的客户端可冒充 op 让 /stop 生效"，当前须把控制端口按控制台同权防护）。
10. **第二轮复查（语义/回归维度，独立代理）**：又修掉 1 条 critical + 6 条 important/minor：
   `/msg` 跨组投递按名字兜底（此前在"实体 UUID ≠ 成员 ID"环境下静默丢消息）、
   `/particle` 的 delta 不再当成坐标、`/function` 按上下文处理（黑盒不得在他组出生点执行）、
   服务端限速改按玩家、限速命中退化为"仅本地执行"、CHAT_RELAY 带上帧内 uuid、
   `proxy_server` 不进玩家表、3/4 级执行留服务端审计输出、退出冻结期不执行转发指令；
   另把"坐标是数据"类指令、多层 `/execute` 的盒膨胀、查询类指令只回首个成功组等
   记入 `DESIGN.md` §13.2.1 表 13-19。详见
   `docs/plans/2026-10-16-command-routing-review-findings.md`。

**指令路由的待办（按顺序）**：

1. **实测验收**（必须实机，需要第二个客户端才能测多组项）：先跑 fix-plan §6 的
   否证用例 F1–F4，再按 §6 的 0–6 条验收；单组环境可先测"服务端 `/op` → 补全里出现
   `/gamemode`、F3+F4 立刻可用、`/gamemode spectator` 生效、非 OP 的报错文案与真人原版
   一致"。注意 **F3+F4 与聊天栏是两条入口**，测试用聊天栏敲指令。
2. **3/4 级指令的组侧副作用**（未实现）：`/save-all` 通知各组刷盘、`/save-off|on` 开关、
   `/reload` 各组重载、`/stop` 各组有序退出、`/ban`/`/kick` 通知所在组踢出成员。
   每条都要复用退出事务/写回通道，属新子系统；未做之前这些指令只有服务端侧效果 + 回执。
3. 已知边界（`DESIGN.md` §13.2.1）：选择器跨组（范围外）、集成服专属指令（如 `/publish`）
   由物理服指令集裁定、混版本节点会在发起组重复执行（升级时各端一起更新）。

未实施（仍在队列）：

- P1：协议 `worldId`、退出/运行期传输收敛（512 KiB 分片 + 线性超时预算 + `EXIT_STATUS` 续传）、
  分离丢数据（`forgetLocal` 前不等 ACK）；
- P2：降字节（规范化判等 + 运行期检查点）、降 CPU（`compactForExit` 移出主线程）、
  服务端逐区块帧成批处理（见 `DESIGN.md` §9.3）、**P2P 会话数速率限制**：
  `P2PBrokerHandlers` 对每个 `P2P_CONNECT_REQUEST` 都无条件新建 sessionId（仅 120 秒过期），
  已握手客户端可循环请求让受害端不断建立会话；本轮只在上限侧兜住（`GroupHost` 的
  `MAX_HOST_SESSIONS` 名额 + 专用小池），broker 侧的去重/冷却/每对端上限仍缺；
- P3：环境快照抖动（易变文件进哈希）。

### 未修但已记录的残留（复查确认，非本次引入或影响可控）

- BEGIN 任务内有对断连清理的无界等待（`disconnectCleanups.await`）：断连清理持续失败（磁盘
  坏）时可占住若干屏障线程；disconnect 路径在 Netty 线程上 `awaitPendingBegins`（有上界但可能
  阻塞事件循环数十毫秒）。
- `EnvironmentScanner` 的"先取信号量、再提交同池任务、再 join"模式（客户端环境扫描）仍属
  "池内任务等池内任务"，但它是叶子任务、无闭环（已被对抗校验判为非缺陷）。
- 客户端 `MergeClient` 的预同步接收线程对"静默死亡"的对端没有确定出口（可靠通道只在本地
  close、对端 FIN 或中断时解除阻塞）——本轮之前既有，未恶化。

### 复查发现（第三轮，勿重复踩）

1. **BEGIN 屏障死锁（曾实测为 blocker）**：屏障在 BEGIN 任务入队**之前**就对
   `writeRealtimeIfAllowed` 可见，因此"早于 EXIT_BEGIN 到达、晚于它出队"的业务帧会
   join 屏障；有界池被这些等待者占满后，排在队尾的 BEGIN 任务永远拿不到线程 → 永久死锁。
   修法：`ExitTransactionHandlers` 增加 `barrierExecutor`，生产注入
   `ThreadPools::startBlocking`（测试仍用同一执行器按序驱动）。**不要把两者合并。**
2. **同种子判同世界（放宽）**：跨版本升级时原版 DataFixer 会重写 `level.dat`，生成设置
   *结构*变了、种子没变。只比结构哈希会把升级误判成换世界并隔离掉未物化到 MCA 的耐久区块。
   现在 `WorldIdentity.sameWorld` = 哈希相等 **或** 两侧种子相等。
3. **`level.dat` 缺失但目录仍有旧 region**：那种目录会被合并式重建"补上一个正确的
   level.dat"，从此看起来已对齐、永久放行旧区块。判据必须是"目录是否存在"，不是
   "level.dat 是否存在"。
4. 其他线程模型遗漏：`GroupHost.accept`（读会话头 15 秒超时兜底依赖任务被调度到）、
   `ProxyEnvService.initialSyncLoop`（成功前不返回的循环）——都已迁 `startBlocking`。

### 服务端现存部署的一次性止血（重要）

`player-to-player/{chunk-journal,registry,exit-transactions,exit-payloads}` 不随世界名分割。
世界换过之后，旧 WAL 会让 `RegionFileProbe` 判"服务端已有数据"，把旧世界区块下发给新世界。
新版代码靠 `world-identity.json` 标记在**下一次**换世界时自动隔离；本次换世界发生在标记
功能之前，需**手工**把这四个目录改名隔离一次（勿删）。见世界身份报告的 §4 第 3 步。

两个**自动隔离覆盖不到**的情形，需要手工做：

- **同种子重建/还原世界**（删 `region/` 保留 `level.dat`、固定 `level-seed` 重置、从同世界
  备份还原）：身份看不出来，没有告警；
- **注册表后端为 MySQL**（`config.mysql.enabled=true`）：占用记录在数据库里，不随目录隔离
  （启动日志会 WARN 提示）。
