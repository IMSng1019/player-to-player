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
| **P0** | **服务端 native 线程耗尽**：`ThreadPools.io()` 是无界 cached pool，服务端把**逐区块**任务丢进去（解压/校验/物化/释放），每条任务还要在全局锁与逐区块 `channel.force(true)` 上阻塞 → 本次会话创建 ≥921 个线程后 `OutOfMemoryError: unable to create native thread`，连接被掐断、组解散 | `util/ThreadPools.java:26-27`、`server/ChunkDataHandlers.java:208/224/304/396`、`server/ExitTransactionHandlers.java`（`executor.execute`）、`server/DurableChunkJournal.java:290-316` | 以 2000 个逐区块任务压测，线程数有上界、无 OOM；顺带把"逐区块一次 fsync"合并成批 | 未开始 |
| **P0** | **世界身份错配**：世界文件夹键只有 `IP+世界名`，客户端见到本地 `level.dat` 就"延续既有状态"，不比对种子 → 换世界后继续用旧本地世界，退出再覆盖服务端 MCA（双向污染，双向都要清） | `config/P2PPaths.java:115-116`、`client/group/LocalWorldLauncher.java:98-102`、`mixin/ChunkMapMixin.java:130-135`、`server/ChunkDataHandlers.java:265-281` | 身份不一致时隔离旧存档并从环境骨架重建；门控 `localRead` 回退加"身份已校验"前提 | **✅ 已实施**（2026-10-14 第二轮：`world.WorldIdentity` + `LocalWorldLauncher.prepareSave` + 门控 + 服务端 `quarantineStaleWorldState`；待你实测） |
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

- **世界身份错配 P0**：`world.WorldIdentity` / `world.WorldIdentityMarker` / `util.Quarantine`
  + 客户端 `LocalWorldLauncher`（校验→隔离→重建）+ `ChunkMapMixin` 门控前提
  + 服务端 `P2PServerService.quarantineStaleWorldState`；新增 15 个单测，全量 261 例通过。
- 未实施（仍在队列）：线程上界、fsync 合并、协议 `worldId`、分离丢数据。

### 服务端现存部署的一次性止血（重要）

`player-to-player/{chunk-journal,registry,exit-transactions,exit-payloads}` 不随世界名分割。
世界换过之后，旧 WAL 会让 `RegionFileProbe` 判"服务端已有数据"，把旧世界区块下发给新世界。
新版代码靠 `world-identity.json` 标记在**下一次**换世界时自动隔离；本次换世界发生在标记
功能之前，需**手工**把这四个目录改名隔离一次（勿删）。见世界身份报告的 §4 第 3 步。
