# player_to_player 架构设计文档

> 本文档是 `player_to_player-prompt.txt`（需求规范）的**工程落地设计**。
> 规范定义"做什么"，本文档定义"怎么做"。所有子系统实现必须与本文档一致。

## 0. 总体判断与分期路线图

完整规范（影子实例热迁移、预同步、MCA 服务端写回、指令逐级路由等）体量极大，
按依赖关系分四期实现，每一期都保持可编译、可运行：

| 期 | 内容 | 状态 |
|----|------|------|
| **Phase 1** | 基础设施：角色/配置/目录、控制协议(Netty)、环境文件扫描与同步、区块注册表、算力评分、NAT 检测与 UDP 打洞、服务端/中转端服务骨架、客户端引导、服务端世界 tick 挂起 | 已完成 |
| **Phase 2** | 主客户端集成服务端接管、副客户端经 P2P 隧道加入主客户端、区块数据上行与 MCA 单区块写回、中转端环境分发 | 已完成（详见第 11 节） |
| **Phase 3** | 预连接→预同步→合并（快照 + 增量追赶 + 冻结原子切换）、单端分离、算力再分配选主、玩家数据上行 | 已完成（详见第 12 节） |
| **Phase 4** | 指令/聊天逐级路由、传送门跨维度加载、末影珍珠特殊加载、组分离（多端）、日志收集完善、MySQL 注册表后端 | **本期实现**（详见第 13 节） |

## 1. 节点模型

```
NodeMode（配置级模式，config.json 的 mode 字段）
  SERVER        服务端        —— 装载 MC 服务端，但挂起世界 tick；维护区块注册表/玩家表/环境分发/MCA 写回
  PROXY_SERVER  中转服务端    —— 打洞协助 + 打洞失败时中转 + 环境分发(Phase 2)；本身也是一个 fabric 服务端实例
  CLIENT        客户端        —— 玩家侧；运行期内再细分 ClientRole

ClientRole（运行期客户端等级，可随合并/分离切换）
  UNASSIGNED    未定级（未加入世界）
  PRIMARY       主客户端 —— 运行本组已加载区块的世界 tick（= 联机房主：集成服务端 + 客户端）
  SECONDARY     副客户端 —— 只渲染与输入；后台热备影子实例（Phase 3）
```

- 首次加载自动检测：`FabricLoader.getEnvironmentType()` 为 SERVER → `server`，CLIENT → `client`；
  `proxy_server` 只能手动在总配置中切换。
- 全局单例 `NodeContext` 保存运行期状态（模式、角色、clientId、groupId、算力、NAT 信息）。

## 2. 目录结构（P2PPaths 统一管理）

```
<游戏目录或服务端根目录>/player-to-player/
  config.json                    # 总配置 GlobalConfig
  world-identity.json            # [服务端] 世界身份标记 WorldIdentityMarker
  registry/                      # [服务端] 区块注册表持久化（每维度一个 json）
  logs/                          # [服务端] 收集的主客户端日志（<玩家名>-latest.log）
  <IP>+<世界名>/                 # [客户端] 世界文件夹（每个"服务器+世界"一个）
    config.json                  # 世界配置 WorldConfig（含 worldIdentityHash）
    primary/                     # 主客户端角色专属
      environment/               #   环境文件
      data/                      #   数据文件（本地存档在其下 <世界名>/）
    secondary/                   # 副客户端角色专属
      environment/
      data/
```

- 世界名/IP 经 `P2PPaths.sanitize()` 做文件名合法化（Windows 保留字符）。
- 客户端每次加入世界时重新选定世界文件夹（同一客户端可进出不同服务器）。
- 被判定为"不属于当前世界"的状态一律<b>同级改名隔离</b>为
  `<原名>.stale-<身份短标识>-<时间戳>`（`util.Quarantine`），绝不删除。

## 3. 控制协议（netproto 包）

服务端/中转端在 MC 端口之外监听一个**独立 TCP 控制端口**（默认 25580 / 中转 25581），
用 MC 自带的 Netty 实现。选择独立端口而非 MC 自定义包的原因：
中转服务端不是完整 MC 服务端也要收发；大文件（环境同步）不受 MC 包大小限制；
控制面与游戏面解耦，Phase 2 重定向游戏连接时控制面不断。

### 3.1 帧格式（ControlMessage）

```
int32 totalLen   # 后续字节总长（type+jsonLen+json+binary）
int32 type       # MessageType.id
int32 jsonLen    # JSON 段长度
byte[jsonLen]    # UTF-8 JSON 对象（结构化字段）
byte[...]        # 可选二进制附件（文件块、区块数据等），长度 = totalLen - 8 - jsonLen
```

- 最大帧长 `Protocol.MAX_FRAME_BYTES`（16 MB），超限即断开（防攻击）。
- 请求-响应关联：JSON 内 `_rid` 字段（long 自增），`ControlConnection.request()` 自动填写并等待。
  **号段按端隔离**（`Protocol.RID_BASE_ACCEPTOR`=接受端低位 / `RID_BASE_DIALER`=发起端高位）：
  同一条连接上两端都会发起 request（客户端上送指令、服务端下发转发执行），而应答匹配规则是
  "命中在途表即完成 future 并不再分发" —— 共用一个自增号段会互吞：本端的 future 被对端的
  请求错配完成，对端的请求永远不进处理器，直到 15 秒超时（表现为"指令在别的组随机不执行"）。
- 心跳：双向 PING/PONG，30s 间隔，90s 无响应断开。

### 3.2 消息类型总表（MessageType）

见 `netproto/MessageType.java`，网络握手/环境同步/区块申请/算力/打洞协助/中转/日志各占一段编号区间。

## 4. 环境文件（env 包）

- **环境文件定义**：服务端根目录下除 `logs/`、`<存档目录>/region`、`<存档目录>/poi`、`<存档目录>/entities`、
  `<存档目录>/DIM-1`、`<存档目录>/DIM1`、`player-to-player/` 以及总配置 `nonEnvironmentPaths` 指定路径之外的所有文件。
  存档目录按 `server.getWorldPath(LevelResource.ROOT)` 实际值动态确定（`level-name` 可以不是 `world`；
  内置排除表仍保留 `world/` 前缀作为兜底）。
- **敏感文件内置排除**：`server.properties`（含 rcon.password 等凭据）、`ops.json`、`whitelist.json`、
  `banned-ips.json`、`banned-players.json`、`usercache.json`、`eula.txt` 一律内置排除，
  绝不进入环境清单分发给客户端（这些文件只对物理服务端有意义），管理员无需手动配置。
- 服务端启动时扫描生成 `EnvironmentManifest`（相对路径 → SHA-256 + 大小），
  清单规范化序列后再取 SHA-256 作为**全局环境哈希**。
- 客户端加入世界时先比对全局哈希，不同则拉取清单做逐文件 diff，只下载差异文件（分块传输）。
- 环境分发由服务端承担；**中转端环境分发（Phase 2，`ProxyEnvService`）**：中转端按
  `parentServerAddress` 从上级服务端以**全量视图**（ENV_MANIFEST_REQUEST 不带 target）
  同步到 `player-to-player/proxy-environment/`，再把 `EnvSyncServerHandlers` 挂到中转
  控制端口（`RelayCore.setExtraHandlers`）向客户端分发；客户端先 RELAY_REGISTER 过
  鉴权门再发 ENV_* 请求（`GlobalConfig.envSyncViaRelay` 开启且存在独立中转时优先走
  中转，失败自动回退直连）。每 `proxyEnvResyncMinutes` 分钟重同步跟进上游更新。
- **mod 前缀分发**（ModPrefixResolver）：`server-` / `proxy_server-` / `server_client-` / `client-`
  前缀可叠加、无序；无前缀 = 所有端。主副客户端可随时切换，因此客户端**同时下载**
  `server_client-` 与 `client-` 两类 mod。

## 5. 区块注册表（registry 包，服务端）

- `ChunkKey(dimension, x, z)`；`ChunkRegistry`：`ConcurrentHashMap<ChunkKey, ClaimInfo>`。
- **申请规则（规范核心）**：`tryClaim(key, groupId)` 原子地检查目标区块 + 东西南北四邻
  是否被**其他组**占用；全部空闲（或属于本组）才授予。四邻即规范中的"缓冲层"。
- 释放：单区块 `release`、整组 `releaseAll`（主客户端掉线）。
- 持久化：`RegistryStore` 后端抽象（Phase 4）——默认 JSON（`player-to-player/registry/<维度>.json`，
  定期 + 关闭时写盘），可切 MySQL（见第 13.7 节）；内存表恒为权威。
- `PlayerTable`：玩家 UUID → 名字/维度/坐标/组/端级别，供跨组 tp 指令与消息路由（第 13.2 节）使用。
  **建行只有一个入口**：HELLO 握手（`HelloHandler` → `PlayerTable.declare`；玩家客户端的
  `clientId` 就是其 MC 档案 UUID，见 `WorldSession.onJoin`）。1Hz 的 `PLAYER_POS_UPDATE(32)`
  刷新坐标、角色指派/合并/分离刷新组与端级别、断连移除。位置上报与指令上送都要过
  "身份自洽"校验（`server.ConnectionIdentity`：上报的 UUID 或玩家名必须属于该连接所在组的
  成员）—— 玩家表落库后这些坐标会直接成为跨组 `/tp` 的落点，不校验就等于把落点交给任意
  已握手连接。**它只保证"同一连接自洽"**（挡不住同组内冒用，那需要控制信道的玩家级认证）。

## 6. 算力（compute 包）

- `ComputeScore(cpuModel, localSingleCoreScore, geekbenchSingleCoreScore, geekbenchVersion,
  freeMemoryBytes, totalMemoryBytes)`。
- **排序键 = `rankScore()` = 本地单核微基准分**（确定性工作负载，每台机器跑逐字节相同的
  负载，是唯一可横向比较、且不依赖第三方页面的量）；**Geekbench 分只是参考值**
  （非官方搜索页查询，可配置关闭、失败无影响），仅在本地分缺失（旧节点上报）时兜底。
  *理由*：旧实现是"Geekbench 查得到就用 Geekbench、查不到才本地跑分"的二选一，
  两种量纲被直接混排比较 —— 查得到 Geekbench 的机器会系统性压过查不到的机器，
  选主结论与真实算力无关。`singleCoreScore()` 保留为 `rankScore()` 的兼容别名，
  JSON 新旧字段并存，故新旧节点混连时按旧字段排序的结论也一致。
- 主客户端资格 = 算力最高 **且** 可用内存 ≥ 服务端配置阈值（默认 0.5 GB，规范值）。
  全员都不达门槛时 `ComputeTable.selectPrimaryOrRelaxed` **确定性降级**为"算力最高者"
  （返回 `Selection.memoryRelaxed=true`，调用方记 warn）——宁可内存吃紧也不能让区块无人 tick。
- 服务端 `ComputeTable` 汇总所有在线客户端评分。**算力分配的候选集是涉及组的全部成员
  （含副客户端）**：规范"重新分配此区域中的玩家的客户端等级（主客户端和副客户端）"。
  但**接管方（合并的 newPrimary）受能力边界约束，只能是当前主客户端** ——
  副客户端没有集成服务端，接不了预同步；二者不等时退化执行并 WARN 归因（见第 12 节）。

## 7. P2P（p2p 包）

- **NAT 检测**：极简 STUN 客户端（RFC 5389 Binding Request，公共 STUN 服务器 + 中转端内置 STUN），
  区分 OPEN / FULL_CONE / RESTRICTED / PORT_RESTRICTED / SYMMETRIC / UDP_BLOCKED。
- **打洞流程**：A、B 均与中转端保持控制连接 → 中转端交换双方公网 endpoint（P2P_ENDPOINT_EXCHANGE）
  → 双方同时向对方 endpoint 发 UDP 探测（指数退避，约 5s 超时）→ 成功后进入加密会话。
- **加密通道**：X25519 ECDH 握手（JDK XDH）派生 AES-256-GCM 会话密钥；帧内嵌递增 nonce 防重放。
- **公钥指纹绑定（消除 UDP 中间人）**：公钥本身仍经打洞 UDP 明文交换，但**双方公钥的
  SHA-256 指纹经服务端控制信道交换并校验**（身份锚点 = HELLO 写入连接的 clientId）——
  路径攻击者替换任一侧公钥都会导致指纹不匹配，握手 fail-closed 并退回中转降级。
  时序：客户端在 HELLO_ACK 后主动上报指纹（P2P_KEY_REPORT）；撮合时若某一方指纹
  尚未就绪，服务端先按需索要（P2P_KEY_REQUEST），**3 秒内仍未就绪则降级为无指纹下发**
  （向后兼容旧服务端/客户端，仅记 warn）。旧实现是"明文交换 + 无认证"的已知取舍，
  此处即其收口。P2P 包格式（P2P1|）与 Protocol.VERSION 均未变。
  **残留风险（既有，非本次引入）**：控制信道的 HELLO 不校验凭证，clientId 为自报 ——
  能连上控制端口者可冒用他人身份。故本机制的保证是"公钥属于服务端认为的那个 clientId"，
  而非"属于玩家本人"；彻底收口需控制信道身份认证（独立议题）。
  另：双方指纹未参与 HKDF 派生（无转录绑定），因每次打洞都用新临时密钥对，实际影响可忽略。
- **失败降级**：打洞失败 → 走中转端 RELAY_FORWARD 转发（若服务端配置允许中转）。

## 8. 服务端行为（server 包）

- `P2PServerService`：SERVER_STARTED 时启动 —— 计算环境哈希 → 启动控制端口 →
  注册全部消息处理器（env / registry / compute / relay 协助 / 日志）。
- **世界 tick 挂起**：`ServerLevelMixin` 在 `mode==SERVER && suspendWorldTick` 时于
  `ServerLevel.tick` HEAD 取消演算，**但保留 `getChunkSource().tick(hasTimeLeft, false)` 与
  `entityManager.tick()` 两个服务面调用**（区块发包/加载与实体装载），玩家可正常登录。
  （1.20.4 中区块发包、DistanceManager ticket 更新与实体分区加载都在 `tick` 内部，
  整体取消会让玩家永久卡在 "Loading terrain"，故必须保留这两个调用。）
- **出生点常加载禁用**：跳过 `MinecraftServer.prepareLevels` 的出生区块加载。
- 日志收集：主客户端定期上传，存 `player-to-player/logs/<玩家名>-latest.log`。

## 9. 线程模型

- Netty 事件循环：boss 1 + worker N（`ThreadPools` 统一命名、守护线程）。
- `ThreadPools.io()`：文件扫描/哈希/磁盘 IO，**固定线程数 + 无界队列**（有界）；
  `ThreadPools.scheduler()`：心跳、注册表落盘等定时任务；
  `ThreadPools.compute()`：跑分等 CPU 密集任务；
  `ThreadPools.startBlocking(name, body)`：**长驻阻塞**任务（每通道接收循环、隧道泵、
  无限重试的断连清理/退出发布/加入与合并工作流）专用单线程；
  `ThreadPools.exitBarrierExecutor()`：EXIT_BEGIN 屏障任务（4 线程，与 io 池隔离）；
  `ThreadPools.hostSessionExecutor()`：主客户端接待入站 P2P 会话（4 线程，与 io 池隔离）。
- **铁律**：任何阻塞操作不得占用 MC 主线程 / Netty 事件循环线程；
  与 MC 世界状态交互一律回主线程（`server.execute` / `MinecraftClient.execute`）。

### 9.1 io 池为什么有界（勿改回 cached pool）

2026-10-14 实测：服务端一次会话内 `newCachedThreadPool` 累计创建 921 条 `p2p-io-*` 线程，
随后在 Netty 事件循环上抛 `OutOfMemoryError: unable to create native thread`，玩家控制连接
被掐断、组被解散。成因是"逐区块帧 → 每帧一个 io 任务"的洪峰：cached pool 在**提交线程内联**
`new Thread().start()`，无空闲线程就新建，而每条线程 ~1 MB 栈 —— 线程数无上界就是 native OOM。

现在：固定线程数（8~32，按核数）+ 无界队列（任务排队而非新建线程/拒绝）。
不设拒绝策略是有意的：上游有预卸载屏障、释放重试等"必须最终送达"的语义，
丢帧会变成无限重试风暴。队列持续偏深时由看门狗每 30 秒采样一次、超阈值打 WARN 暴露。

### 9.2 有界池下"池内任务等待池内任务"是唯一死锁风险（改动前必读）

已核对成立、改动 io 池容量或新增"任务内等待"前必须重新核对的等待闭包：

- `EXIT_BEGIN` 的**屏障任务**必须与业务帧分池，**且"发布屏障 + 入队任务"必须是原子步**
  （`ExitTransactionHandlers.beginSubmitLock`）。两条都是实测/blocker 级教训：
  ① 屏障在 BEGIN 任务入队**之前**就对 `writeRealtimeIfAllowed` 可见，任何"在 EXIT_BEGIN
  之前到达、却排在它之后出队"的 claim/上行/释放帧都会 join 这个屏障；这些等待者一旦占满
  io 池，排在队尾的 BEGIN 任务永远拿不到线程去 `complete` 屏障 —— 固定线程池下是**永久
  死锁**（无界 cached pool 时代每条任务都新建线程，掩盖了这个结构问题）；
  ② 若发布与入队可分（同一 clientId 两条连接并发到达），屏障链会**反序**：T2 等的是只能由
  排在它后面的 T1 完成的 B1 —— 屏障执行器线程数不足即死锁，单线程执行器会立刻暴露。
  装配点：`P2PServerService` 注入 `ThreadPools.exitBarrierExecutor()`（4 线程固定、FIFO，
  既不是单线程（一条卡住的登记会冻结所有客户端的退出登记），也不是"每帧一条专用线程"
  （EXIT_BEGIN 是网络帧触发的））。
  *残留*：BEGIN 任务内有对断连清理等外部条件的无界等待（`disconnectCleanups.await`），
  极端情况下可占住若干屏障线程；disconnect 路径还会在 Netty 线程上 `awaitPendingBegins`，
  等待有上界但可能阻塞事件循环数十毫秒 —— 属既有设计，改造需连同断连状态机一起动。
- `EXIT_BEGIN` 任务自身 join 前一个 BEGIN 屏障 —— 前序任务 **FIFO 先出队先执行**，
  链头永不等待，故只要池内有 ≥1 条线程可推进即可；
- claim/release/上行任务 join 自己客户端的 BEGIN 屏障 —— 屏障由 BEGIN 任务完成，
  而 BEGIN 任务只取会话条带锁（等待方在进入业务锁**之前**等待，见
  `ExitTransactionHandlers.writeRealtimeIfAllowed` 的调用位置约定）；
- 接待入站会话（`GroupHost.accept` → 读会话头）走 `hostSessionExecutor()`（4 线程）：接待要
  阻塞等对端发会话头（最长 15 秒），放共享 io 池会占满名额饿死区块上行，而"每会话一条
  专用线程"又会被对端反复请求放大成线程无界 —— 故用小池 + 队列，并以
  `MAX_HOST_SESSIONS` 名额（含隧道生命周期，隧道终止回调归还）硬顶；
- 上游环境同步循环（首轮与定期轮次）都曾用"池内任务等池内任务"的写法，已迁到
  `startBlocking`；
- **磁盘等待仍占用本池线程**：IOWorker 队列、`flushWorker()`、Journal `force(true)` 都是
  在池内线程上同步阻塞的调用 —— 它们不依赖别的池任务才能完成（不构成死锁），但确实把
  线程占住，这正是"容量按下游串行点选"的原因；
- 断连清理、退出发布、加入/合并工作流等长驻任务一律走 `startBlocking`，**不进 io 池**。

因此容量按"下游串行点"而不是"任务总数"选：fsync、批次 WAL 锁、区块分片锁都是串行的，
线程再多也只排队。

### 9.3 "逐区块一次 fsync"的现状与真正杠杆（2026-10-14 结论）

运行期上行/释放三条路径（`CHUNK_DATA_UPLOAD`、`CHUNK_DATA_BATCH`、`CHUNK_RELEASE`）
**都在 `ChunkDataHandlers.durableBatchLock` 内完成 append + force**，即彼此天然串行 ——
所以在 `DurableChunkJournal` 内做 group commit（聚合多次 append 共用一次 force）
**对逐区块热路径没有收益**（没有可合并的重叠窗口），只会在退出/`appendExitCommit`
等本来已是整批的路径上增加风险。

两条逐区块 force 链的真实构成（`chunks.wal` 每区块一次 + `registry/mutations.jsonl`
每次释放一次）在**同一把锁内**，靠"减少临界区个数"才能合并：

1. **已否决的做法**：客户端 `ChunkUploadService` 的"单区块滞留窗口"（短等若干毫秒等更多
   区块入队再发批次）曾短暂实现后<b>撤除</b>。原因：卸载存盘的 enqueue 会被
   `claimedPredicate` 过滤（释放 ACK 已在同一临界区摘除 claim），而原版自动存盘是
   <b>同一 tick 内同步入队</b>的，工作线程醒来时批次通常已经攒好 —— 滞留省下的基本只有
   每个洪峰的首帧，却给孤立区块白加一次延迟。
2. **未落地**：把服务端的逐区块帧改成"队列 + 成批工作线程"，一次临界区内处理 K 个区块
   （含 `RegistryMutationJournal` 的第二条逐条 force 链）；需要先有并发度量（io 池队列
   深度看门狗已就位）再决定。
3. **顺带见效的一条**：世界身份修复后，干净区块的释放不再携带最终 NBT
   （`requiresInitialWrite` 不再全场为真），释放路径的 force 次数自然减半。

## 10. 兼容性原则

- 不改动原版存档格式与网络协议语义；Mixin 全部配置门控（关掉即回退原版行为）。
- 不引入第三方依赖：Netty/GSON 用 MC 自带，加密用 JDK，HTTP 用 `java.net.http`。
- Mojang 官方映射；`src/main` 不得引用客户端专用类。
- 当前控制协议版本为 **v4**：`EXIT_COMMIT` 增加 `writtenChunks[]` 并取消新事务的
  preserveExisting 区块帧；`CHUNK_RELEASE` 增加稳定 `releaseId`，新增
  `CHUNK_RELEASE_RECEIPT_CONFIRM(29)`。这是显式的不向后兼容变更，v3/v4 节点握手时
  直接拒绝混连。
- 指令路由改造（第 13.2 节）只做**加性字段**（`dimension`/`x`/`y`/`z`/`sourceUuid`/
  `origin`/`messageJson` 等）与**新增消息类型**（`PERMISSION_RELAY(72)`），不升
  `Protocol.VERSION`。但 `COMMAND_RELAY` 的语义变了（"本地失败才上送"→"全量上链"）：
  混版本时两个方向都会出错（重复执行 / 发起组漏执行），**升级时各端一起更新**
  （§13.2.1 表 7）。

## 11. Phase 2：组世界（集成服务端接管 + P2P 隧道 + 区块数据面）

### 11.1 加入流程全景

```
客户端 JOIN 物理服务端（Phase 1 会话：HELLO → 世界文件夹 → 环境同步）
  → ROLE_REQUEST(33) → 服务端 RoleAssignHandler：
      读 playerdata/<uuid>.dat 的 Pos/Dimension（无存档用主世界出生点）定位目标区块
      → 目标区块+四邻被某在线组占用 → secondary（附 primaryClientId）
      → 否则 → primary（GroupTable 建组，groupId == 主客户端 clientId）
  → ROLE_ASSIGN(31) 回到客户端：
      primary  → LocalWorldLauncher：环境骨架(level.dat 等) 复制为 data/primary/<世界名> 存档
                 → GroupRuntime.arm() → 编排性断开 → WorldOpenFlows 打开本地世界
                 → SERVER_STARTED 时 GroupRuntime.tryAttach 接管 → GroupHost 发布 LAN 端口
      secondary → SecondaryJoiner：先挂会话监听器再发 P2P_CONNECT_REQUEST(40)
                 → 打洞成功（或 P2P_USE_RELAY(43) 中转降级）拿到 P2PTransport
                 → ReliableChannel 上送会话头 {op:"tunnel_join"} → 本机 127.0.0.1 随机口
                 TCP 隧道（TcpTunnel）→ 编排性断开 → ConnectScreen 连隧道口进主客户端世界
```

- **编排性断开**：`WorldSwitcher` 的一次性切换旗标让 `WorldSession.onLeave` 跳过会话
  拆除；副客户端连隧道口触发的 JOIN 由隧道端口登记（`isTunnelAddress`）跳过。
  与物理服务端的控制连接活到组世界全程（区块申请/上行/位置上报都走它）。
- **退出时序**：主客户端先停止新 claim，并等待所有普通预卸载屏障完成，再冻结剩余
  ownership 清单。随后才允许集成服务端停止；停服 saveAll 产生的最后脏快照继续进入
  未确认集合。`SERVER_STOPPED`（detach）构建增量 bundle 后经 stopListener 补拆，
  控制连接只有在物理服确认完整退出事务后才关闭。

### 11.2 区块申请门控与数据面（规范"特殊的区块加载"）

- **世界身份（`world.WorldIdentity`）**：世界文件夹键只有 `IP+世界名`，换世界后若不比对身份，
  客户端会沿用旧本地存档、服务端会沿用旧 WAL，造成双向静默污染。身份 = level.dat 的
  `Data.WorldGenSettings`（缺失时回退 `RandomSeed`+`Generator`）递归规范化后的 SHA-256；
  刻意不含 `DataVersion`/`LastPlayed`/内嵌玩家（每局都变，算进去会每次加入都误判）。
  两处落点：**客户端** `LocalWorldLauncher.prepareSave` 身份不一致即隔离旧存档（同级改名）
  并从服务端骨架重建，重建/校验通过的结论经 `GroupRuntime.GroupPlan` 镜像到 main 源集，
  供加载门控使用；**服务端** `P2PServerService.quarantineStaleWorldState` 在世界身份标记
  （`world-identity.json`）变化时隔离 `chunk-journal/registry/exit-*`。
  判定纪律：标记缺失或身份无法确定时**只告警、不清理**（误清理会丢掉未物化到 MCA 的耐久数据）。
- **门控（ChunkMapMixin.readChunk @RETURN）**：被接管维度的每次区块读都替换为
  "先 CHUNK_CLAIM_REQUEST(20) 申请"的组合 future —— 授予+有服务端数据 →
  CHUNK_DATA_REQUEST(25) 拉取权威 NBT（走原版 `upgradeChunkTag` 升级管线）；
  授予+无数据 → 原 future（本地缓存/种子生成），**前提是 `GroupRuntime.isLocalWorldIdentityVerified()`**，
  未校验时按无缓存处理（返回空 → 种子生成），杜绝读到别的世界的区块；被拒 → future 悬置，
  `ChunkClaimClient` 每 `chunkClaimRetrySeconds` 秒重试（区块保持未加载，
  拒绝信息里的 blockingGroup 是 Phase 3 预连接/合并的入口）。
- **实时上行（ChunkStorageMixin.write @HEAD 旁挂）**：`ChunkStorage.write` 是区块落盘
  唯一咽喉（自动存盘/卸载存盘/停服 saveAll 殊途同归），凡原版写盘必上行
  CHUNK_DATA_UPLOAD(24)——与原版 unsaved 脏标记语义对齐，天然去抖；
  `ChunkUploadService` 按区块合并 + 有界队列 + 单 io 工作线程，绝不反压主线程。
- **预卸载屏障（ChunkMap.scheduleUnload HEAD）**：通过
  `ChunkHolder.addSaveDependency` 把可靠 CHUNK_RELEASE future 接入原版卸载链。
  区块先进入 PRE_UNLOAD；物理服正 ACK 前原版不能移除内存区块。服务端已有基线且
  `unsaved=false`、无未确认快照时只释放 ownership；新生成、脏或未确认区块才在主线程
  序列化最终 NBT。异常、断线、负 ACK 均保持屏障并无限重试。Fabric CHUNK_UNLOAD 只作
  防御性兜底，不再承担正常最终序列化。
- **原子写回释放**：服务端对同一 ChunkKey 的实时上行、退出写入与最终释放共用分片锁；
  CHUNK_RELEASE 在锁内严格执行“先查 `releaseId` 回执 → 最终 NBT 写回并 flush → 单条 WAL
  同时建立成功回执并删除 ownership → 解锁”。正 ACK 丢失或服务端重启后，相同操作直接
  重放原 ACK，不再次写 NBT，也不会因 ownership 已删除而永久卡住预卸载。迟到实时上传
  只能先于最终写回完成，或在释放后因所有权消失而被拒绝，不能覆盖最终态。
- **回执回收**：客户端收到正释放 ACK 后立即完成 save dependency、允许本地卸载；随后在
  后台可靠重试 `CHUNK_RELEASE_RECEIPT_CONFIRM(29)`。确认不增加卸载 RTT；服务端在内存删除
  回执并随周期 checkpoint 批量落盘，避免每次卸载增加第二次 fsync。服务端在释放请求进入
  IO 队列前按 `releaseId` 计数；即使 confirmation 已到达，也必须等该 ID 的所有已接收请求
  （包括仍在排队的旧重试）排空后才真正删除回执，防止迟到请求误释放同组重新 claim 的新区块。
  确认删除前崩溃最多让已无害回执暂时复活，重复确认仍成功；整组退出/断连也会统一清理。
  checkpoint 按“WAL 屏障 → ownership 快照 → 回执原子快照 → 清 WAL”落盘，覆盖双文件崩溃窗口。
- **MCA 单区块写回（服务端 ChunkDataHandlers）**：不自实现 MCA 格式，直接走服务端
  自身存储栈 `level.getChunkSource().chunkMap.write(pos, tag)`（IOWorker 线程安全，
  region 内单区块定位写入）。校验：占用组核对、解压上限 64MB、xPos/zPos 一致、
  DataVersion 不高于本服。**物理服务端自身存盘已取消**（ChunkMapMixin.save @HEAD，
  SERVER 模式 + suspendWorldTick 门控）：登录期加载的陈旧内存区块永不写盘，
  MCA 唯一写入来源收敛为客户端上行，消灭覆盖竞态。
- **位置上报**：集成服务端每 20 tick 把组内全部玩家（主 + 副）的 uuid/名字/维度/坐标经
  PLAYER_POS_UPDATE(32) 上报服务端玩家表；服务端按"本组成员自洽"校验后才写入（第 13.2 节
  的身份绑定；名字字段用于覆盖离线/开发环境下集成服派生 UUID 与档案 UUID 不一致的情形）。

#### 11.2.1 服务端数据基线与增量退出

- 每个 claim 保存 `hasServerData` 基线；任意实时 CHUNK_DATA_UPLOAD 正 ACK 后转为已知。
  实时上行按对象版本确认，旧 ACK 不能清除同键后来产生的新快照。
- 正常退出的 `ExitManifest.chunks` 仍是完整 ownership 清单，用于原子整组释放；
  `ExitBundle.writtenChunks` 只包含最后一次物理服确认之后仍未确认的区块。干净清单项
  隐式保留服务端现状，不再发送 preserveExisting 帧，也不在服务端逐项读 region。
- `EXIT_COMMIT` 持久化排序后的 `writtenChunks[]`。服务端要求本轮 received 集合与声明
  集合完全相等，只读取并应用这些暂存文件，全部区块完成后统一 flush 一次，再释放完整
  manifest。事务在 committed 后崩溃时可按该计划准确恢复。
- 旧事务 JSON 缺少 `writtenChunks` 时进入 legacy disposition，继续识别旧 data/marker
  暂存格式；在线协议不兼容，因此 `Protocol.VERSION=4`。
- 退出统计日志记录 `ownedChunks`、`writtenChunks` 和最终压缩字节数，便于确认耗时是否
  随脏区块量而非 ownership 总量增长。

### 11.3 P2P 隧道传输栈

```
MC 原版 TCP 协议字节
  └ TcpTunnel        本机环回 socket 双向泵（副端 listen 127.0.0.1 backlog=1 / 主端 connect LAN 口）
    └ ReliableChannel ARQ 可靠流：DATA/ACK/FIN，1200B 分片，发送窗 256，
      接收缓冲 4MB 流控（扣住 cumACK 反压对端），RTO 400ms~4s 指数退避
      └ P2PTransport   直连 P2PChannel（UDP + AES-256-GCM）或 RelayClient（中转转发），上层透明
```

- **中转降级编排（P2PBrokerHandlers）**：按 sessionId 跟踪撮合，任一方 P2P_RESULT(42)
  失败且中转可用（与 HELLO_ACK 同口径的端点计算）→ 向双方发 P2P_USE_RELAY(43)，
  双方经 `RelayConnector` 共享中转连接各建 RelayClient（同 sessionId）；
  `P2PSessions` 的会话监听器对直连/中转一视同仁地回调"会话就绪"。

### 11.4 Phase 2 已知边界（Phase 3+ 收口）

- ~~角色指派固定"既有主客户端不动摇"~~ → Phase 3 已收口：算力更强者加入指派为主，
  合并流程随区块申请自动触发（见 12.2）；
- 游离实体（poi/、entities/ 独立存储）不跨端同步 —— 方块实体（箱子等）在区块 NBT 内不受影响；
- ~~玩家背包在组世界内由主客户端存档保存，不回传物理服务端~~ → Phase 3 已收口：
  PLAYER_DATA_UPLOAD/REQUEST 数据面 + 预同步 PLAYER 记录（见 12.3/12.5）；
- ~~副客户端渲染区域与组无重叠 10s 的分离（split）未实现~~ → Phase 3 已收口（见 12.5）。

## 12. Phase 3：合并与分离（预连接→预同步→原子切换）

### 12.1 协议扩展（80-89 段）

```
MERGE_REQUEST(80)   C→S 合并申请（区块被拒触发；targetGroupId + blockingChunk）
MERGE_PLAN(81)      S→C 合并计划（算力分配选出 newPrimary；双方主客户端各一份）
MERGE_PROGRESS(82)  C→S 阶段回报（presync_started/snapshot_done/caught_up/switched/failed）
MERGE_ABORT(83)     S→C 中止（任一方失败/掉线/超时；A 继续运行）
MERGE_COMMIT(84)    S→C 提交（新组最终成员视图；副客户端据此重定向隧道）
SPLIT_REQUEST(85)   C→S 分离申请（主客户端检测达标后发；携带迁移区块清单）
SPLIT_ACK(86)       S→C 分离确认
PLAYER_DATA_UPLOAD(87) / PLAYER_DATA_REQUEST(88) / PLAYER_DATA(89)  玩家数据面
```

### 12.2 合并全景（server.MergeCoordinator + client.MergeClient）

```
A 组主客户端区块申请被拒（blockingGroup）
  → MergeTriggers（去抖 + 分离后抑制窗口）→ MERGE_REQUEST
  → MergeCoordinator：在线校验 + 冷却 + 忙碌互斥 → ComputeTable.selectPrimary
    （单核分 + 内存门槛 + UUID 决胜 —— 与 RoleAssignHandler 完全同口径）
  → MERGE_PLAN 下发双方 → 让出方 A（oldPrimary）/接管方 B（newPrimary）分工：
      A：P2P 预连接（P2P_CONNECT_REQUEST，失败自动中转降级）
         → 会话头 op=presync → PresyncSender
      B：GroupHost 按会话头 op=presync 分派 → PresyncReceiver
  → A 回报 switched → 服务端 GroupTable.mergeGroups + ChunkRegistry.migrateAll
    （原子改挂，无"释放-再申请"空窗）→ MERGE_COMMIT 广播新组
  → A 关停集成服务端（suppressNextStopTeardown 保住控制连接）
    → SecondaryJoiner 经隧道加入 B（Phase 2 同一条管线）
  → 原 A 组副客户端收到 MERGE_COMMIT 后主动向 B 发起 P2P 重定向
```

- **触发面二**：算力更强者加入世界（RoleAssignHandler 指派其为主 → 其区块申请
  必然被占用组挡住 → 走上面同一条链路），规范"加入玩家算力更强则合并"零专用协议。
- 失败语义（规范）：任何阶段失败/超时（300s）/当事方掉线 → MERGE_ABORT 双方，
  A 解冻继续运行；组对冷却 30s 防重试风暴。

### 12.3 预同步（group.PresyncSender / PresyncReceiver / PresyncProtocol）

- **对规范"影子实例"的工程取舍**：客户端 JVM 同时只能有一个集成服务端（原版单例
  结构），真起第二实例需类加载器隔离，脆弱且吃内存。改用**数据等价**方案：区块级
  全量覆盖幂等（同键后到覆盖先到），"重放到影子世界"与"暂存最新 NBT、接管后按需
  装载"最终态一致 —— B 的集成服务端全程不中断，接管即生效。
- 流程：**先挂增量分接（PresyncTaps，寄生在 ChunkUploadService.enqueue 咽喉）再拍
  快照**（快照期写盘不漏）→ 快照逐区块主线程序列化（微秒级/块，不卡游戏）→
  SNAPSHOT_DONE/ACK → 增量追赶循环至吃空 → **冻结**（GroupRuntime.setTickFrozen，
  ServerLevelMixin 挂起演算但保留区块服务）→ 主线程 saveAllChunks（脏区块经写盘
  咽喉产生最终增量）→ 尾部 + 玩家 NBT → TAIL_DONE/ACK → switched。
  冻结窗口 = 存盘 + 尾部传输 + 一次 ACK 往返（规范目标 300ms 量级）。
- B 侧入库：区块 → PresyncStore（4096 上限，超限丢弃回退服务端拉取）；玩家 →
  B 本地存档 playerdata（原子写）。ChunkMapMixin 门控授予后**优先消费暂存**
  （与服务端存档同源，省一次往返 —— 规范"继承"减轻服务端带宽）。

### 12.4 会话复用与所有权（P2PSessions.initiator 标记）

SessionListener 增加 initiator 参数（P2P_USE_RELAY 也随帧下发）：主客户端同时
是"被动接待方"（GroupHost，只认非发起会话）与"主动发起方"（MergeClient 预连接，
只认自己发起的），两类监听器按标记分流，互不误抢。

### 12.5 单端分离（group.SplitMonitor）

- 判据：规范"渲染区块的周围4格的周围4格无交集持续 10s" ≡ 两渲染矩形曼哈顿间隙
  > 2（O(1) 矩形运算，GroupServerHooks 每秒驱动一轮）；交集恢复即计时归零。
- 达标：主客户端上传离组玩家 NBT（PLAYER_DATA_UPLOAD）→ SPLIT_REQUEST（携带其
  渲染区内本组占用区块）→ 服务端 GroupTable.splitGroup + ChunkRegistry.migrate
  （定向迁移）→ 直发离组客户端 ROLE_ASSIGN(primary) → 其 WorldSession 处理主动
  推送的指派（不带 _rid，与 request 应答路径天然不冲突）→ LocalWorldLauncher
  切本地组世界；原主 forgetLocal 已迁移区块 + MergeTriggers.suppressFor(30s)
  防分离↔合并震荡。
- **简化（对规范"10s 期间预同步"）**：离组者区块数据由服务端供给（原主实时上行
  保证新鲜），一致性等价；代价是一次本地世界加载（数秒）。组分离（多端同时离组）
  见第 13.5 节（Phase 4 已实现，单端分离成为其单人分量特例）。

### 12.6 注册表迁移原语（ChunkRegistry）

- `migrateAll(from, to)`：整组占用原子改挂（合并提交用）—— 相比"释放再申请"
  不存在第三组趁隙抢占的空窗；
- `migrate(keys, from, to)`：定向迁移（分离用），非 from 组的条目静默跳过（弱一致容忍）。

## 13. Phase 4：指令/聊天路由、特殊加载、组分离、日志与 MySQL

### 13.1 协议扩展（只增不改号）

```
CHUNK_PROBE_REQUEST(27) / CHUNK_PROBE_RESPONSE(28)
    只读归属探测：与 tryClaim 同一套"目标+四邻缓冲"检查但不登记占用。
    传送门预检与珍珠交接判定共用（预申请会泄漏永不释放的占用，探测无副作用）。
GROUP_WORLD_READY(34)   C→S 主客户端组世界完成 LAN 发布的就绪通告
COMMAND_RELAY(70)       指令全量上链 / 转发执行 / 聚合回执（三种载荷靠 JSON 字段区分，见 13.2）
CHAT_RELAY(71)          聊天全网分发 / 跨组私聊（privateTo）
PERMISSION_RELAY(72)    S→C 权限下发（组成员 → ops 真实等级；本地权限位落地用，见 13.2）
PEARL_HANDOFF(90)       珍珠交接（json 带目标组/维度/抛出者；binary 透传 gzip NBT）
PEARL_LANDED(91)        珍珠落点回报（回程给抛出者所在组，落地=小型 tp）
```

### 13.2 指令路由（client.mixin.CommandDispatchMixin + group.CommandRelayClient + server.CommandChatHandlers）

口径（2026-10-15 定稿）：**全量上链 + 服务端唯一裁定 + 分组执行**。
规范链路"副→主→中转服务端→服务端→其他的主客户端→其他的组客户端的副客户端"即由
这条链路承载（中转端不持业务状态，控制连接本就可经中转桥接，链路上它是透明的）：

```
玩家（主/副客户端）在聊天栏敲指令
  └ 集成服务端 ServerGamePacketListenerImpl.performChatCommand（原版签名校验之后）
    └ CommandDispatchMixin：本地执行那次 Commands.performCommand 调用前拦截
      → 无条件上链，本地不执行（副客户端的指令经隧道汇聚到主客户端集成服，同一入口）
        COMMAND_RELAY 上送（playerUuid / playerName / command / dimension / x,y,z）
        └ 服务端 CommandChatHandlers（权限唯一裁定的地方）：
            ① 身份自洽（ConnectionIdentity：必须是本连接所在组的成员）
            ② 以发起者真实 ops 等级解析原版指令树（requires 由原版裁定；
               解析不通过 → 回原版报错文案"在原版中不合理的指令是不会被执行的"）
            ③ 三个特例：/list（玩家表答全网玩家）、/tp <玩家名>（玩家表解析出坐标，
               由发起端本地 teleportTo）、/msg <玩家名> <文本>（跨组私聊投递）
            ④ 3/4 级指令（2 级权限下解析不通过）→ 在服务端执行，结果文案回发起端
            ⑤ 0-2 级指令 → 形状规则 + 归属预检决定目标组，转发执行（权限钳到 2）
          └ 目标组主客户端执行一次：发起组用【发起玩家实体】当指令源，
            其余组用虚拟源（该维度出生点、entity=null）
            └ COMMAND_RELAY 应答（executed / message）
      └ CommandAggregator 聚合：首成功 → 执行端的消息；全失败 → 发起组那次的报错
        → 发起端把回执显示给玩家（action=teleport 时改走本地传送）
```

- **拦截点为什么选数据包入口**：只有"玩家敲的指令"经过 `performChatCommand`
  （聊天栏与 F3+F4/F3+N 同一条路）。告示牌、命令方块、数据包函数与第三方程序化调用
  不经过它 —— 它们照旧在本地按服务端下发的真实权限位执行（第 13.2.2 节）。
  注入位置在**本地执行那次调用点**而非方法 HEAD：原版的签名/LastSeenMessages 校验
  与失败处理照常执行，取消的只是本地执行这一步。
- **形状规则**（`group.CommandRouting`，判定全部从解析树自动得出，不维护指令表）：
  凡引用<b>执行上下文</b>的指令 —— `@s`/`@p`/`@r`、相对/局部坐标（`~`/`^`）、
  以及缺省位置或缺省目标（`/summon zombie`、`/gamemode creative`、裸 `/kill`）——
  <b>只在发起组执行</b>，且指令源是发起玩家实体（原版语义完整保留）；
  其余指令在所有组各执行一次（用户口径："一条指令的语义 = 全网各执行一次"）。
- **归属预检（挂死守则）**：指令影响的<b>区块包围盒</b>必须整体落在同一个组手里，
  才发给那个组（`ChunkRegistry.ownerOf` + `server.CommandOwnershipRouter`）。
  理由是实测的冻结机制：集成服务端里 `Level.getBlockState` 等路径会在主线程
  `managedBlock + join` 等区块加载，而"本组不持有、被其他组占用"的区块其加载 future
  永远悬置（门控无限重试）⇒ 主线程<b>永久忙等</b>（集成服没有 watchdog）；
  组内玩家视野里的区块本来就有 PLAYER 票，所以"执行到别人的区块"随时可复现。
  四条分支：
  ① <b>单一持有组</b> → 只发它；② <b>无人持有</b> → 交回发起组按需申请（与原版本地执行
  同一条加载门控），但先"预演认领"：若包围盒里有<b>本组认领不了</b>的无主区块
  （侧邻属他组的死区块）就拒绝 —— 那种区块对全网都不可认领，执行必然忙等；
  ③ <b>跨多个组</b> → 拒绝（没有任何一个组能整体执行）；
  ④ <b>包围盒过大</b>（> 8192 区块）→ 拒绝（防"一个巨大 /fill 把预检变成 DoS"）。
  **包围盒而不是逐点**：`/fill`、`/clone` 只声明角点、`/place jigsaw` 还会向外延伸，
  内部区块不属于任何实参 —— 只查角点会漏掉"角点在本组、内部在他组"的形态。
  相对/局部坐标（`~`/`^`）由服务端用<b>发起者上报的位置与朝向</b>结算成绝对方块坐标，
  所以"<b>引用执行上下文</b>的指令"同样要过这道预检（越界时干净拒绝，
  而不是让发起组就地冻结）。
  接收端在执行前再按服务端核过的持有区块清单（`ownedChunks`）复核一次，
  兜住"服务端预检 → 实际执行"之间的归属变化（合并/分离/退出期间）。
- **身份归一**：上链的 `playerUuid`/`playerName` 必须<b>配对命中</b>本连接所在组的
  某个具体成员（`server.ConnectionIdentity.canonicalMemberId`），并归一到该成员的
  档案 UUID：集成服里经隧道加入的副客户端实体 UUID 是<b>原版离线派生值</b>，
  直接按上报 UUID 落行会一人两行（`/list` 重复计数、`/tp` 落到 0,0,0 的幽灵行）；
  按名字兜底时要求"上报 UUID 恰是该名字的离线派生值"，不允许把 A 的 UUID 与 B 的
  名字拼起来用（否则组内任意成员都能借外部 op 的 UUID 换取 3/4 级执行）。
- **维度核对**：发起组执行前比对"玩家此刻所在维度"与上链时自报的维度，不一致即拒绝 ——
  否则越界预检查的是另一个维度的注册表，等于没查（穿传送门或谎报维度都能绕过）。
- **混版本能力位**（加性协议字段 `commandRouting`）：客户端 HELLO 声明该位、
  服务端 HELLO_ACK 回带该位；服务端只按新语义处理声明过的连接的上送（老客户端
  只会上送"本地失败"的指令且本地已执行过 ⇒ 忽略即可，避免重复执行），
  客户端只在 ACK 里看到该位才拦截本地执行（老服务端 = 保持老行为，不会漏执行）。
- **权限两套口径**（不可混，第 13.2.2 节）：
  ① <b>本地权限位</b> = 服务端 ops 名单的<b>真实等级</b>（`PermissionRelay` 下发、
     `LocalPermissions` 落到该组主客户端的集成服务端）；
  ② <b>跨组广播执行的权限</b> = `min(真实等级, FORWARD_PERMISSION_CAP=2)`。
- **回执聚合**（`server.CommandAggregator`）：首个成功即回"执行端的消息"；全部失败回
  发起组那一次的报错（发起组也在执行集合里，它的失败文案就是规范说的"本地的报错信息"）。
  执行侧输出统一经 `group.CommandFeedback` 收集回传、**不直发玩家** —— 玩家看到的输出
  只有这一条回执；代价是本地报错从"立即显示"变为"回执后显示"（每条指令一次 RTT）。
- **错误文案本地化**：解析类报错（未知指令/权限不足）随回执附带可本地化组件 JSON
  （`messageJson`），玩家端显示的语言与真人原版一致。

#### 13.2.1 边界与已知取舍（改这条链路前先读）

| # | 边界 | 说明 |
|---|------|------|
| 1 | 选择器只在本组解析 | `@a`/`@e` 类指令在各组各解析一次（符合"每组各执行一次"）；跨组选择器（"全网最近的玩家"）不实现，`@s`/`@p`/`@r` 归发起组。**规范剩余的"选择器跨组"差距在此明确记为范围外** |
| 2 | 指令集以物理服务端的树为准 | 集成服专属指令（如 `/publish`）不在物理服树里 → 上链后被判"未知指令"并回原版报错 |
| 3 | 3/4 级指令的组侧副作用未实现 | `/save-all`、`/stop`、`/reload` 目前只有服务端侧效果 + 回执；"通知各组刷盘/有序退出/重载"属独立子系统（后续阶段） |
| 4 | 3/4 级指令真的会在服务端生效 | `/stop` 会停物理服务端、`/ban`/`/op` 会改名单 —— 这是定稿口径（"3/4 级指令在服务端执行"）；3/4 级指令绝不跨组 |
| 5 | 客户端半可信 | 身份绑定只保证"同一连接自洽"（UUID/名字属于本组），挡不住被改造的主客户端冒用本组他人身份；权威只有物理服务端的权限裁定期 |
| 6 | 落权是纯内存、按会话 | 集成服从不读 `ops.json`（`PlayerList` 构造器不 `load`）；`StoredUserList.add` 会顺手把文件写到进程 CWD，但没有任何回读路径（无害副作用），世界关闭即随实例销毁 |
| 7 | 混版本节点安全降级（不重复执行、不静默漏执行） | 协议只做加性字段、不升 `Protocol.VERSION`：新增能力位 `commandRouting` 双向声明 —— 服务端只按新语义处理声明过的连接的上送（老客户端上送被忽略，它本地已执行过），客户端只在 ACK 里看到该位才拦截本地执行（老服务端 = 老行为）。代价是混版本期间"跨组下放"能力暂时缺失（照旧只在本组执行）；建议仍各端一起更新 |
| 10 | `/spreadplayers` 的散点半径不参与预检 | 其中心点已计入（XZ 坐标），但 `spreadDistance`/`maxRange`（上限 200 格）决定的散点区域无法从参数统一定界 —— 中心在自家领地但半径够到邻组领地时仍可能触发忙等。已知残余；要收口需按指令特判参数名 |
| 11 | `/place jigsaw` 的延伸范围不参与预检 | 拼图结构会向四周延伸数个区块，且原版该子命令没有 `isLoaded` 守卫（其余 `place` 子命令都有）。预检只覆盖 `pos` 所在区块，属已知残余 |
| 12 | 控制信道无玩家级认证（半可信模型的根） | HELLO 的 `clientId` 是自报值：能连上控制端口者可以声称自己是某个 op（会顶掉其在线连接，属既有行为）。身份绑定的所有判据都建立在"同一个自报 clientId"之上，只能保证<b>同一连接自洽</b>。⇒ 一个被改造的客户端仍可冒充 op 让 `/stop`、`/ban` 在物理服务端真实生效（本批把 3/4 级指令改成服务端执行后后果变大）。彻底收口需要控制信道的玩家级认证（Mojang 会话校验），属独立议题；当前部署须把控制端口当作<b>与服务器控制台同权</b>的入口来防护 |
| 8 | 限速 | 客户端上链入口与服务端上链入口各一道令牌桶（突发 12/20、约 4 条每秒，单调钟）；超限回明确文案，不静默丢弃 |
| 9 | 相对坐标的维度重定向 | `/execute in <维度> run <绝对坐标指令>` 按上下文处理（只在发起组执行）：绝对坐标属于哪个维度无法从本地解析判定，保守收在发起组 |

#### 13.2.2 本地权限位与权限下发（PermissionRelay → LocalPermissions）

组世界的权威演算是主客户端的集成服务端，而它发布时不开放作弊
（`GroupHost` 的 `publishServer(…, false, …)`，有意决策："组世界的权威演算不应被本地
作弊指令干预"），集成服又从不读取 `ops.json` —— 于是玩家的本地权限恒为 0：
F3+F4 被客户端 `hasPermissions(2)` 闸门静默拦下、指令补全树按权限过滤后连 `/gamemode`
都看不到。修复是**按玩家、按服务端 ops 名单精确落权**，不是开全局作弊：

- 服务端 `PermissionRelay` 按"组 → 组成员"核出真实等级（`MinecraftServer
  .getProfilePermissions`，组成员 clientId 即玩家档案 UUID），下发给该组主客户端；
  触发时机 = HELLO 重连、角色指派、合并提交、分离，外加每 5 秒一次变化 diff
  （覆盖控制台 `/op`、直接改 ops.json 等绕过模组的路径），只在变化时发包；
- 主客户端 `LocalPermissions` 在自己的集成服里落权：
  `PlayerList.getOps().add(new ServerOpListEntry(profile, 真实等级, false))` +
  `sendPlayerPermissionLevel(player)`（同一次调用既发权限事件包、也重发指令树），
  等级 0 则移除条目而不是留一个"0 级 op"。全部在集成服主线程做
  （`ServerOpList` 内部是非并发 HashMap），落点：LAN 发布后、玩家 JOIN、
  名单变化时；`GroupHost.reset` / `GroupRuntime.detach` / `disarm` 清缓存。**不做**：
  改 `level.dat` 的 `allowCommands`、`setAllowCheatsForAllPlayers(true)`（都违反上述决策）。

### 13.3 消息处理（group.ChatRelay）

- 出站：ServerMessageEvents.CHAT_MESSAGE（仅被接管集成服务端）→ CHAT_RELAY 上送
  → 服务端广播给**其他组**主客户端（发起组不回发，天然无环）。
- 入站：以 `<名字> 内容` 系统消息在本组集成服务端广播（原版链路自然下发副客户端）。
  跨组重放不用签名聊天：1.19+ 聊天签名与玩家会话绑定，跨服务端无法复现（所有
  跨服聊天桥的通行做法）；系统消息不触发 CHAT_MESSAGE 事件，回环免疫。
- 跨组私聊（/msg 特例）：服务端解析目标 → 目标组主客户端收 privateTo 载荷定向投递；
  正文按**解析结果的实参区间**提取（旧实现用 `indexOf` 定位目标名，目标名与指令字面量
  同形时会截错，如 `/tell tell hi`）。

### 13.4 传送门与末影珍珠（mixin.EntityPortalMixin / ThrownEnderpearlMixin）

- **传送门（PortalPreloader）**：跨维度加载的正确性本就由门控保证（ChunkKey 含
  维度，目的地区块照常走申请-授予；产生加载的组承担演算直到卸载/退出 —— 即
  规范语义）。Phase 4 补的是"提前"：注入 Entity.handleInsidePortal（下界门 4s
  等待窗口的起点），按 coordinateScale 换算目的落点，CHUNK_PROBE 探测 3×3 区块，
  被他组占用 → 直接打 MergeTriggers.onClaimBlocked（与区块被拒同一入口），
  等待窗口给预连接/预同步当缓冲。末地门无等待窗口，由门控兜底。
- **末影珍珠（PearlHandoff）**：规范四规则的判定 —— 珍珠下一 tick 位置（pos+
  velocity）不属本组 → CHUNK_PROBE（四邻检查天然覆盖"未加载但在他组缓冲层"）：
  被他组占用/缓冲 → 交接（先 PEARL_HANDOFF 得到对方 handedOff=true 确认再
  discard，杜绝珍珠凭空消失）；自由区块 → 抛出者保留（原版：未加载则暂停）。
  抛出者身份用命令 Tag（`p2p_pearl_owner:<uuid>`）随 NBT 跨端携带；接收方注入
  时重掷 UUID 防撞车并打 `p2p_remote_pearl` 记号。远端落地 → onHit 取消原版 +
  PEARL_LANDED 回程 → 抛出者组主客户端 teleportTo 落点 + 5 点摔落伤害
  （规范"落地后看做一个小型 tp"）。整套目的：不为一颗过路珍珠触发合并。

### 13.5 组分离——多端（SplitMonitor 聚类 + MergeCoordinator 暂存重定向）

- 判据升级为**连通分量**：``同维度且曼哈顿间隙 ≤ 2`` 为连通边，并查集划分全组
  玩家；不含主客户端的分量整体离组（单端分离=单人分量特例，同一条代码路径）。
  分量**成员构成**（UUID 排序签名）持续 10s 不变才触发，成员进出即计时归零。
- 触发：逐人上传 NBT → SPLIT_REQUEST 携带 departingClientIds[] + 迁移区块并集
  → 服务端在离组集合内 ComputeTable.selectPrimaryOrRelaxed（规范"每个组内进行算力分配"，
  与合并选主同口径：内存门槛 + rankScore 排序 + UUID 决胜，全员不达门槛时确定性降级；
  无一人上报算力才按 UUID 字典序兜底）→ splitGroup + migrate →
  新主直收 ROLE_ASSIGN(primary)。
- **暂存重定向**：其余离组成员的 ROLE_ASSIGN(secondary) 不能立发（新主组世界
  未起，隧道必然扑空）——服务端暂存至新主 LAN 发布后上报 GROUP_WORLD_READY
  再冲刷；60s 未就绪超时兜底冲刷（副客户端侧自带失败留守，宁可尝试）。

### 13.6 日志收集（group.LogUploader + server.LogCollector）

- "所有端都有 latest.log"由原版 log4j 自身保证；主客户端接管期间每 30s 增量
  尾随 logs/latest.log 上行（LOG_UPLOAD），服务端落 `logs/<玩家名>-latest.log`。
- 首帧 append=false 截断旧会话并只回溯最近 1MB；文件变短视为轮转重置游标；
  单帧 ≤512KB、每轮 ≤2 帧，积压分轮追平不独占 io 线程。

### 13.7 MySQL 注册表后端（registry.RegistryStore 抽象）

- 内存表恒为权威，后端只负责持久化：`RegistryStore { load / save(snapshot) }`，
  JsonRegistryStore（默认，原逻辑原样抽出）与 MysqlRegistryStore 可插拔。
- **不捆绑 JDBC 驱动**（第 10 节"不引入第三方依赖"）：DriverManager 走
  ServiceLoader 发现服务端环境里已有的 mysql-connector-j；连接失败启动时即回退
  JSON（注册表可用性优先于后端偏好）。表 `p2p_chunk_claims`（PK 维度+x+z），
  save=事务内全删全插（30s 低频快照，行数为活跃占用数量级，简单正确优先）。
