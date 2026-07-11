# player_to_player 架构设计文档

> 本文档是 `player_to_player-prompt.txt`（需求规范）的**工程落地设计**。
> 规范定义"做什么"，本文档定义"怎么做"。所有子系统实现必须与本文档一致。

## 0. 总体判断与分期路线图

完整规范（影子实例热迁移、预同步、MCA 服务端写回、指令逐级路由等）体量极大，
按依赖关系分四期实现，每一期都保持可编译、可运行：

| 期 | 内容 | 状态 |
|----|------|------|
| **Phase 1** | 基础设施：角色/配置/目录、控制协议(Netty)、环境文件扫描与同步、区块注册表、算力评分、NAT 检测与 UDP 打洞、服务端/中转端服务骨架、客户端引导、服务端世界 tick 挂起 | 已完成 |
| **Phase 2** | 主客户端集成服务端接管、副客户端经 P2P 隧道加入主客户端、区块数据上行与 MCA 单区块写回、中转端环境分发 | **本期实现**（详见第 11 节） |
| Phase 3 | 预连接→预同步→合并（影子实例 + 快照 + 增量追赶 + 原子切换）、单端/组分离、算力再分配 | 未开始 |
| Phase 4 | 指令/聊天逐级路由、传送门跨维度加载、末影珍珠特殊加载、日志收集完善、MySQL 注册表后端 | 未开始 |

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
  registry/                      # [服务端] 区块注册表持久化（每维度一个 json）
  logs/                          # [服务端] 收集的主客户端日志（<玩家名>-latest.log）
  <IP>+<世界名>/                 # [客户端] 世界文件夹（每个"服务器+世界"一个）
    config.json                  # 世界配置 WorldConfig
    primary/                     # 主客户端角色专属
      environment/               #   环境文件
      data/                      #   数据文件
    secondary/                   # 副客户端角色专属
      environment/
      data/
```

- 世界名/IP 经 `P2PPaths.sanitize()` 做文件名合法化（Windows 保留字符）。
- 客户端每次加入世界时重新选定世界文件夹（同一客户端可进出不同服务器）。

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
- 持久化：`player-to-player/registry/<维度>.json`，定期 + 关闭时写盘；MySQL 后端留接口（Phase 4）。
- `PlayerTable`：玩家 UUID → 名字/维度/坐标/组/端级别，供跨组 tp 指令与消息路由（Phase 4）使用。

## 6. 算力（compute 包）

- `ComputeScore(cpuModel, singleCoreScore, source, freeMemoryBytes, totalMemoryBytes)`。
- 评分来源：① Geekbench 非官方公开接口按 CPU 型号查询单核分（可配置关闭、失败降级）；
  ② 本地单核微基准（确定性工作负载跑分，映射到 Geekbench 量级）。
- 主客户端资格 = 算力最高 **且** 可用内存 ≥ 服务端配置阈值（默认 0.5 GB）。
- 服务端 `ComputeTable` 汇总所有在线客户端评分，合并/加入世界时据此选主。

## 7. P2P（p2p 包）

- **NAT 检测**：极简 STUN 客户端（RFC 5389 Binding Request，公共 STUN 服务器 + 中转端内置 STUN），
  区分 OPEN / FULL_CONE / RESTRICTED / PORT_RESTRICTED / SYMMETRIC / UDP_BLOCKED。
- **打洞流程**：A、B 均与中转端保持控制连接 → 中转端交换双方公网 endpoint（P2P_ENDPOINT_EXCHANGE）
  → 双方同时向对方 endpoint 发 UDP 探测（指数退避，约 5s 超时）→ 成功后进入加密会话。
- **加密通道**：X25519 ECDH 握手（JDK XDH）派生 AES-256-GCM 会话密钥；帧内嵌递增 nonce 防重放。
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
- `ThreadPools.io()`：文件扫描/哈希/磁盘 IO；`ThreadPools.scheduler()`：心跳、注册表落盘等定时任务；
  `ThreadPools.compute()`：跑分等 CPU 密集任务。
- **铁律**：任何阻塞操作不得占用 MC 主线程 / Netty 事件循环线程；
  与 MC 世界状态交互一律回主线程（`server.execute` / `MinecraftClient.execute`）。

## 10. 兼容性原则

- 不改动原版存档格式与网络协议语义；Mixin 全部配置门控（关掉即回退原版行为）。
- 不引入第三方依赖：Netty/GSON 用 MC 自带，加密用 JDK，HTTP 用 `java.net.http`。
- Mojang 官方映射；`src/main` 不得引用客户端专用类。

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
- **退出时序**：主客户端退出本地世界时 DISCONNECT 先于集成服务端停止，而停服
  saveAll 的最终区块上行必须赶在控制连接关闭前 —— onLeave 检测
  `GroupRuntime.isArmedOrActive()` 延迟拆除，`SERVER_STOPPED`（detach）后经
  stopListener 补拆。

### 11.2 区块申请门控与数据面（规范"特殊的区块加载"）

- **门控（ChunkMapMixin.readChunk @RETURN）**：被接管维度的每次区块读都替换为
  "先 CHUNK_CLAIM_REQUEST(20) 申请"的组合 future —— 授予+有服务端数据 →
  CHUNK_DATA_REQUEST(25) 拉取权威 NBT（走原版 `upgradeChunkTag` 升级管线）；
  授予+无数据 → 原 future（本地缓存/种子生成）；被拒 → future 悬置，
  `ChunkClaimClient` 每 `chunkClaimRetrySeconds` 秒重试（区块保持未加载，
  拒绝信息里的 blockingGroup 是 Phase 3 预连接/合并的入口）。
- **实时上行（ChunkStorageMixin.write @HEAD 旁挂）**：`ChunkStorage.write` 是区块落盘
  唯一咽喉（自动存盘/卸载存盘/停服 saveAll 殊途同归），凡原版写盘必上行
  CHUNK_DATA_UPLOAD(24)——与原版 unsaved 脏标记语义对齐，天然去抖；
  `ChunkUploadService` 按区块合并 + 有界队列 + 单 io 工作线程，绝不反压主线程。
- **卸载释放（Fabric CHUNK_UNLOAD）**：主线程序列化最终 NBT → CHUNK_RELEASE(22)
  携带二进制 → 服务端**先写回刷盘再释放占用**（顺序保证释放后其他组的
  hasServerData 探测立刻可见）。
- **MCA 单区块写回（服务端 ChunkDataHandlers）**：不自实现 MCA 格式，直接走服务端
  自身存储栈 `level.getChunkSource().chunkMap.write(pos, tag)`（IOWorker 线程安全，
  region 内单区块定位写入）。校验：占用组核对、解压上限 64MB、xPos/zPos 一致、
  DataVersion 不高于本服。**物理服务端自身存盘已取消**（ChunkMapMixin.save @HEAD，
  SERVER 模式 + suspendWorldTick 门控）：登录期加载的陈旧内存区块永不写盘，
  MCA 唯一写入来源收敛为客户端上行，消灭覆盖竞态。
- **位置上报**：集成服务端每 20 tick 把组内全部玩家位置经 PLAYER_POS_UPDATE(32)
  上报服务端玩家表。

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

- 角色指派固定"既有主客户端不动摇"，算力更强者加入不触发主客户端迁移（Phase 3 合并）；
- 游离实体（poi/、entities/ 独立存储）不跨端同步 —— 方块实体（箱子等）在区块 NBT 内不受影响；
- 玩家背包在组世界内由主客户端存档保存，不回传物理服务端（Phase 3/4 玩家数据上行）；
- 副客户端渲染区域与组无重叠 10s 的分离（split）未实现（Phase 3）。
