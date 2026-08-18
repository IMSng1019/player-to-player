# 金织 Player to Player（P2P）

`player_to_player` 是面向 Minecraft Java 版 Fabric 1.20.4 的分布式运算模组。它保留物理服务端作为控制面和数据持久化中心，将世界区块的主要 tick 运算交给玩家客户端，并通过主客户端、副客户端和 P2P/中转连接组成可动态合并、分离的组客户端。

> 当前实现覆盖 Phase 1 至 Phase 4 的核心链路：角色分配、环境同步、区块所有权、区块数据上行与写回、P2P 打洞及中转、组世界、预同步、合并/分离、指令/聊天转发、传送门与末影珍珠交接、日志收集和可选 MySQL 注册表。
>
> 这是一个底层改动较大的实验性模组。正式使用前请备份世界，并在目标模组组合和真实网络环境中完成客户端/服务端联调。

## 特性

- **多端角色模型**：服务端、中转服务端、主客户端、副客户端按职责运行；主副角色可在合并、分离和玩家离开时重新分配。
- **客户端承担世界 tick**：服务端模式下挂起原版世界演算，同时保留区块服务和实体装载等必要服务面调用；可配置是否禁用出生点常加载区块。
- **区块唯一所有权**：服务端原子检查目标区块及东西南北四个邻区，避免多个组客户端同时写入同一片区域。
- **区块数据可靠写回**：客户端区块 NBT 通过服务端原生存储栈写回 MCA；支持脏区块上传、预卸载屏障、释放回执、WAL 和退出事务恢复。
- **环境文件同步**：服务端生成 SHA-256 环境清单，客户端只拉取差异文件；可通过中转服务端分发，且内置排除凭据和玩家管理文件。
- **P2P 打洞与安全传输**：支持 STUN/NAT 检测、UDP 打洞、X25519 密钥协商、AES-256-GCM 加密；直连失败时可降级到中转转发。
- **组世界与隧道接入**：主客户端运行本组的集成服务端，副客户端通过 P2P 或中转隧道加入，不改变 Minecraft 原版游戏连接语义。
- **预同步、合并和分离**：使用区块快照、增量追赶和冻结切换完成组间合并；按渲染区域连通性和持续时间执行单端或多端分离。
- **跨组协作**：支持指令逐级路由、聊天广播/私聊、传送门预加载、末影珍珠交接，以及主客户端日志上传。
- **算力选主**：结合 Geekbench 查询或本地单核基准、可用内存门槛和 UUID 决胜规则选择主客户端。
- **注册表持久化**：默认使用 JSON；可选用服务端已有的 MySQL JDBC 驱动保存区块注册表。

## 运行环境

| 项目 | 要求 |
| --- | --- |
| Minecraft | Java Edition 1.20.4 |
| Java | 17 或更高版本 |
| Mod Loader | Fabric Loader `>= 0.19.3` |
| Fabric API | `0.97.3+1.20.4` 或与当前构建兼容的版本 |
| 协议版本 | P2P 控制协议 v4；v3 与 v4 节点不能混连 |

服务端、客户端和中转端需要使用兼容的模组版本、协议版本以及相同的环境文件。主客户端和副客户端随时可能切换，因此客户端侧应同时准备两类角色所需的模组和环境。

## 安装

### 服务端

1. 使用 Java 17 安装 Fabric 1.20.4 服务端。
2. 将本模组 JAR 和 Fabric API 放入服务端 `mods/`。
3. 启动一次服务端，让模组生成 `player-to-player/config.json`。
4. 确认总配置的 `mode` 为 `server`，检查控制端口和可选中转配置。
5. 开放 Minecraft 原版端口，以及独立的 P2P 控制端口（默认 TCP `25580`）。如果服务端兼任中转，还需要开放默认 TCP `25581`。

服务端不再承担被接管世界的主要 tick 运算，但仍负责玩家控制面、区块注册表、环境同步、区块写回和跨组消息路由。

### 客户端

1. 使用与服务端相同的 Minecraft、Fabric Loader、Fabric API 和本模组版本。
2. 将本模组 JAR 放入客户端 `mods/`。
3. 启动客户端并进入目标服务器。客户端首次加载会自动生成 `player-to-player/config.json`，默认 `mode` 为 `client`。
4. 客户端加入世界时会提交算力和 NAT 信息，并根据服务端环境哈希自动同步差异文件。

客户端连接的仍是服务器的 Minecraft 地址；控制面使用服务端下发的独立端口，组世界内部的游戏流量再通过 P2P 或中转隧道传输。

### 中转服务端（可选）

中转端是一个 Fabric 服务端实例，配置 `mode: "proxy_server"`，并设置 `parentServerAddress` 指向上级服务端。它负责：

- 协助双方交换公网 UDP endpoint；
- 打洞失败时转发可靠数据；
- 从上级服务端同步环境快照并向客户端分发；
- 按配置周期重新校验上游环境哈希。

如果服务端设置 `serverActsAsRelay: true` 且没有独立 `relayServerAddress`，服务端自身会兼任中转端。

## 运行模式与角色

总配置的 `mode` 只有以下三种值：

| `mode` | 运行位置 | 作用 |
| --- | --- | --- |
| `server` | 物理 Minecraft 服务端 | 控制面、区块注册表、环境同步、持久化和跨组路由 |
| `proxy_server` | 独立中转 Fabric 服务端 | P2P 打洞协助、失败转发和环境分发 |
| `client` | 玩家客户端 | 加入组世界，并在运行期成为主客户端或副客户端 |

客户端运行期角色：

- **主客户端（PRIMARY）**：运行本组已加载区块的世界 tick，向服务端上传区块、玩家状态和日志，并向副客户端提供组世界连接。
- **副客户端（SECONDARY）**：负责渲染和输入，保持热备状态，通过 P2P/中转隧道接入主客户端；合并或分离后可晋升为主客户端。
- **未定级（UNASSIGNED）**：尚未进入世界或正在切换角色。

组客户端由一个主客户端和其副客户端组成。玩家进入未知区块时需要先经过服务端区块申请；区块被其他组占用时，会触发预连接、算力选主和预同步流程，而不是直接重复加载。

## 配置

配置文件位置：

```text
<游戏目录或服务端根目录>/player-to-player/config.json
```

首次启动会自动创建完整默认配置。下面是常用字段示例（JSON 文件不支持注释）：

```json
{
  "mode": "server",
  "relayServerAddress": "",
  "parentServerAddress": "",
  "serverActsAsRelay": true,
  "controlPort": 25580,
  "relayPort": 25581,
  "p2pUdpPort": 0,
  "chunkClaimRetrySeconds": 10,
  "minFreeMemoryBytes": 536870912,
  "geekbenchLookupEnabled": true,
  "nonEnvironmentPaths": [],
  "envFileChunkBytes": 1048576,
  "envSyncViaRelay": false,
  "proxyEnvResyncMinutes": 10,
  "envSnapshotRetentionMinutes": 120,
  "suspendWorldTick": true,
  "disableSpawnChunks": true,
  "mysql": {
    "enabled": false,
    "jdbcUrl": "",
    "username": "",
    "password": ""
  }
}
```

关键配置说明：

- `mode`：节点运行模式。服务器环境首次默认 `server`，客户端环境首次默认 `client`。
- `relayServerAddress`：独立中转端地址，格式为 `IP:端口` 或 `IP`。
- `parentServerAddress`：仅中转端使用的上级服务端地址。
- `serverActsAsRelay`：服务端是否兼任中转端。
- `controlPort` / `relayPort`：控制面和中转面端口，均独立于 Minecraft 原版端口。
- `minFreeMemoryBytes`：成为主客户端所需的最低剩余内存，默认 512 MiB。
- `chunkClaimRetrySeconds`：区块申请被其他组占用后的重试间隔。
- `nonEnvironmentPaths`：相对于服务端根目录的额外非环境文件路径。
- `suspendWorldTick`：服务端是否挂起被接管世界的主 tick；关闭后回退为原版服务端行为，便于排障。
- `disableSpawnChunks`：是否禁用出生点常加载区块，默认开启。
- `mysql`：可选注册表后端。模组不捆绑 JDBC 驱动，需要服务端环境自行提供 MySQL Connector/J。

### 环境文件范围

服务端启动时会扫描环境文件并计算全局 SHA-256。默认排除以下内容：

- `logs/`、`player-to-player/`；
- 存档的 `region/`、`poi/`、`entities/`、`DIM-1/`、`DIM1/` 等世界数据目录；
- `server.properties`、`ops.json`、`whitelist.json`、封禁列表、`usercache.json`、`eula.txt` 等敏感或仅属于物理服务端的文件；
- `nonEnvironmentPaths` 中额外指定的路径。

客户端加入世界时先比较清单哈希，再按文件差异和分块传输同步，不会把上述服务端凭据发给客户端。

## 数据目录

### 服务端

```text
player-to-player/
  config.json
  registry/                 # 各维度区块注册表（默认 JSON）
  logs/                     # <玩家名>-latest.log
  proxy-environment/        # 中转端缓存的上游环境快照（仅中转端）
```

### 客户端

每个“服务器地址 + 世界名”使用独立目录，避免不同服务器之间共享环境和数据：

```text
player-to-player/
  <IP>+<世界名>/
    config.json
    primary/
      environment/
      data/
    secondary/
      environment/
      data/
```

目录名中的 Windows 非法字符会被自动清理。客户端每次加入世界都会重新选择对应的世界目录。

## Mod 前缀分发

服务端环境同步可以根据 JAR 文件名前缀决定节点是否读取某个模组。前缀可叠加且顺序不影响匹配：

| 前缀 | 读取节点 |
| --- | --- |
| `server-` | 服务端 |
| `proxy_server-` | 中转服务端 |
| `server_client-` | 主客户端 |
| `client-` | 副客户端 |
| 无前缀 | 所有节点 |

例如 `server-server_client-client-areas-hint-4.4.5.jar` 会提供给服务端、主客户端和副客户端。由于主副角色可切换，客户端环境同步时会同时准备 `server_client-` 和 `client-` 两类模组。

## 网络与安全

- Minecraft 原版端口、控制端口和中转端口相互独立，请分别配置防火墙和端口转发。
- P2P 打洞使用 UDP；`p2pUdpPort: 0` 表示由系统分配本地端口。严格 NAT、对称 NAT 或 UDP 被阻断时，会自动尝试中转（前提是中转可用）。
- 控制协议默认最大帧为 16 MiB，并带心跳、请求 ID、长度校验和超时处理。
- P2P 数据通道使用 X25519 ECDH 派生 AES-256-GCM 会话密钥，并带递增 nonce 防重放。
- 不要把 MySQL 密码、真实 IP、relay 地址或运行期世界数据提交到 Git；生产部署前应限制控制端口来源并配置备份策略。
- 控制协议当前为 v4，v3/v4 节点会在握手阶段拒绝混连。

## 构建与测试

使用仓库内置的 Gradle Wrapper。Windows：

```powershell
.\gradlew.bat build
.\gradlew.bat test
.\gradlew.bat runClient
.\gradlew.bat runServer
.\gradlew.bat genSources
```

Linux/macOS 将 `./gradlew.bat` 替换为 `./gradlew`。

构建产物位于 `build/libs/`。修改网络、区块所有权、合并/分离或生命周期逻辑后，除了自动化测试，还应在真实客户端/服务端及不同 NAT 条件下进行冒烟测试。

## 项目结构

```text
src/main/java/imsng/player_to_player/
  core/       节点模式、运行上下文和启动引导
  config/     总配置、世界配置和数据路径
  env/        环境扫描、快照和同步
  netproto/   控制协议、编解码和消息类型
  p2p/        NAT 检测、打洞、加密通道和中转
  registry/   区块注册表、玩家表和持久化后端
  group/      组运行时、区块数据面、合并/分离和玩家协作
  server/     服务端控制面、写回、退出事务和路由
  proxy/      中转服务和环境分发
  mixin/      服务端区块、世界 tick、指令和特殊实体注入
src/client/java/imsng/player_to_player/client/
  boot/       客户端引导
  group/      本地组世界、主副客户端和合并切换
  session/    世界会话和退出协调
```

## 文档

- [架构设计与实现分期](docs/DESIGN.md)
- [项目需求与设计提示词](player_to_player-prompt.txt)
- [仓库开发规范](AGENTS.md)

## 许可证

本项目使用 [MIT License](LICENSE)。
