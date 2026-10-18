# 登录锚点与出生点区块加载修复方案（P0，供下个会话直接实施）

日期：2026-10-18
状态：**P0 已实施（2026-10-18）**；`./gradlew clean test build` 通过（350 tests）；游戏内场景 A-D 验收待跑
关联：
- `AGENTS.md` L48-L52、L60-L63、L76、L87、L91、L93、L96（原始需求）
- `player_to_player-prompt.txt` L44（出生点常加载区块）
- `docs/DESIGN.md` §8「出生点常加载禁用」
- 实测日志：`test_log/26.10.17_2/`（server / Eznibuil / big_eye97 / LXW1019）
- 上一场复现：`test_log/26.10.17/server/latest (4).log` + `Eznibuil_rotated.log`

---

## 0. 目标与不变量

### 目标（P0，只做这一组）

1. **受管组世界的登录不再加载世界共享出生点区块**，从而不再触发原点 23×23（529 个）依赖区块的跨组 claim。
2. **登录锚点改为权威 playerdata 的真实位置**：玩家在哪个区块，就从那个区块（+ 四邻缓冲）开始申请，而不是从共享出生点开始。
3. **服务端主线程不再被跨组 claim 无限期挂住**：正常路径秒级登录；异常路径有界等待、可观测、可失败，而不是 295 秒静默卡死。
4. 不破坏现有链路：主/副客户端登录、分离、合并、退出事务、区块写回、隧道 join。

### 不变量（动代码时必须守住）

- **不要为了“快”让调用方在受管世界本地生成一个已归属其他组的区块**。规范 L63/L93：区块先问服务端；有数据拉服务端数据，无数据才按种子生成。跨组占用时只能等 claim 或走合并，不能本地生成后上行覆盖。
- **不要把 claim 门控整体删掉**。门控本身是规范要求（L49/L63）。只允许对“出生点/启动期不该加载的区块”做豁免。
- **不要改共享出生点坐标、不要 `setDefaultSpawnPos` 挪世界出生点**。`WorldIdentity` 不哈希它，但它会影响世界数据与其它玩家；正确做法是“不读出生点”，不是“把出生点挪走”。
- **不要让服务端主线程无限等待**。规范 L96 明确“未加载出来的区块玩家不可进入、视野空白”——等待是允许的，死锁不是。
- **副客户端同样受影响**：副客户端经隧道加入主客户端集成服时也会构造 `ServerPlayer`，同样会走 `fudgeSpawnLocation`。修复必须覆盖所有受管世界登录，不只主客户端本人。
- **协议只做加性字段**，不升 `Protocol.VERSION`（本 P0 原则上不需要改协议）。

---

## 1. 规范依据（实现必须对齐）

| 规范原文 | 对本方案的含义 |
|---|---|
| `AGENTS.md` L76 / prompt L44：「出生点常加载区块：装载了该模组服务器不加载出生点常加载区块」 | 受管组世界**不加载**共享出生点常加载区；登录也不能借 `fudgeSpawnLocation` 偷偷加载。 |
| L48：「被加载区块…每一个被加载区块会在服务端有一个区块列表」 | 归属以服务端注册表为准；本地不能自行认定。 |
| L49 / L63：「如果该区块以及周围四个区块未被其他加载，则被加载」「加载一个区块都需要向服务端询问…检查该区块以及周围四个区块是否被其他组客户端加载」 | 初始申请也必须是**真实位置区块 + 四邻**，不是出生点。 |
| L51：「申请到的最广的范围是主客户端与所属副客户端的视距的并集」 | 登录后的加载范围是玩家视距并集；出生点不在其中。 |
| L52 / L87 / L91：「需要的未知区块…被其他组加载 → 预连接/预同步/算力分配/合并」「没有被加载 → 作为主客户端运行」 | 真实位置区块被占用时走预连接/合并；空闲时直接作为主客户端加载。 |
| L60 / L61：分离后「原组的主客户端不再加载不同副客户端加载的区块」 | 分离迁移的 625 个区块应归新主；新主登录应直接使用它们，而不是回到原点。 |
| L87：「玩家加入世界…确认玩家所需要加载的未知区块是否被加载」 | 登录锚点 = 玩家**需要加载的区块**（真实位置），不是世界出生点。 |
| L91：「玩家大幅度移动（主要是使用 tp）…检查渲染区块是否被其他组加载」 | 与本次 tp 后分离场景一致；分离后按新位置申请。 |
| L96：「玩家的申请的区块因为申请和合并并未加载出来：未加载出来的区块玩家不可进入 未加载出来的区块玩家视野仍然为空白」 | 登录/加载可以等待或空白，**不能**让主线程永久阻塞；也不能本地生成覆盖。 |

---

## 2. 根因与证据（一页）

### 2.1 直接根因

原版 `ServerPlayer` 构造器在**读档之前**调用 `fudgeSpawnLocation(level)`：

```
ServerPlayer 构造器
  → Player(level, level.getSharedSpawnPos(), ...)      // 初始位置 = 共享出生点
  → fudgeSpawnLocation(level)
    → level.getSharedSpawnPos()
    → PlayerRespawnLogic.getOverworldRespawnPos(level, spawnX, spawnZ)
      → level.getChunk(spawnX>>4, spawnZ>>4)           // 同步 FULL 加载出生点区块
        → ServerChunkCache.getChunk(...).join()        // 服务端主线程阻塞
          → ChunkMapMixin.readChunk 被替换为 claimWithRetry
            → 被旧组拒绝 → future 永不完成 → 主线程无限等待
```

- `MinecraftServerMixin` 只取消了 `prepareLevels`（`MinecraftServerMixin.java:53-78`），`DESIGN.md:174` 也只写了这一条；
- `fudgeSpawnLocation` 是**登录路径**，不经过 `prepareLevels`，所以“跳过出生点预热”对它无效；
- 出生点区块升到 FULL 需要 **23×23 = 529** 个依赖区块（半径 11 的生成依赖足迹），日志中 529 个唯一坐标（x/z=-12..10，中心 -1,-1）正是它，不是视距 625；
- 旧组 `914b5cdf`（big_eye97 在原点）持有这些区块，claim 全部被拒；
- 23:50:03 旧组解散释放区块 → 23:50:05 Eznibuil 才 `logged in`，并报 `Can't keep up! Running 295390ms or 5907 ticks behind`；
- 遥测：本地 `world_loaded` 23:50:05、`world_unloaded` 23:50:07（只跑 1 tick），中间 5 分钟世界未加载。

### 2.2 复现证据

- 上一场 `Eznibuil_rotated.log` 00:44:47 集成服启动，00:44:49 开始原点区块被拒，00:46:09 才登录并报 `Running 80890ms or 1617 ticks behind`；
- big_eye97 首次接管时同样报 `Running 50604ms or 1012 ticks behind`；
- 说明这不是本次 tp/分离的偶发，而是**每次受管世界登录**都可能触发的系统性缺陷。

### 2.3 为什么不是 `prepareLevels` / START ticket

- `prepareLevels` 已被 mixin 取消，日志有「已跳过出生点常加载区块预热」；
- 本次实测的阻塞点在 `ServerPlayer` 构造器 → `fudgeSpawnLocation` → `getChunk(sharedSpawn)`，与 START ticket 是否添加无关；
- 因此修复不能只继续加固 `prepareLevels`，必须覆盖登录路径。

---

## 3. 方案总览

### P0（本方案，下一会话实施）

- **P0-1**：受管组世界登录时取消 `ServerPlayer.fudgeSpawnLocation`，不再加载共享出生点区块。
- **P0-2**：确认登录锚点顺序：`PlayerList.placeNewPlayer` 在 `load`（playerdata）之后才 `addNewPlayer`（区块 ticket），所以取消 fudge 后玩家会以真实位置进入，无需重排原版顺序。
- **P0-3**：给 `ChunkMapMixin` 的同步等待加“登录期看门狗”：服务端主线程上的同步 `getChunk` 超时/超长时打印区块、阻塞组、ticket、线程栈，并按明确策略失败返回，绝不无限挂起。
- **P0-4**：登录期诊断日志：记录玩家真实区块、维度、组 ID、首批 claim 结果；出现原点区块申请时打印调用栈。
- **P0-5**：测试与验收：三人组 tp 分离、旧组保持在线；新主 ≤5 秒登录；无 529 原点申请；无 `Can't keep up`。

### P1（本方案只列边界，不在同一批实现，避免范围膨胀）

- 合并触发加空间邻接校验 + 分离抑制跨进程下发；
- `PlayerStateUploadService` 离组清理、`not_authorized` 终态、退避/轮转；
- P2P 隧道双向存活检测、中转降级/重连。

> P1 的详细方案见本文件 §8；P0 完成后应先验证 P0 是否能自然消掉“出生点触发合并”这一半，再决定 P1 的实现顺序。

---

## 4. P0 详细实施

### 4.1 P0-1：新增 `ServerPlayerMixin`

**文件**：`src/main/java/imsng/player_to_player/mixin/ServerPlayerMixin.java`

**目标类/方法**（已用 1.20.4 字节码核实）：

- 类：`net.minecraft.server.level.ServerPlayer`
- 方法：`private void fudgeSpawnLocation(net.minecraft.server.level.ServerLevel)`
- 描述符：`(Lnet/minecraft/server/level/ServerLevel;)V`

**注入**：

```java
@Mixin(ServerPlayer.class)
public abstract class ServerPlayerMixin {

    @Unique
    private static final Logger player_to_player$LOGGER =
            LoggerFactory.getLogger("player_to_player/mixin");

    @Inject(
            method = "fudgeSpawnLocation(Lnet/minecraft/server/level/ServerLevel;)V",
            at = @At("HEAD"),
            cancellable = true
    )
    private void player_to_player$skipSpawnFudge(
            ServerLevel level, CallbackInfo ci) {
        // 只在被组运行时接管的集成服务端世界生效；单人/专用服务器/未接管世界零行为差异
        if (GroupRuntime.isManagedLevel(level)) {
            // 规范 L76：受管组世界不加载出生点常加载区块。
            // 玩家真实位置由 placeNewPlayer 的 load(playerdata) 设置，
            // 且 addNewPlayer（区块 ticket）在 load 之后，故这里跳过安全出生点
            // 修正不会让玩家真的停在共享出生点。
            player_to_player$LOGGER.info(
                    "受管组世界跳过原版出生点 fudge: dimension={} spawn={}",
                    level.dimension().location(), level.getSharedSpawnPos());
            ci.cancel();
        }
    }
}
```

**必须一起改**：

- `src/main/resources/player_to_player.mixins.json` 的 `mixins` 数组加入 `"ServerPlayerMixin"`（放在 `MinecraftServerMixin` 附近，保持字母序/分组习惯）。
- 该 mixin 放在 `src/main`（common），所以客户端集成服与专用服都会加载；`GroupRuntime.isManagedLevel` 保证专用服不受影响。
- 日志只打一次/每次登录打一次均可；建议用 `level` 维度 + `player` 可用的上下文。构造器阶段拿不到玩家名，可在 `ServerPlayerMixin` 里用 `@Shadow` 拿 `getGameProfile()`；若嫌重，先打维度与出生点即可。
- 不要试图在 `fudgeSpawnLocation` 里“改成用玩家真实位置做安全修正”——构造器阶段 `load(playerdata)` 还没发生，拿不到真实位置；正确做法就是跳过，让 `placeNewPlayer` 后续按真实位置落地。

**验收点**：

- 日志出现「受管组世界跳过原版出生点 fudge」；
- 受管世界登录不再出现 `getChunk(sharedSpawn)` 引起的原点 529 个 claim；
- 专用服务器启动/玩家登录行为不变（mixin 条件不成立）。

---

### 4.2 P0-2：确认登录锚点顺序（无需重排原版）

**已核实**（1.20.4 `PlayerList.placeNewPlayer` 字节码）：

```
placeNewPlayer(Connection, ServerPlayer player, CommonListenerCookie)
  offset 74  : load(player)                  // 读 playerdata，设置真实位置
  offset 185 : player.setServerLevel(level)  // 按 playerdata 的 Dimension 切维度
  offset 242 : LOGGER.info("... logged in ... at ({},{},{})", getX/Y/Z)  // 已是真实位置
  offset 702 : level.addNewPlayer(player)    // 这里才加 player ticket / 区块跟踪
```

结论：

- `addNewPlayer` 在 `load` 之后，**登录锚点天然就是真实位置**；P0-1 跳过 fudge 后不需要额外重排；
- 客户端收到的 `ClientboundLoginPacket` 也使用真实位置（`createCommonSpawnInfo` 基于 `player`），所以“先出生点再切真实位置”的服务端数据路径被消除；
- 若实测仍有视觉闪一下，属于客户端 `ReceivingLevelScreen`/区块包时序，单独排查（见 §7.3），不属于服务端锚点问题。

**验收点**：

- `logged in ... at (x,y,z)` 的坐标是 playerdata 的坐标（本次为 ~3000,3000）；
- 从 `Starting integrated minecraft server` 到 `logged in` 的时间 ≤5 秒（旧组在线）；
- 不出现 `Can't keep up`（或仅首 tick 的毫秒级抖动）。

---

### 4.3 P0-3：`ChunkMapMixin` 登录期看门狗

**文件**：`src/main/java/imsng/player_to_player/mixin/ChunkMapMixin.java`

**目的**：即使将来还有其他原版路径在服务端主线程同步 `getChunk`，也不能再出现 295 秒静默挂起。

**现状**：

```java
cir.setReturnValue(claims.claimWithRetry(key).thenCompose(outcome -> { ... }));
```

`claimWithRetry` 被拒时 future 不完成，调用方若在服务端线程 `join()` 就永久阻塞。

**建议实现**（按优先级从保守到强）：

1. **诊断看门狗（必须做）**
   - 在 `GroupRuntime` 或新类 `LoginLoadGuard` 中维护登录期状态：
     - `beginLogin(playerId, dimension, chunkPos, now)`：`ServerPlayConnectionEvents.JOIN` 之前或 `placeNewPlayer` 进入时置位；
     - `endLogin(playerId)`：`JOIN` 或失败/断线时清除；
     - `isLoginPhase()`：登录窗口内为 true。
   - 在 `player_to_player$gateChunkLoad` 返回组合 future 之前，如果 `level.getServer().isSameThread()` 且 `LoginLoadGuard.isLoginPhase()`：
     - 记录 `key`、`blockingGroup`（由 `ChunkClaimClient` 暴露诊断）、`attempt`、`firstDeniedAt`；
     - 用 `ThreadPools.scheduler()` 在 `LOGIN_WARN_MILLIS`（建议 5s）打印一次 WARN：
       ```
       登录期同步区块加载等待超过 5s: key=..., 阻塞组=..., 玩家=..., 维度=...,
       ticket 摘要=..., 线程栈=...
       ```
     - 只打印一次/每 key 一次，避免刷屏。
2. **硬超时（建议做，需明确数据语义）**
   - 在 `LOGIN_HARD_TIMEOUT_MILLIS`（建议 60s）后让组合 future 异常完成（`completeExceptionally(new IOException("登录期区块 claim 超时: ..."))`）；
   - **2026-10-18 实施修正（对抗式复查发现）**：不能使用 `IOException`。1.20.4
     `ChunkMap.scheduleChunkLoad` 的 `exceptionallyAsync` 把 IOException 视为
     “磁盘读失败”，记录 `Couldn't load chunk` 后返回一个**空 ProtoChunk**，后续按种子
     生成并上行 —— 正是本方案禁止的数据污染。必须使用非 IOException 的
     `CompletionException`（实现为 `LoginLoadGuard.LoginLoadTimeoutException`），
     原版才会走“标记失败 + rethrow”分支；同时硬超时只在同一个登录窗口仍有效时生效，
     JOIN 之后不再误杀异步加载。
   - 原版 chunk load 会失败并记录错误，服务端线程继续；玩家可能停留在加载界面/空白视野，符合 L96；
   - **不要**在超时后返回 `Optional.empty()` 让原版本地生成——该区块可能属于其他组，会污染数据；
   - 超时后 `ChunkClaimClient` 的 pending claim 仍在按 10s 重试；一旦授予，后续再次加载会成功。
3. **不要做的**
   - 不要在 `readChunk` 里对“真实位置被其他组占用”的区块返回 `Optional.empty()`；
   - 不要把 `LOGIN_HARD_TIMEOUT` 设得太短（例如 1s），否则正常跨中转的慢路径会被误杀；
   - 不要在非服务端线程上启用硬超时（异步区块加载本来就可以等）。

**需要 `ChunkClaimClient` 提供的诊断**（小改）：

- 为每个 `ChunkKey` 记录最近一次拒绝的 `blockingGroup`/`blockingChunk`/`firstDeniedAt`/`attempts`；
- 提供 `ClaimDiagnostics snapshot(ChunkKey)` 或在拒绝时回调给 `LoginLoadGuard`；
- 这些是只读诊断，不改变 claim 状态机。

**验收点**：

- 人为让旧组持有真实位置区块，登录期 5s 打印 WARN（key/阻塞组/线程栈）；
- 60s 后不再静默挂起；服务端 tick 继续，日志有明确超时原因；
- 旧组释放后，claim 授予，后续加载成功。

---

### 4.4 P0-4：登录期诊断日志

**建议位置**：`GroupServerHooks` 的 `ServerPlayConnectionEvents.JOIN` 回调（已有），或 `ChunkMapMixin` 的首次 claim。

记录：

```
[player_to_player/login] 玩家加入: name=..., uuid=..., group=...,
  dimension=..., chunk=(x,z), pos=(x,y,z),
  firstClaims=[key1, key2, ...]（最多 20 个）
```

以及：

- 如果登录期出现 `sharedSpawn` 区块的 claim 请求，打印 WARN + 调用栈（说明 P0-1 没覆盖到的路径）；
- 如果首个被拒区块在玩家渲染矩形内，打印 `blockingGroup`、距离、该组占用区块数；
- 如果首个被拒区块不在玩家渲染矩形内（例如原点/出生点），打印 WARN「非玩家视野区块触发了 claim/合并」，并标记为疑似启动期加载。

这些日志是下一次回归验收的直接证据。

---

### 4.5 P0-5：测试

#### 单元测试（不依赖真实 MC 客户端）

1. **`LoginAnchorPolicy` 纯函数测试**
   - 新增一个纯 helper（可放 `group/LoginAnchorPolicy.java` 或 `world/`）：
     - `shouldSkipSpawnFudge(boolean managedLevel, boolean disableSpawnChunks)`；
     - `isLoginPhase`/`isServerThread` 的组合判断；
     - `shouldWarnNonPlayerChunk(...)`。
   - 测试：受管 + `disableSpawnChunks=true` → skip；非受管 → 不 skip；专用服 → 不 skip。
2. **`ChunkClaimClient` 诊断/超时测试**
   - 用现有测试注入的 `retryScheduler`/fake connection，模拟 claim 被拒；
   - 断言 `ClaimDiagnostics` 记录了 blockingGroup、attempt、firstDeniedAt；
   - 断言超时 future 异常完成且不会重复完成。
3. **`PlayerStateUploadService`（P1 预留，不在 P0 必须）**
   - 离组清理/终态识别/退避，单独一批。

#### 集成/手动验收（必须做）

**场景 A：本次事故的复现**

1. 三人：big_eye97（旧主，原点）、LXW1019（旧组副）、Eznibuil（先副后 tp 3000）。
2. Eznibuil `/tp 3000 ~ 3000`。
3. 等 10s 分离完成；**保持 big_eye97 在线，不要退出**。
4. 预期：
   - Eznibuil 的新主本地服在 ≤5s 内 `logged in`；
   - 日志不再出现原点 x/z=-12..10 的 529 个 claim；
   - 不出现 `Can't keep up!`；
   - 不出现「已向服务端发起合并申请」由原点区块触发；
   - 他的本地世界 `world_loaded` 与 `logged in` 时间差 ≤5s（遥测）。
5. 反向验收：让 big_eye97 保持在线 2 分钟，Eznibuil 仍能正常游玩（不依赖旧组退出解锁）。

**场景 B：上一场复现**

- 重跑 `00:44` 场景（Eznibuil 成为主、原点归旧组）：
  - 旧版本 80.9s → 修复后 ≤5s；
  - 无 529 claim。

**场景 C：新玩家/重生（边界）**

1. 新玩家（无 playerdata）加入受管组世界：
   - 预期不加载共享出生点常加载区；若出生点区块空闲，正常 claim；若被其他组占用，进入等待/预连接/合并路径；
   - 玩家不应窒息/掉虚空；若视野空白，符合 L96。
2. 玩家死亡重生：
   - 预期不因 `fudgeSpawnLocation` 被取消而出现“重生点在方块里”；
   - 若出现，按 §7.2 处理（只对“首次登录”跳过，或对真实重生点做安全修正，但不得读共享出生点）。

**场景 D：副客户端**

- 副客户端经隧道加入主客户端组世界：
  - 登录 ≤5s；不出现原点 claim；隧道/合并链路不退化。

---

### 4.6 P0-6：文档更新

- `docs/DESIGN.md` §8「出生点常加载禁用」改为：
  - 受管组世界不加载出生点常加载区，覆盖两条入口：
    1. `MinecraftServer.prepareLevels`（启动预热，已有 mixin）；
    2. `ServerPlayer.fudgeSpawnLocation`（每次登录，本方案新增）。
  - 明确“登录锚点 = 权威 playerdata 的真实位置；`PlayerList.placeNewPlayer` 的 `load` 在 `addNewPlayer` 之前，故无需重排原版顺序”。
- `MinecraftServerMixin` 的 Javadoc 补一句“本 mixin 只覆盖 prepareLevels；登录路径见 ServerPlayerMixin”。
- 本文件保留，作为下一会话的实施依据。

---

## 5. 代码改动清单（下一会话按此顺序）

| 序号 | 文件 | 改动 | 备注 |
|---|---|---|---|
| 1 | `src/main/java/imsng/player_to_player/mixin/ServerPlayerMixin.java` | 新增；`fudgeSpawnLocation` HEAD cancel（`GroupRuntime.isManagedLevel`） | 核心 P0 |
| 2 | `src/main/resources/player_to_player.mixins.json` | 加入 `ServerPlayerMixin` | 必须与 1 同批 |
| 3 | `src/main/java/imsng/player_to_player/group/LoginLoadGuard.java` | 新增；登录期状态 + 5s WARN / 60s 硬超时策略 | 可先只做 WARN |
| 4 | `src/main/java/imsng/player_to_player/mixin/ChunkMapMixin.java` | 组合 future 加登录期看门狗/超时；不发本地生成 | 防御性 |
| 5 | `src/main/java/imsng/player_to_player/group/ChunkClaimClient.java` | 记录 blockingGroup/attempt/firstDeniedAt 诊断 | 只读诊断 |
| 6 | `src/main/java/imsng/player_to_player/group/GroupServerHooks.java` | JOIN 时打印真实区块/首批 claim | 验收证据 |
| 7 | `src/test/java/imsng/player_to_player/group/LoginAnchorPolicyTest.java` | 新增纯函数测试 | 必做 |
| 8 | `docs/DESIGN.md` | 更新出生点章节 | 与代码同批 |
| 9 | `src/test/java/imsng/player_to_player/group/ChunkClaimDiagnosticsTest.java` | 新增诊断/超时测试 | 建议 |

**不要在本批改**：

- `MergeCoordinator`/`MergeTriggers`/`PlayerStateUploadService`/`P2PChannel`（P1，见 §8）；
- 任何 `level.dat` 出生点坐标；
- `ChunkMapMixin` 的整体门控逻辑（只加看门狗，不删 gate）。

---

## 6. 实施前必须核对的两件事

1. **Mixin 目标映射**
   - 运行 `./gradlew genSources` 后确认 `ServerPlayer.fudgeSpawnLocation` 的 mojmap 名称/描述符；
   - 用 `javap -p` 复核 `private void fudgeSpawnLocation(ServerLevel)`；
   - 如果 Loom 的 intermediary 名称不同，以 `genSources` 结果为准，但方法体与调用点不变。

2. **`PlayerList.placeNewPlayer` 顺序**
   - 已用 1.20.4 字节码确认 `load`（offset 74）→ `addNewPlayer`（offset 702）；
   - 实现前再跑一次 `javap` 或 `genSources` 复核，防止版本差异；
   - 若顺序在未来版本变为 `addNewPlayer` 先于 `load`，则需要额外的 `PlayerListMixin` 把位置设置提前；本版本不需要。

---

## 7. 风险、边界与待决策

### 7.1 新玩家（无 playerdata）

- 无 playerdata 时，`load` 返回 null，玩家位置仍是构造器里的共享出生点；
- 跳过 `fudgeSpawnLocation` 后，玩家会直接在这个位置进入，不加载出生点常加载区；
- 若该位置区块空闲，正常 claim 加载；若被其他组占用，按 L52/L87 走预连接/合并；
- 若出生点在方块里，可能出现窒息/卡住。**建议**：在 `placeNewPlayer` 之后、`addNewPlayer` 之前，对“无 playerdata 的新玩家”做一次**基于真实位置区块**的安全检查；如果不想做，至少在验收里记录该边界，后续 P1 再处理。

### 7.2 死亡重生

- `fudgeSpawnLocation` 也会在重生构造 `ServerPlayer` 时调用；
- 跳过它后，重生位置由重生逻辑/玩家数据决定；可能出现“重生点在方块里”；
- **建议**：先按“受管世界一律跳过”实施，并在验收场景 C2 中验证；若出现窒息，再改为“只对首次登录跳过”（需要区分构造器调用来源）或“用真实重生点做安全修正，但不读共享出生点”。

### 7.3 客户端“闪一下”

- 服务端修复后，登录包位置已经是真实位置；如果仍有视觉闪，属于客户端 `ReceivingLevelScreen`/区块包时序，建议单独排查；
- **不要**为了消除闪而让客户端跳过 claim 或本地生成；服务端锚点正确是前提，视觉优化是后续独立项。

### 7.4 真实位置区块被其他组占用

- 登录会等待 claim/合并，玩家可能停留在加载界面/空白视野；
- 这符合 L96；但不能让服务端主线程无限等待（P0-3）；
- 合并触发必须只对“玩家渲染矩形内”的区块生效（P1 §8.1），否则又会回到“原点出生点触发远距离合并”。

### 7.5 副客户端登录

- `ServerPlayerMixin` 的 `isManagedLevel` 条件同样覆盖副客户端经隧道加入的集成服；
- 副客户端的真实位置来自 playerdata，`placeNewPlayer` 同样先 `load` 后 `addNewPlayer`；
- 验收场景 D 必须覆盖。

---

## 8. P1（本方案不实施，但下一批必须接着做）

### 8.1 合并触发空间校验 + 分离抑制

- `MergeCoordinator.handleMergeRequest`：解析双方组的玩家位置，使用与 `SplitMonitor.blocksIntersect` 相同的“渲染矩形 + 2 格曼哈顿缓冲”判据；无交集直接 `error("not_adjacent")`。
- `ChunkClaimClient` 触发 `MergeTriggers.onClaimBlocked` 前，仅当该 key 落在任一在线玩家的渲染矩形内才转发；出生点/启动期区块不触发合并。
- 分离完成时把抑制窗口下发/应用到新主进程（`SPLIT_ACK` 加 `suppressMergeMillis`，或服务端对组对加 5 分钟冷却）。

### 8.2 玩家状态上行风暴

- `PlayerStateUploadService`：
  - 组变更/分离/玩家离开时清理该玩家的 `pending`/`confirmed`；
  - `ERROR not_authorized` 视为终态，丢弃并只告警一次；
  - 失败日志退避 + 限速；
  - 轮转处理 pending，不要 `findFirst()` 卡住全组。
- `GroupServerHooks.reportPositions` 只上报当前组成员。

### 8.3 P2P 隧道存活与降级

- `P2PChannel` 增加双向心跳/最近收包时间；
- `ReliableChannel` 在无 ACK 进展超过阈值时判定路径死亡；
- 路径死亡后尝试重新直连，失败则走服务端重新撮合的中转；
- 记录 last-send/last-recv/重传队列/降级事件；
- 本次 LXW1019 掉线是独立问题，不能与出生点问题混为一谈。

---

## 9. 验收清单（下一会话完成后逐条打勾）

- [ ] `./gradlew build` 通过；
- [ ] `./gradlew test` 通过（含新增 `LoginAnchorPolicyTest`）；
- [ ] 受管世界登录日志出现「受管组世界跳过原版出生点 fudge」；
- [ ] 场景 A：旧组在线，Eznibuil tp 3000 分离后 ≤5s 登录；
- [ ] 场景 A：无原点 529 个 claim；无 `Can't keep up`；无原点触发的合并申请；
- [ ] 场景 A：旧组保持在线 2 分钟，Eznibuil 仍可正常游玩；
- [ ] 场景 B：上一场 00:44 复现从 80.9s 降到 ≤5s；
- [ ] 场景 C1：无 playerdata 新玩家可进入、不窒息；
- [ ] 场景 C2：死亡重生可用、不因跳过 fudge 卡在方块里（若失败，按 §7.2 处理）；
- [ ] 场景 D：副客户端经隧道登录 ≤5s；
- [ ] 专用服务器启动/登录无行为变化；
- [ ] `docs/DESIGN.md` 出生点章节已更新；
- [ ] 日志/遥测中 `world_loaded` 与 `logged in` 的时间差 ≤5s（场景 A）。

---

## 10. 一句话交接

**下一会话从 `ServerPlayerMixin` 开始：在受管组世界取消 `ServerPlayer.fudgeSpawnLocation`，让登录不再同步加载共享出生点区块；`PlayerList.placeNewPlayer` 本身已经先 `load`（真实位置）再 `addNewPlayer`（区块 ticket），所以不需要重排原版顺序；再用 `ChunkMapMixin` 的登录期看门狗兜底，保证服务端主线程永远不会再被跨组 claim 无限挂住。**

---

## 11. 实施记录（2026-10-18）

- **P0-1/P0-2**：新增 `mixin/ServerPlayerMixin.java`（`fudgeSpawnLocation` HEAD 取消；构造器 HEAD/RETURN 的短窗口仅覆盖 fudge 防御路径），注册进 `player_to_player.mixins.json`。构建 jar 已复核 remap 为 `method_14245(Lnet/minecraft/class_3218;)V`。
- **P0-3**：新增 `group/LoginLoadGuard.java`；新增 `mixin/PlayerListMixin.java` 在 `placeNewPlayer` HEAD 开启权威登录窗口（重生不经过 placeNewPlayer，不会留下假窗口）；`ChunkMapMixin` 对登录窗口 + 服务端主线程的 `readChunk` 组合 future 武装 5s WARN / 60s 硬超时。硬超时为 `LoginLoadTimeoutException`（**非 IOException**）：原版把 IOException 当作读盘失败并生成空 ProtoChunk → 种子生成；非 IOException 才安全 rethrow。硬超时只在同一登录窗口仍有效时生效。
- **P0-4**：`ChunkClaimClient` 新增只读 `ClaimDiagnostics`（阻塞组/区块/次数/首次与最近拒绝时间）；`GroupServerHooks` JOIN 打印真实锚点、firstClaims、视野外申请 WARN，DISCONNECT 结束窗口。
- **P0-5**：新增 `LoginAnchorPolicyTest`、`LoginLoadGuardTest`、`ChunkClaimDiagnosticsTest`；`./gradlew clean test build` → 350 tests / 0 failures / BUILD SUCCESSFUL。
- **P0-6**：`docs/DESIGN.md` §8 与 `MinecraftServerMixin` Javadoc 已更新。
- 游戏内场景 A-D（三人 tp 分离、旧组在线、副客户端隧道登录、新玩家/重生边界）仍需按 §9 实测。
