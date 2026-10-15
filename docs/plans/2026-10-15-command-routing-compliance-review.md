# 指令传输路径复查（规范 → 实现逐条对照）

日期：2026-10-15
状态：**只读复查，未改任何代码**
权威文本：`player_to_player-prompt.txt`（82 行；L37/L48/L49，与 `AGENTS.md` 第 35-112 行同文）
配套方案：`docs/plans/2026-10-15-command-routing-fix-plan.md`

> **取证说明（必须如实标注）**：本次复查原本派 5 路独立代理，其中 **4 路因 API 余额不足（402 Insufficient Balance）中途失败**：
> `对照:本地执行与权限污染`、`对照:上送触发与限速`、`对照:权限口径与OP`、`对照:玩家表与如同原版`。
> 因此本文件 = **唯一完成的代理结论（维度③ 服务端面，含 javap 级核实）** + **我本人对其余四个维度的复核**（读源码得出，均已给出 file:line）。
> 凡是我没有独立复核过的代理断言，都标了 `[代理未复核]`。**下个会话若余额恢复，可优先把这四个维度重跑一遍做对抗校验。**

---

## 1. 规范要求 → 实现 对照总表

| # | 规范条款（原文与行号） | 实现 | 判定 | 证据 |
|---|---|---|---|---|
| R1 | L48「指令传递的顺序为 副客户端→主客户端→中转服务端→服务端→其他的主客户端→其他的组客户端的副客户端」 | 副→主=原版语义（隧道+集成服执行）；主→服务端=**本地失败才上送**；中转=传输层透明；服务端→其他主=`forwardToOtherPrimaries` 并发下发；→其他副=各组内原版链路 | 部分符合 | `CommandsMixin.java:36-52`、`CommandRelayClient.java:125-174`、`CommandChatHandlers.java:186-229`、`DESIGN.md:471-495` |
| R2 | L48「单个组服务端相当于是一个小服务器它是相对独立的 所以**只有全局性的指令和跨区块的指令需要分级下放**」 | **全仓无任何指令类别判定**（grep `gamerule|weather|difficulty|全局` 在 src/ 0 命中）；触发轴 = "本地失败"（`CommandChatHandlers.java:149-150`、`CommandRelayClient.java:167-174`） | **缺失（P0/P1）** | 见 G-01/G-02 |
| R3 | L48「该方法有一个必要基础是在原版中不合理的指令是不会被执行的」 | 实现用它给"失败即上送"背书；但**本地权限被三条判据压到 0**（`GroupHost.java:183-184` cheats=false + 骨架 `level.dat allowCommands=0` + `ops.json` 被 `EnvironmentPathPolicy.java:31` 排除）→ **"本地失败" ≠ "指令不合理"** | **前提被打破（P0）** | 见 G-01；`2026-10-14-stale-p2p-state-after-world-replace.md` §5 |
| R4 | L48「报错提示规则 在指令被传递完所有的端都报错后则返回本地的报错信息」 | 全败/超时 → `replyUnhandled` → 客户端静默（保留本地已显示的报错）→ **最终答复就是本地报错**，语义等效；差别是**时序**（本地报错立即显示、不撤回） | 有意偏离（建议记账） | `CommandChatHandlers.java:199-201,224-226`、`CommandRelayClient.java:191-195`、`DESIGN.md`（javadoc 记录取舍） |
| R5 | L48「如果被执行 则将执行端的消息返回发出指令的端」 | ✓ `message` 回投；`/tp` 特例另有 `action=teleport` | 符合 | `CommandChatHandlers.java:218-223,116-148`、`CommandRelayClient.java:196-201,243-251` |
| R6 | L48「服务端可以执行原主客户端不能执行的 tp 指令」 | ✓ 两段式 `/tp\|/teleport <玩家名>` 用玩家表解析 → 发起端本地 `teleportTo`；但**仅覆盖两段式**，坐标式 `/tp x y z`、`/tp @s`、`/execute …` 会走泛化下放，而转发端是 `entity=null` 虚拟源 → 远端必然失败、发起组又被排除 → 两头落空 | 部分符合 | `CommandChatHandlers.java:116-134`、`CommandRelayClient.java:205-241,311-314` |
| R7 | L37/L48「玩家表来统计玩家实时位置**以便于指令传输如同原版一样**」 | 表在（UUID→名字/维度/坐标/组/角色），1Hz（20 tick）上报喂入，`/tp`、`/msg` 用它寻址；但：**选择器 @a/@p/@e 只在本组解析**、`/list` 特例不可达、坐标 1Hz 陈旧、`uuid` 客户端自报 | 部分符合 | `PlayerTable.java:19-137`、`GroupServerHooks.java:42,110-138`、`RegistryHandlers.java:335-347` |
| R8 | L49「消息需要让所有客户端看到（副→主→中转→服务端→中转→其他主→其他副）」 | ✓ `ChatRelay`：上送→服务端广播给**其他**组主客户端→各组内系统消息重放（副客户端经原版可见）；跨组私聊 `privateTo` | 符合 | `CommandChatHandlers.java:233-259`、`ChatRelay.java:89-108` |

---

## 2. 缺口清单

### G-01（P0）本地执行面被权限压成 0，"逐级处理"的第一级失效 —— **【对抗校验：CONFIRMED】**
- **要求**：L48 假定"本地小服务器能处理本地指令"。
- **机制**：三条判据同时为 0 —— `GroupHost.java:183-184` 发布 LAN 时 `publishServer(getDefaultGameType(), false, port)`（原版 `setAllowCheatsForAllPlayers(false)`）；骨架 `level.dat` 的 `allowCommands=0`；客户端目录无 `ops.json` 且它被 `EnvironmentPathPolicy.java:31` 排除。
- **字节码级核实（校验代理，javap 反汇编 loom 缓存 1.20.4 jar）**：
  - `IntegratedServer.publishServer(boolean cheats, int port)`：先 `PlayerList.setAllowCheatsForAllPlayers(cheats)`，再 `LocalPlayer.setPermissionLevel(getProfilePermissions(minecraft.player.getGameProfile()))`，最后对每名在线玩家 `Commands.sendCommands(player)`；
  - `MinecraftServer.getProfilePermissions(GameProfile)`：**先判 `PlayerList.isOp`，不满足直接 `return 0`**；
  - `PlayerList.isOp` = `ops.get(id) != null` **或** `(server.isSingleplayerOwner(profile) && worldData.getAllowCommands())` **或** `allowCheatsForAllPlayers` —— 三条分支在本部署里**全为 false** → 权限恒 0 → `LocalPlayer.permissionLevel = 0` → F3+F4 处 `player.hasPermissions(2)` 为 false，**客户端直接拒绝、连指令都不发**（实测 `test_log/26.10.15/cilent/latest.log:137-141` 连续 5 条）。
  - **追加屏障（新）**：`Commands.sendCommands(ServerPlayer)` 会按 `CommandNode.canUse(sourceStack)` **过滤该玩家的指令树** —— 权限 0 的客户端连 `/time` 这类指令都不在树里。[已核实-字节码] 因此 OP 指令在本地不仅"执行失败"，很多连"被识别"都做不到。
  - 反方论点已逐条排除：全仓 grep 无任何写 `ops.json` / 授 op / 改 `allowCommands` / `setPermissionLevel` 的代码；12 个 mixin 无一触碰权限判定；原版**不会**因"单机世界所有者"自动给 4 级（owner 分支被 `isOp` 外层门挡住，且 owner 分支本身还要 AND `getAllowCommands()`）。
- **影响**：一切 2 级指令在本地必然失败 → 全部进入下放通道（G-02 的触发器）；用户症状"服务端 OP 传不到客户端、无法查看区块"的直接原因。**注意 `GroupHost.java:183` 的行内注释表明 cheats=false 是有意决策**（"组世界的权威演算不应被本地作弊指令干预"）→ 修法必须是**按玩家、按服务端 ops 名单精确落权**，不是开全局作弊。
- **关于"指令上送 0 次"**：与本节一致但要分清因果——F3+F4 被客户端拦下（不发指令）、`/op` 与 `/gamemode` 是**服务端控制台**敲的（`test_log/26.10.15/server` 无"指令上送"行）；如果玩家真的在聊天栏敲一条 OP 指令，探针会发火并上送（[推断]）。
- **否证**：若在本地权限仍为 0 的前提下，客户端能执行任意 2 级指令（例如 `/gamemode creative` 生效），本缺口不成立。
- **证据**：`GroupHost.java:183-184`、`EnvironmentPathPolicy.java:31`、`WorldIdentity.java:200-219`（身份哈希不含 allowCommands）；实测 `test_log/26.10.15/cilent/latest.log:137-141`。

### G-02（P1）下放触发轴错位：任何本地失败都在**所有其他组**并发真实执行 —— **【对抗校验：CONFIRMED】**
- **要求**：L48"只有全局性的指令和跨区块的指令需要分级下放"。
- **机制**（代理复核 + 校验代理逐行核实）：`CommandChatHandlers.java:212-228` 对**每个**其他组发 `execute=true`，**循环内不等待、无互斥/取消**——`done.compareAndSet` 之后**不再触碰其他 target 的 future**，即"首个成功"只决定给发起端回哪条消息，其余请求早已发出并在对端执行；`GroupTable.primariesSnapshot()`（`:212-218`）是**全表拷贝**，组数 ≥3 时必然 ≥2 个 target（恰好 2 组时只有 1 个 target，多执行不可见——属**适用范围**而非反驳）；接收端 `CommandRelayClient.java:288-329` 只判 `execute=true`、长度、权限钳制，**没有任何"别的组已成功就别执行"的门**，直接 `performPrefixedCommand`；全仓无指令白/黑名单。`DESIGN.md:485-486` 的"逐个试执行"措辞与代码不符。
- **精确边界（校验代理补）**：接收端仍受**原版权限检查**约束（转发权限钳 ≤2 → 3/4 级指令在对端也失败）；虚拟源 `entity=null`、锚在**接收组主世界出生点**（`:311-314`）→ 需要玩家上下文/相对坐标的指令在对端失败。这两点收窄了"实际会被执行的指令集合"，但不改变"发给了谁、谁被执行"的结论。
- **影响**：① 本地域指令（`/give @p`、`/kill @e`、`/setblock`…）会**落在其他组的世界里**（`@p` 以远端出生点为锚、`@e` 在他组世界选实体）——不可逆；② 全局类指令**只落他组、不落发起组**（G-03）；③ 单组部署（当前实测环境）`targets` 为空 → `replyUnhandled` → **OP 的一切特权指令哑火**。
- **否证**：两组环境，A 组（op）执行 `/give @p diamond`：若 B 组无任何实体收到、且 B 组无"已代执行其他组指令"日志（`CommandRelayClient.java:325`），本缺口不成立。
- **证据**：`CommandChatHandlers.java:149-150,186-229`、`GroupTable.java:212-218`、`CommandRelayClient.java:288-329,311-314`。

### G-03（P1）全局性指令永不在发起组落地 —— **【对抗校验：CONFIRMED】**
- **要求**：L48 全局指令（`/time`、`/weather`、`/gamerule`、`/difficulty`）应在整个逻辑世界生效。
- **机制**（校验代理逐行核实）：触发=本地失败；下放=除发起组外的全部组；发起组**永不回灌**（`:190-193` `if (e.getKey().equals(originGroup)) continue;`）；客户端也没有"本地重放"路径——上送应答只有两支（`action=="teleport"` → 本地传送；非空 `message` → 纯显示 `sendSystemMessage`），**没有任何把 command 字符串回灌本地执行的路径**（全仓 `performPrefixedCommand` 只有 `CommandRelayClient.java:316` 一处，在入站转发执行面）；服务端三个回发起组的出口（`:128/267/273`）全是 `msg.reply` 应答，不会触发执行。
- **校验代理发现的一个防御性缺口（建议顺手修）**：若 `groups.groupOf(requester)` 返回 null（`:102`），则 `:191` 的 `e.getKey().equals(null)` 恒为 false → **发起组自己也会进 target 列表**。正常态不可达（主客户端建组即写反向索引，`GroupTable.java:43-53`），但建议把条件写成 `originGroup == null || e.getKey().equals(originGroup)`（一行防御）。
- **影响**：多组时各世界状态漂移（正是**发起者所在组**缺）；单组时全局指令完全无效（G-02③）。
- **关键次序约束**：**G-01 修好之后本缺口会换个形态存在**——本地成功即不再上送（`CommandRelayClient.java:167-174`），于是全局类**完全不传播**。⇒ **G-01 与"按类别下放"必须同一个变更集落地**，否则修完 A 反而丢掉多组一致性。
- **否证**：两组环境，A 组执行 `/time set day` 后 A 组时间也变（存在回灌/本地重放），或存在按类别判定而非"本地失败"的广播机制（全仓 grep 无）。

### G-04（P2）`/list` 特例不可达，"全网玩家列表"从未生效；`DESIGN.md:483` 声明与实现不符
- **机制**（[代理结论，已复核]）：`/list` 原版 0 级、无 `requires` → 本地必成功 → 探针 `anySuccess` → **不上送** → 服务端特例（`CommandChatHandlers.java:111-114`）成为死代码；玩家看到的是本组集成服的列表。
- **影响**：功能缺失 + 文档误导（下个会话按 DESIGN 修会找错根因）。
- **修法二选一**（见方案 §2 阶段 B）：把 `/list` 放进"仅上送/不下放"类别并让服务端应答替换本地输出；或**接受"本组视野"并改文档**。

### G-05（P2）3/4 级全局指令全链不可达（`/save-all`、`/stop`、`/op`、`/ban`…）
- **机制**（[代理结论，javap 核实 requires 等级]）：本地 0 → 失败；转发权限 `min(2, ops)`（`CommandChatHandlers.java:63-64,106-107`）；而物理服务端**从不执行世界指令**（全仓无物理端 `performPrefixedCommand`）→ 没有"服务端自己执行"这条路。
- **判定**：钳制到 2 是**有意的安全取舍**（`:41-44` 已文档化）→ 缺口不在钳制，而在**没有明文列出"不支持清单"**、也没有给用户点名的 `/save-all` 之类留专用通道。
- **建议**：本轮**记为"不支持"并写进 DESIGN**（含理由）；若将来要做，走"服务端→各组：刷盘"的专用小协议。

### G-09（P1）**玩家表从未被写入（恒空）** —— 三个"玩家表特例"全部失效 【第二轮校验新增，已由主会话独立复核】
- **要求**：L37/L48"服务器需要有一个玩家表来统计玩家实时位置以便于指令传输如同原版一样"。
- **机制**：`PlayerTable.put(PlayerEntry)` **没有任何调用者**；`new PlayerEntry(...)` 只出现在 `PlayerTable.java` 内部（`:22` 记录定义、`:55` 反序列化、`:115`/`:124` 两个 `computeIfPresent` 的 lambda）。全仓 grep `PlayerTable` 的生产使用只有：`RegistryHandlers.handlePosUpdate`（`:342` → `updatePosition` = `computeIfPresent`，**空表上恒 no-op**）、`CommandChatHandlers`（`byName`/`snapshot`）、`P2PServerService`（`players.remove`）、`MergeCoordinator`（`updateGroupRole`）。⇒ **表恒空**。
- **影响（连锁）**：
  1. `/list` 的服务端特例（`:111-114`）若被触发，返回的是"0 名玩家在线"；
  2. `/tp <玩家名>`（`:116-134`）、`/msg|/tell|/w <玩家名>`（`:135-148`）的 `byName` **恒为 null** → 全部落到泛化下放 → 对需要玩家上下文的形态必然失败 ⇒ **跨组 tp 与跨组私聊实际上不可用**；
  3. `GroupServerHooks.reportPositions`（每 20 tick 对组内全员发 `PLAYER_POS_UPDATE`）**是空转**：接收端 `updatePosition` 在空表上什么也不做 ⇒ 每秒全组无效上行；
  4. **文档与实现不符（新增两处）**：`PlayerTable.java:14` javadoc 声称"HELLO 登记、PLAYER_POS_UPDATE 刷新"——**"HELLO 登记"无实现**；`DESIGN.md:335` 承诺位置"上报服务端玩家表"——管道在、接收端恒 no-op。
- **否证**：若能找到 `PlayerTable.put(...)`（或任何构造 `PlayerEntry` 后写入）的调用点，或实测 `/list` 能列出其他组玩家、`/tp <他组玩家名>` 能生效，本缺口不成立。
- **注**：这正是"三个特例全是死代码"的根——它比 G-04（`/list` 不可达）更底层：**即使 `/list` 可达，表也是空的**。

### G-06（P2）权限/身份裁决建立在客户端自报之上 —— 【第二轮校验：CONFIRMED，但表述需修正】
- **修正后的机制**（校验代理逐行核实）：
  - **组归属是绑定了的**：`CommandChatHandlers.java:87 requester = conn.peerId()`、`:102 originGroup = groups.groupOf(requester)` ⇒ 我原先写的"未与 `conn.peerId` 绑定"**混淆了"组归属"与"玩家身份"**，实际未绑定的只有后者；
  - **身份由主客户端进程代报**：`CommandRelayClient.java:148-150` 在宿主进程里从 `ServerPlayer` 取 UUID/名字，`:185-189` 由宿主组包上送；物理服务端 `:92-94` 直接采信该 JSON 并用于 `:106-107` 的 ops 查询。**伪造面 = 宿主进程**（任何被改造/攻陷的组主客户端实例）——它本来就能在游戏内做任何事，但在"跨组"维度上它获得了他组世界的写入口。
- **影响**：已握手/被攻陷的主客户端可冒用 op 的 UUID 取得 permission=2、以他人名义发跨组私聊、把 `/tp` 目标指向任意玩家。**"钳到 2"的安全意图可被绕过**。
- **第二轮校验新增的两个面**：
  1. **`clientId` 冒用会踢掉真身**：`HelloHandler.java:134-139` 对同一 `clientId` 的重复握手直接 `previous.close()` ⇒ 可用他人 `clientId` 把在线玩家（含 op）踢下线（会话劫持/踢线 DoS 面）；
  2. **服务端在 Netty 线程读非并发 ops 名单**：`CommandChatHandlers.java:106-107` → `PlayerList.isOp` → `ServerOpList`（`Maps.newHashMap`，字节码已核），而 `/op` 的 `add` 在服务端线程 ⇒ 最坏短暂陈旧读（影响小，口径应与 A2 注入口统一）。
- **⚠️ 时序依赖（必须写进方案）**：**修好 G-09（玩家表落库）会同时"激活"今天完全惰性的坐标注入面** —— `RegistryHandlers.java:336` 用消息里自报的 `uuid` 调 `updatePosition`，今天因表恒空而无害；一旦表开始有数据，任何已握手连接即可刷写任意玩家坐标，**直接污染 `/tp` 落点**。⇒ **身份绑定必须与玩家表修复同批交付**，否则"修复本身"就是新攻击面的开关。
- **建议口径**（需拍板）：接受"客户端半可信"并**明文记录**（DESIGN 已有相关记述，需核对口径）；或做一层绑定（只接受**本组成员的 `uuid`**：以 `conn.peerId` 反查组 + 组表成员资格——注意它只能保证"同一连接自洽"，**挡不住同组内冒用 op 的 uuid**，不要高估）。

### G-07（P3）`/tp` 落点取 1Hz 快照
- 1Hz 上报（`GroupServerHooks.java:42,110-138`）+ 表内坐标直接用作落点（`CommandChatHandlers.java:123-126`）→ 移动中的目标最多约 1 秒位移偏差。原版用实时实体坐标。
- 若在意：`/tp` 特例改为"服务端向目标组主客户端**索要实时坐标**再应答"（多一次往返）。

### G-08（P3）`/msg` 文本提取用 `indexOf` 首个匹配 —— 【第二轮校验：CONFIRMED，示例已更正】
- `CommandChatHandlers.java:140`：`command.substring(command.indexOf(tokens[1]) + tokens[1].length()).trim()`。
- **更正我原来写错的示例**：`/msg tell hi`（目标名 `tell`）**不会**污染——`indexOf("tell")=5`（独立出现，落在实参位置）→ 截得 `hi` ✓。真正触发要求"**目标名是指令头/前缀的子串**"：例如目标名 `tell` 时执行 `/tell tell hi` → `indexOf("tell")=1`（落在 `/tell` 里）→ 截得 `"tell hi"` ✗；或目标名 `msg` 时 `/msg msg hi` ✗。另外 `:136-139` 还要求 `tokens.length>=3` 且目标在玩家表中（而 G-09 说表恒空 → **该分支今天根本走不到**，所以这是"修好玩家表之后才会显形"的缺陷）。
- 修法：按 Brigadier 解析结果取剩余参数（或从 `tokens[1]` 之后的固定偏移取）。

### 附带：文档/实现不符（必须一并修，否则下个会话会被误导）

| # | 文档声明 | 实现 | 判定 |
|---|---|---|---|
| 1 | `DESIGN.md:485-486`"逐个试执行" | **并发全量执行**（`CommandChatHandlers.java:212-228`，循环内不等待/不取消） | 【校验 CONFIRMED】 |
| 2 | `DESIGN.md:483` `/list` → 玩家表应答（全网玩家） | 特例不可达（G-04）；**且表恒空**（G-09） | 【校验 CONFIRMED】 |
| 3 | `DESIGN.md:487-489` 只说 `action=teleport` | 未写边界：**仅覆盖两段式** `/tp <玩家名>`，坐标式/`@s`/`execute` 型落泛化下放且两头落空 | 【校验 CONFIRMED】 |
| 4 | — | "过滤依据=本地失败"带来的过度下放后果：**DESIGN 未承认**（`fix-plan` L41 已记录）——**更正我原来写的"无任何文档承认"** | 【校验：子项 REFUTED 后更正】 |
| 5 | `PlayerTable.java:14` javadoc"HELLO 登记、PLAYER_POS_UPDATE 刷新" | **"HELLO 登记"无实现**（G-09） | 【新增】 |
| 6 | `DESIGN.md:335` 位置"上报服务端玩家表" | 管道在、接收端恒 no-op（G-09） | 【新增】 |

---

## 2.9 第二轮校验结论汇总（4 维补跑 + 4 个对抗校验，8 代理全部完成）

**新发现（本轮新增）**：

| 编号 | 结论 | 等级 | 备注 |
|---|---|---|---|
| **G-09** | **玩家表恒空**（`put` 无调用者）→ `/list`、`/tp <名>`、`/msg` 三个特例全失效；1Hz 上报空转 | **P1** | 已由我独立复核（grep + 代码） |
| A2-注入点 | 候选 1（`getOps()`/`sendPlayerPermissionLevel` 均 public）**可行**；候选 3（直接设 `ServerPlayer` 权限）**不可行**（无 setter，权限每次实时派生）；候选 2 无必要 | P2 | javap 核实；`PlayerList.op()` 内部用 `getOperatorUserPermissionLevel`（等级不可控，勿用） |
| **ops.json 无效** | **集成服从不加载 ops.json**（`IntegratedPlayerList.<init>` 只 super+setViewDistance；`PlayerList.<init>` 只 `new` 四个名单对象、无 load 调用——**我本人字节码复核**）⇒ 方案原 §5 的手工应急**作废** | 方案修正 | 集成服从不读该文件；`getOps().add()` 只是内存态（写文件是副作用，无回读路径） |
| F3+F4 vs 聊天栏 | **两条入口行为分裂**：F3+F4/F3+N 被 `hasPermissions(2)` 闸门静默拦截（不发包）；聊天栏 `sendCommand` **无权限门、无条件发包** → 同一条"切模式"操作在聊天栏会走上送链路、在快捷键下静默消失 | 机制 [已核实-字节码] | 影响验收设计：测试要用**聊天栏敲指令**，别用 F3+F4 |
| 失败可区分性 | `getExceptions` **在注入点可用**（`Commands.mapSource` 原样复制进 ParseResults）⇒ "只上送跨组类失败、不上送打字错误"**技术上可实现**（我/代理原写"不可达"被否证） | 供阶段 B 参考 | 但规范未要求区分，非必须 |
| `/execute`/`/function` 结算 | 原"anySuccess 恒真"的怀疑**被静态否证**：fork 派生源保留 `resultCallback`，内层失败会 `onResult(false,0)` ⇒ 探针能正确判失败 | — | 消除一个潜在盲区 |
| G-05 数值 | `SaveAllCommand requires=4`、`OpCommand requires=3`（javap 核实）> cap 2 ⇒ 全链不可达；"物理端从不执行世界指令"核实成立 | — | 补齐 G-05 依据 |
| 原版逃生门 | 暂停屏"对局域网开放"里的**允许作弊**可以调 `publishServer(gameMode, true, port)`（→ 权限 4）；但模组流程下该入口正常不可达（发布 LAN 后原版按钮已被替换） | — | 所以"改 level.dat allowCommands"是唯一的零代码应急 |

**被否证/需更正的（我原稿的问题）**：

1. 我引用的 `2026-10-14-stale-…md` §5 链条里 `publishServer → ClientboundEntityEventPacket(24+0)` **是错的**：`publishServer` 无任何 EntityEvent 调用（直接 `LocalPlayer.setPermissionLevel`）；`(24+n)` 由 `PlayerList.sendPlayerPermissionLevel` 从 `placeNewPlayer`/`respawn`/`op`/`deop` 发出 —— 已在本文档正文用正确链路（`… → setPermissionLevel → ClientboundEntityEventPacket` 仅作为**概念**表述存在风险，**以后者为准**）。
2. "F3+F4 被拒 → 客户端连指令都不发"只对 **F3+F4/F3+N** 成立，**不能泛化到聊天栏**（见上表）。
3. G-01 的 "本地权限恒为 0" 是**当前世界文件状态下的结论**，不是机制不变量：宿主改 `level.dat` 的 `allowCommands`（或用原版逃生门）即可拿到 ≥2 —— 这既是唯一应急手段，也说明"本地世界权威完全在宿主进程内"（G-06 的同类）。
4. G-06 的绑定描述、G-08 的示例（见各自小节）已按校验更正。
5. "无任何文档承认过度下放后果" → 更正为"**DESIGN 未承认**（fix-plan L41 已记录）"。

**校验者指出我漏掉的（已并入 G-06/G-09 与方案）**：`clientId` 冒用可踢真身（`HelloHandler.java:134-139`）；修 G-09 会激活坐标注入面（身份绑定必须同批）；服务端在 Netty 线程读非并发 ops 名单；`CommandRelayClient.reset()` 未覆盖所有拆除路径（2 秒限速戳可能残留到下一会话）；限速用 `System.currentTimeMillis()`（非单调钟，回拨期间静默拒绝所有上送）。

**轻微文档缺陷（校验 CONFIRMED，已在方案补记）**：`256` 长度上限、`2 秒/UUID` 限速的键与静默语义在 DESIGN/HANDOFF 均无记载；`CommandRelayClient` 的注释称"`/execute`/函数会重入 `performCommand`"**不成立**（真实重入源是命令方块/告示牌/服务端控制台与 mod 自己的 `performPrefixedCommand`）。

---

## 3. 我未能完成的复查（诚实标注）

| 维度 | 状态 | 备注 |
|---|---|---|
| ① 本地执行与权限污染 | 我本人复核（读码 + 实测日志），未做 javap 二次核对 | 前一轮会话已有 javap 结论（`publishServer → setPermissionLevel`），可信度较高 |
| ② 上送触发与限速 | 我本人复核（`CommandRelayClient` 全文） | 触发全集：非玩家指令/未接管/连接未开/超长(>256)/2 秒限速 → 跳过；失败判据只有 `CommandResultCallback` 的 success 位，**无法区分权限不足 vs 解析失败 vs 目标不在本组**（[推断]，未用 javap 验证原版是否另留可区分信号） |
| ③ 服务端面 | **代理完成（含 javap）** + 我抽查复核代码行 | 本文件 G-02～G-06 主要来自这里 |
| ④ 权限口径与 OP | 我本人复核；注入点清单已列但**签名未核实** | 见方案 §2 阶段 A（实现时必须 javap 核对） |
| ⑤ 玩家表与"如同原版" | 我本人复核 | 剩余差距：跨组选择器（明确记为范围外）、`/list`、1Hz 陈旧、身份自报 |

**对抗校验状态**：

| 结论 | 状态 | 方式 |
|---|---|---|
| G-01、G-02、G-03 | **已对抗校验：全部 CONFIRMED**（反驳点逐条排除） | 独立校验代理，javap 反汇编核实原版语义（`getProfilePermissions`/`isOp`/`publishServer`/`sendCommands`） |
| G-04、G-05、G-07、G-08 + 文档不符 1-4 | **已对抗校验：CONFIRMED**（并修正了 G-08 示例、文档不符#4 的表述） | 第二轮：4 维补跑 + 4 个对抗校验者（8 代理全部完成） |
| **G-09（玩家表恒空）** | **本轮新增，由我独立复核**（grep 全仓无 `put` 调用者、`PlayerEntry` 无外部构造） | 待下个会话用 F4 实测确认（见方案 §6） |
| G-06 | CONFIRMED 但**表述已按校验更正**（组归属已绑定 `conn.peerId`；未绑定的只有玩家身份） | 同上 |

即便如此，方案 §6 的**可实测否证用例 F1–F4 仍然要在下个会话开工前跑一遍**——那是端到端实证，与静态校验互补（F2/F3/F4 需要第二个客户端）。
