# 切组后本地 OP 丢失：诊断与下会话执行方案

- 日期：2026-10-19
- 状态：诊断完成，**未改代码**
- 现场：`test_log/26.10.17_2`（LXW1019 自建组 → 合并进 big_eye97 组）
- 一句话结论：**物理服 ops 名单一直是正确的（LXW1019=4）；丢失的是“主客户端集成服上的本地权限位”。**
  `ServerPlayConnectionEvents.JOIN` 的补落权钩子在 Fabric 事件时序上早于
  `PlayerList.players.add`，所以 JOIN 时按 UUID/名字都查不到刚加入的玩家；
  名单又因“签名未变化”不再周期重发，直到 23:42:02 另一名成员重连触发强制重推才补上。

---

## 0. 结论先行

| 问题 | 结论 |
|------|------|
| 物理服是否还把 LXW1019 当 OP？ | 是。服务端日志全程 `[LXW1019=4]`，合并后多次重推都包含他。 |
| 为什么切组后 F3+F4 说没权限？ | big_eye97 本机集成服的 `PlayerList.getOps()` 里没有 LXW1019；客户端 `hasPermissions(2)` 读的是这个本地权限位。 |
| 为什么本地落权没发生？ | 23:41:04 LXW1019 加入时，JOIN 回调里 `LocalPermissions.applyNow` 按 UUID 和名字都查不到玩家（Fabric JOIN 早于原版 `players.add`），静默跳过且没有重试。 |
| 为什么 23:42:02 又好了？ | Eznibuil 重连触发 `HelloHandler.pushForClient` 强制重推名单；此时 LXW1019 已在玩家列表里，落权成功。**属于偶然被另一名成员“救”了，不是自愈机制。** |
| 自组里为什么感觉有 OP？ | 物理服 OP + 指令上链（物理服裁定、广播执行）在自组里是通的；F3+F4 的本地权限位是另一条路径。本轮日志里 LXW1019 自组也没有 `本地权限位已落地` 记录，说明本地权限位当时也没有稳定落地，只是没有被触发/暴露。下轮测试第 C 场景专门确认这一点。 |

---

## 1. 现场时间线（证据）

### 1.1 服务端（物理服，UTF-8）

`test_log/26.10.17_2/server/latest (4).log`

| 时间 | 行号 | 事件 |
|------|------|------|
| 23:38:42 | 135 | `权限已下发: 组 9207ce8d… → 1 名成员（[LXW1019=4]）` |
| 23:40:40 | 397 | 合并提交后：`组 914b5cdf… → 3 名成员（[LXW1019=4, Eznibuil=0, big_eye97=0]）` |
| 23:41:01 | 418/430 | 两次重推，仍为 `[LXW1019=4, Eznibuil=0, big_eye97=0]` |
| 23:41:04 | — | LXW1019 加入 big_eye97 的集成服世界（big_eye97 日志） |
| 23:42:02 | 577/586 | Eznibuil 重连 HELLO 触发两次强制重推，仍为 `[LXW1019=4, …]` |
| 23:43:52 | 950 | `Made Eznibuil a server operator` |
| 23:43:55 | 960 | 重推 `[LXW1019=4, Eznibuil=4, big_eye97=0]` |
| 23:44:27 | 1074 | `Made big_eye97 a server operator` |
| 23:44:30 | 1087 | 重推 `[LXW1019=4, Eznibuil=4, big_eye97=4]` |

**要点：物理服从未把 LXW1019 的等级算错。**

### 1.2 big_eye97（宿主，集成服侧）

`test_log/26.10.17_2/cilent/big_eye97/latest (4).log`

| 时间 | 行号 | 事件 |
|------|------|------|
| 23:40:40 | 1755 | `收到权限名单: 组 914b5cdf… → 3 名成员` |
| 23:40:50 | 1760 | `本地权限位已落地: big_eye97 → 等级 0`（服务端主线程刚从 50 秒冻结中恢复） |
| 23:41:01 | 1765 | `收到权限名单 … 3 名成员` |
| 23:41:02 | 1769 | `收到权限名单 … 3 名成员` |
| **23:41:04** | **1782** | **`LXW1019加入了游戏`** |
| **23:41:04 之后** | — | **没有 `本地权限位已落地: LXW1019`** |
| 23:42:02 | 1786/1787 | `收到权限名单` + `本地权限位已落地: LXW1019 → 等级 4` |

补充：23:40:50 有一条 `Can't keep up! Running 50604ms or 1012 ticks behind`，说明合并切换期间宿主主线程冻结了约 50 秒；这解释了 big_eye97 自己的落权为什么从 23:40:40 拖到 23:40:50，但**不是** LXW1019 这次 58 秒空窗的主因。

### 1.3 LXW1019（副客户端）

`test_log/26.10.17_2/cilent/LXW1019/latest.log`

| 时间 | 行号 | 事件 |
|------|------|------|
| 23:38:42 | 141 | `收到权限名单: 组 9207ce8d… → 1 名成员`（自组） |
| 23:41:04 | 200 | `经 P2P 隧道加入主客户端组世界` |
| **23:41:23/24** | **202–204** | **`[调试]：你没有权限打开游戏模式切换器` ×3** |
| 23:42:39 | 207 | `已将自己的游戏模式设置为创造模式`（指令上链，物理服裁定后执行） |
| 23:42:46 | 208 | `已传送到 Eznibuil`（同上） |
| 23:42:52 | 210 | `已将自己的游戏模式设置为生存模式`（同上） |

**关键区分：**
- 23:41:23 失败的是 **F3+F4 本地权限闸门**（客户端 `hasPermissions(2)` 读本地权限位）；
- 23:42:39 成功的是 **指令上链**（物理服按自己的 ops 名单裁定，广播给组执行）。
  两者不是同一条权限路径，所以“指令能用”不能证明本地权限位已经落地。

---

## 2. 根因

### D1（P0）：JOIN 补落权发生在玩家进入玩家列表之前

代码位置：

- `src/main/java/imsng/player_to_player/group/GroupServerHooks.java:79-85`
  ```java
  ServerPlayConnectionEvents.JOIN.register((handler, sender, server) -> {
      if (GroupRuntime.isManagedServer(server)) {
          LocalPermissions.applyScheduled(server);
          CommandRelayClient.flushPendingFeedback(handler.player);
      }
  });
  ```
- `src/main/java/imsng/player_to_player/group/LocalPermissions.java:137-170`
  `applyScheduled` 在服务器线程上直接调用 `applyNow`；`applyNow` 先
  `playerList.getPlayer(uuid)`，再 `getPlayerByName(name)`，两者都为 null 就
  `continue`（注释写着“JOIN 事件会再触发一次”，但 JOIN 本身就是触发点，之后没有重试）。

Fabric 事件时序（已用字节码核对）：

- Fabric API `fabric-networking-api-v1 3.1.9` 的
  `net.fabricmc.fabric.mixin.networking.PlayerManagerMixin` 把 `JOIN` 回调注入在
  `PlayerAbilitiesS2CPacket.<init>`（Mojang 映射：`ClientboundPlayerAbilitiesPacket.<init>`）
  的调用点。
- 1.20.4 原版 `PlayerList.placeNewPlayer` 的字节码顺序：
  - 偏移 430–441：构造并发送 `ClientboundPlayerAbilitiesPacket`；**JOIN 回调在这里触发**；
  - 偏移 656–667：`players.add(player)` / `playersByUUID.put(uuid, player)`。
- 因此 JOIN 回调执行时，`playerList.getPlayer(uuid)` 和 `getPlayerByName(name)` **必然查不到**
  这个刚登录的玩家。

复现命令（本次取证用）：

```bash
javap -c -p <loom-cache>/minecraft-merged-1.20.4-…jar \
  net.minecraft.server.players.PlayerList
```

在输出中对比 `ClientboundPlayerAbilitiesPacket.<init>` 与 `players.add` 的偏移即可。

**与日志的对应：** big_eye97 在 23:41:04 收到 JOIN，但直到 23:42:02 才出现
`本地权限位已落地: LXW1019`；中间没有任何重试。

### D2（P1）：早到的权限名单被宿主启动时的 reset 清掉

代码位置：

- `src/client/java/imsng/player_to_player/client/group/GroupHost.java:137`
  `start()` 开头调用 `reset()`。
- `src/client/java/imsng/player_to_player/client/group/GroupHost.java:247`
  `reset()` 调用 `LocalPermissions.reset()`，清空 `groupId`、`members`、`applied`。

与本轮日志的对应（LXW1019 自组）：

1. 23:38:42 收到名单（`LXW1019=4`）→ `members` 已写入；
2. 23:38:43 集成服启动；
3. 23:38:44 `GroupRuntime.tryAttach` → `GroupHost.start` → `reset()` → **名单被清空**；
4. 23:38:46 `publish()` 调用 `LocalPermissions.applyScheduled(server)`，此时 `members` 为空，
   `applyNow` 直接 return；
5. 结果：LXW1019 作为宿主的本地权限位也没有落地（他的日志里没有
   `本地权限位已落地: LXW1019`）。

这条缺陷不影响“切组后丢失”的直接归因，但它解释了为什么自组里也没有稳定的本地权限位，
并且会让任何“名单比集成服早到”的场景都落空。

### D3（P1，放大器）：名单无变化时周期 sweep 不重发

代码位置：

- `src/main/java/imsng/player_to_player/server/PermissionRelay.java:181-199`
  每 5 秒 `sweep`，但 `pushNow` 内部先比较签名。
- `src/main/java/imsng/player_to_player/server/PermissionRelay.java:222-225`
  ```java
  String sig = signature.toString();
  if (sig.equals(lastPushed.get(groupId))) {
      return; // 无变化：不发
  }
  ```
- 强制重推只发生在：
  - `HelloHandler.java:181-184`（HELLO 重连，`pushForClient`）；
  - `RoleAssignHandler.java:154-156 / 189-190`（角色指派，`refreshForClient`）；
  - `MergeCoordinator` 合并提交（`permissions.push`）。

因此 D1 漏掉一次落权后，**没有自愈路径**；本轮纯属 Eznibuil 在 23:42:02 重连，
才触发了对同一个组的强制重推。若当时没有第三人重连，LXW1019 会一直保持本地等级 0。

---

## 3. 影响面

1. **所有副客户端**：只要加入时宿主已经收到过名单，JOIN 补落权就会失败；之后能否恢复取决于
   是否有人重连、是否有 `/op` `/deop` 改变签名、是否发生新的角色指派/合并。
2. **宿主自己**：名单比集成服早到（本轮 23:38:42 → 23:38:44）时，会被 D2 清掉；
   本轮宿主也没有本地等级。
3. **权限相关的客户端功能**：F3+F4 游戏模式切换、指令补全树、未被拦截的本地执行路径
   （告示牌/命令方块/第三方程序化调用）都受影响。
4. **指令上链不受影响**：`/gamemode`、`/tp` 等仍由物理服按 ops 名单裁定后广播执行，
   所以会出现“指令能用、F3+F4 不能用”的割裂现象。

---

## 4. 下个会话执行方案

### 4.1 修复顺序（建议同批提交，但按下列顺序实现/验证）

#### F1（P0）JOIN 时直接用 `handler.player` 落权

不依赖玩家列表查找，直接对 JOIN 回调手里的 `ServerPlayer` 落权：

- 在 `LocalPermissions` 增加：
  ```java
  public static void applyJoiningPlayer(MinecraftServer server, ServerPlayer player)
  ```
  逻辑：
  1. 校验 `GroupRuntime.activeGroupId()` 与 `groupId` 一致、`members` 非空；
  2. 先按 `player.getUUID()` 匹配，失败再按 `player.getGameProfile().getName()`
     （`equalsIgnoreCase`）匹配；
  3. 找到后调用现有 `applyOne(server, playerList, player, level)`；
  4. 找不到时记一条 INFO/DEBUG（见 F4），不要静默。
- `GroupServerHooks.JOIN` 改为：
  ```java
  LocalPermissions.applyJoiningPlayer(server, handler.player);
  LocalPermissions.applyScheduled(server); // 其余已在线成员
  ```
- 为什么可行：`applyOne` 只需要 `ServerPlayer`、`PlayerList.getOps()` 和
  `sendPlayerPermissionLevel(player)`；这些都不要求玩家已经在 `players` 列表里。
  原版 `placeNewPlayer` 在偏移 487 还会再调用一次 `sendPlayerPermissionLevel`，
  那时会读到我们已经写好的 ops 条目，因此不会把等级覆盖回 0。

#### F2（P0 兜底）JOIN 后延迟一 tick 再整表落一次

- 在 JOIN 回调里追加：
  ```java
  server.execute(() -> LocalPermissions.applyScheduled(server));
  ```
  该任务会在当前 `placeNewPlayer` 返回后、下一次服务器任务队列执行时运行；
  此时 `players.add` 已完成，整表查找必然能看到新玩家。
- 这是对 F1 的防御：即使 F1 的直落路径因未来版本事件顺序变化失效，延迟路径仍能补上。
- 可选加强：`LocalPermissions` 维护一个“未落地成员”集合，在
  `ServerTickEvents.END_SERVER_TICK` 里做有界重试（例如最多 200 tick），
  全部落地或超时后停止并记一条日志。若不想引入 tick 钩子，F1+F2 已足够。

#### F3（P1）宿主启动时不要清空早到的名单

- 把 `GroupHost.reset()` 里的 `LocalPermissions.reset()` 改为
  `LocalPermissions.onServerInstanceChanged()`：
  - 只清 `applied`（新集成服实例的 ops 表是新的，不能沿用旧的“已落地”判断）；
  - 保留 `members` 与 `groupId`；`applyNow` 已有“组不匹配就忽略”的保护，
    新组的名单到达后会整表替换。
- 完整的 `LocalPermissions.reset()` 仍保留在：
  - `GroupRuntime.detach()`（`GroupRuntime.java:246`，会话结束/换世界）；
  - `WorldSession` / `MergeClient` 触发的宿主 reset 之外的全量拆除路径。
- 这样 23:38:42 的名单可以活到 23:38:46 `publish()`，宿主本地等级立刻落地。

#### F4（P2）诊断补齐

- `applyNow` 每次名单版本只记一条汇总：
  `权限名单落地: 2/3（未在线: Eznibuil/98cde…）`，避免每 tick 刷屏。
- `memberFor` 找不到时记清楚是 UUID 不匹配还是名字不匹配。
- 名单版本可用 `groupId + members.size() + 内容哈希` 或简单的递增序号标识。
- 这些日志是下轮验收的直接证据，也能避免本次这种“静默 continue”再次发生。

### 4.2 测试

#### 单元测试（沿用现有纯静态 helper 模式）

测试环境无法构造 `ServerPlayer`（参见 `SplitMonitorGeometryTest` 的注释），
所以把可测逻辑抽成纯函数：

- `LocalPermissionsParseTest` 已覆盖 `parseMembers`，保留。
- 新增 `LocalPermissionsApplyTest`：
  1. `memberFor(table, uuid, name)`：UUID 优先；UUID 不中时按名字（大小写不敏感）；
     都找不到返回 null。
  2. 等级 0 → 撤权；等级 ≥1 → 加 `ServerOpListEntry` 并 `sendPlayerPermissionLevel`。
  3. 「先收到名单、玩家后加入」：模拟 lookup 第一次返回 null、第二次返回玩家，
     断言第二次落权成功（对应 F2 的延迟路径）。
  4. 「名单早于集成服启动」：`onServerInstanceChanged()` 后 `members` 仍在，
     `applied` 已清空（对应 F3）。
- 如果不想为 `PlayerList` 造 fake，可把“查找成员”和“是否需要落权”抽成
  `static Member memberFor(...)` / `static boolean needsApply(...)`，
  落权动作仍由集成测试覆盖。

#### 集成验收（必须按场景跑，不只看单测）

| 场景 | 步骤 | 通过标准 |
|------|------|----------|
| **A（本次 bug 复现）** | A 是 OP，自建组为主；B 非 OP，自建组为主；A 合并进 B 的组；C 全程离线 | B 的日志在 `A加入了游戏` 后 **1 秒内**出现 `本地权限位已落地: A → 等级 4`；A 的 F3+F4 立即可用；不依赖 C 重连 |
| **B（名单先到、人后到）** | B 的宿主先收到含 A 的名单（A 此刻离线），随后 A 再加入 | 同上 |
| **C（宿主自己）** | A 自建组，名单在集成服启动前到达 | A 的日志在 `集成服务端已发布 LAN 端口` 后出现 `本地权限位已落地: A → 等级 4`；不再需要等第二次推送 |
| **D（在线改 op）** | 成员在线时物理服 `/op`、`/deop` | 本地等级在 5 秒内跟随（现有 frame 路径应已通过） |
| **E（不泄漏）** | 合并/分离后检查各组 | 非 OP 成员保持等级 0；A 的等级不会落到别组玩家身上 |

验收日志 grep：

```bash
# 宿主侧：落权时间必须紧跟加入时间
grep -n '加入了游戏\|本地权限位已落地' <host>/latest.log

# 副客户端侧：加入后不应再出现
grep -n '你没有权限打开游戏模式切换器' <secondary>/latest.log

# 物理服侧：名单应始终包含 OP
grep -n '权限已下发' <server>/latest.log
```

命令：

```bash
./gradlew test
./gradlew build
```

#### 验收口径（DoD）

1. 副客户端加入任意组后，本地权限位在 **1 tick / 1 秒内**落地；
2. 不依赖其他成员重连或 `/op`/`/deop` 触发重推；
3. 宿主自己的本地权限位在 LAN 发布后立即落地；
4. 组间不泄漏权限；
5. `./gradlew test` 全绿，日志能看到 F4 的汇总行。

### 4.3 涉及文件

| 文件 | 改动 |
|------|------|
| `src/main/java/imsng/player_to_player/group/LocalPermissions.java` | 新增 `applyJoiningPlayer`、`memberFor`、`onServerInstanceChanged`；`applyNow` 增加未命中诊断/汇总 |
| `src/main/java/imsng/player_to_player/group/GroupServerHooks.java` | JOIN 回调改为“直落 + 延迟一 tick 整表” |
| `src/client/java/imsng/player_to_player/client/group/GroupHost.java` | `reset()` 不再清 `members`；改用 `onServerInstanceChanged()` |
| `src/test/java/imsng/player_to_player/group/LocalPermissionsApplyTest.java`（新增） | 纯函数/查找/待落地状态测试 |
| `docs/plans/2026-10-19-local-permission-lost-on-group-switch.md` | 本文件 |

### 4.4 实施注意

- **版本核对**：本轮日志是 10-17 的构建；当前 `build/libs/player_to_player-1.0.2.jar`
  已包含 10-19 的登录锚点改动。下轮必须用新构建跑，并在客户端日志里核对
  `player_to_player 1.0.2` 与 jar 时间戳，避免拿旧 jar 复测。
- **不要顺手改指令路由**：本次问题与 `CommandChatHandlers` 的权限裁定无关；
  物理服 `levelSnapshot` 全程正确。修复范围限定在本地权限位落地。
- **`StoredUserList.add` 的副作用**：`LocalPermissions` 加 ops 条目会触发
  `ops.json` 写入集成服世界目录，这是既有行为（类注释已说明无回读路径），
  本次不改。
- **名字兜底的安全边界**：UUID 优先；名字兜底只用于 `equalsIgnoreCase` 的唯一匹配；
  若未来出现同名（原版通常不允许），跳过并告警，不能把 OP 给错人。
- **提交信息**：按仓库既有约定，不加 `Co-Authored-By` 尾行。

---

## 5. 待确认（只有你能回答的部分）

1. **自组里“有 OP”的具体表现**：是 F3+F4 能打开，还是只用过 `/gamemode` 等指令？
   - 如果是后者，说明你感知到的是“指令上链”的物理服 OP，本地权限位当时可能也没落地
     （与本轮日志一致），F3 的修复仍然必要。
   - 如果自组里 F3+F4 确实能用，说明还有一条本轮日志没覆盖到的本地落权路径；
     下轮场景 C 复测即可确认。
2. **是否接受 F2 的 tick 级有界重试**：F1+F2 已能覆盖本次问题；
   额外的 tick 重试会增加一点复杂度，但对未来事件顺序变化更稳。建议先做 F1+F2+F3，
   把 tick 重试留作观察后的加强项。

---

## 6. 证据附录

### 6.1 关键日志原文

服务端：

```
[23:40:40] [Server thread/INFO]: 权限已下发: 组 914b5cdf-1d60-4d6e-85fd-23fdc0edc73a → 3 名成员（[LXW1019=4, Eznibuil=0, big_eye97=0]）
[23:41:01] [Server thread/INFO]: 权限已下发: 组 914b5cdf-… → 3 名成员（[LXW1019=4, Eznibuil=0, big_eye97=0]）
[23:42:02] [Server thread/INFO]: 权限已下发: 组 914b5cdf-… → 3 名成员（[LXW1019=4, Eznibuil=0, big_eye97=0]）
```

big_eye97：

```
[23:40:40] [p2p-netty-client-1/INFO]: 收到权限名单: 组 914b5cdf-… → 3 名成员
[23:40:50] [Server thread/INFO]: 本地权限位已落地: big_eye97 → 等级 0（服务端 ops 名单口径）
[23:41:01] [p2p-netty-client-1/INFO]: 收到权限名单: 组 914b5cdf-… → 3 名成员
[23:41:02] [p2p-netty-client-1/INFO]: 收到权限名单: 组 914b5cdf-… → 3 名成员
[23:41:04] [Server thread/INFO]: LXW1019加入了游戏
[23:42:02] [p2p-netty-client-1/INFO]: 收到权限名单: 组 914b5cdf-… → 3 名成员
[23:42:02] [Server thread/INFO]: 本地权限位已落地: LXW1019 → 等级 4（服务端 ops 名单口径）
```

LXW1019：

```
[23:38:42] [p2p-netty-client-1/INFO]: 收到权限名单: 组 9207ce8d-… → 1 名成员
[23:41:04] [Render thread/INFO]: 经 P2P 隧道加入主客户端组世界，物理服务端会话保持
[23:41:23] [Render thread/INFO]: [System] [CHAT] [调试]： 你没有权限打开游戏模式切换器
[23:41:24] [Render thread/INFO]: [System] [CHAT] [调试]： 你没有权限打开游戏模式切换器
[23:41:24] [Render thread/INFO]: [System] [CHAT] [调试]： 你没有权限打开游戏模式切换器
[23:42:39] [Render thread/INFO]: [System] [CHAT] 已将自己的游戏模式设置为创造模式
```

### 6.2 代码位置速查

| 位置 | 作用 |
|------|------|
| `GroupServerHooks.java:79-85` | JOIN 补落权入口 |
| `LocalPermissions.java:137-151` | `applyScheduled`：同线程直跑 / 异线程 execute |
| `LocalPermissions.java:154-171` | `applyNow`：按 UUID/名字查找，查不到静默 continue |
| `LocalPermissions.java:174-192` | `applyOne`：写 ops 表 + `sendPlayerPermissionLevel` |
| `GroupHost.java:137` | `start()` 调用 `reset()` |
| `GroupHost.java:216` | LAN 发布后 `applyScheduled(server)` |
| `GroupHost.java:247` | `reset()` 清空 `LocalPermissions` |
| `PermissionRelay.java:222-225` | 签名未变化则不重发 |
| `HelloHandler.java:181-184` | HELLO 强制重推 |
| `RoleAssignHandler.java:154-156` | 副客户端角色指派后强制重推 |
