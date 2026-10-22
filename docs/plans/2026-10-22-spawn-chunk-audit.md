# 2026-10-22 出生点常加载排查与验证

## 结论与修复

用户提供的 `minecraft-exported-logs-2026-10-22T20-26-26.log` 在世界加载前即崩溃。
第 64 行根因是 `ServerPlayerMixin` 构造器 HEAD 处理器不是 static，违反 Mixin
对 `super()` 之前注入的限制。处理器只使用参数与静态状态，已改为 static。

本地 Minecraft 1.20.4 字节码审计另确认两处永久出生点票据遗漏：

1. `prepareLevels` 的禁用条件遗漏 PROXY_SERVER。
2. `setDefaultSpawnPos` 会重新添加永久 START，`/setworldspawn` 或模组调用可绕过启动禁用。

启动与运行期入口现在共用 `SpawnChunkPolicy`：SERVER/PROXY_SERVER 按
`disableSpawnChunks`，已 arm 或接管的 CLIENT 始终禁用，普通单人世界保持原版。
`WrapWithCondition` 只阻止新增 START，保留旧票据清理、出生点坐标、朝向及广播。
沿用项目要求的 Fabric Loader 0.19.3 所附 MixinExtras，没有新增依赖。

新增方法调用注入点属于兼容性敏感的 Mixin 行为变更；协议版本、消息类型、
Mixin 配置清单及 Fabric 元数据均未修改。没有替换外部测试客户端中的模组。

## 验证

- 原代码真实启动：复现 `handler before super() invocation must be static`。
- 修改 static 后真实启动：不再发生该 Mixin 转换错误。
- 角色策略测试：旧门控下 18 例中 3 例按预期失败，修复后全部通过。
- `gradlew.bat build --console=plain`：成功；373 例测试，失败/错误/跳过均为 0。
- 真实冒烟环境：Minecraft 1.20.4、Fabric Loader 0.19.3、Java 17。
  临时测试模组读取实际 `DistanceManager` 票据，测试后自动停服；测试文件与世界
  位于忽略目录 `build/`，不随发行 JAR 打包。

| 场景 | 结果 |
| --- | --- |
| SERVER 禁用常加载，修复前调用 setDefaultSpawnPos | START=1，断言失败 |
| SERVER 禁用常加载，修复后启动和修改出生点 | START=0 |
| PROXY_SERVER 禁用常加载，实际启动和修改出生点 | START=0 |
| SERVER/PROXY_SERVER 配置关闭禁用，修改出生点 | START=1，保留排障入口 |
| 普通 CLIENT 模式，修改出生点 | START=1，保留原版行为 |
| CLIENT 已 arm，禁用配置为 true / false，修改出生点 | 均 START=0 |

还验证了出生点坐标/朝向继续更新、旧 START 被清除、运行中的显式强制加载可设置和解除。
CLIENT 角色在隔离服务端中通过设置角色和 `GroupRuntime.arm` 验证；未启动图形客户端，
未执行真实多人登录、死亡重生、合并或迁移。独立代码复查未发现本次修改的阻塞问题。

## 边界

- 结论是原版永久出生点 START 的两个添加入口均已覆盖，不表示出生点附近永远不加载。
  新建世界寻找安全地面、玩家实际出生/重生和视距区块仍然需要加载。
- 登录 `fudgeSpawnLocation` 拦截仍仅针对受管组世界。物理服务端登录期的正常加载
  不属于永久 START 常加载，不能据此一并禁止。
- 原先整体跳过 `prepareLevels` 会跳过已保存 `/forceload` 区块的启动恢复；
  这是已有边界，本次没有改变。
- SERVER 冒烟还发现环境快照读取锁定的 `world/session.lock` 失败，与票据断言独立；
  未纳入本次修复，本次冒烟不能解读为环境同步已通过。
- 未声称覆盖第三方模组绕过原版入口、自行直接添加 START 的行为。

本地详细记录：`build/tmp/spawn-smoke-before.log`、`build/tmp/spawn-smoke-after.log`、
`build/tmp/spawn-smoke-proxy.log`、`build/tmp/spawn-fix-build.log`。
