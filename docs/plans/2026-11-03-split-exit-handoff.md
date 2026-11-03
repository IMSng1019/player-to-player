# 合并后分离与主客户端退出交接修复计划

**目标：** 恢复合并后安全分离，并确保主客户端正常退出时，已经进入分离等待的成员仍参加接管。

**架构：** 保留中心服唯一所有权、元数据屏障和耐久退出协议。正常退出接替未提交的分离，不走非正常断线的失败通知；客户端等待按事务和来源组隔离，避免旧消息关闭新交接。

**技术：** Java 17、Fabric 1.20.4、Gradle、JUnit 5。

## 已核实的现场证据

- 中心日志 00:11:59 三人成组；00:14:23 big_eye97 传送到远处；00:14:34 分离 prepare 到达。
- 00:14:35 big_eye97 控制连接断开，同秒退出屏障只剩一名成员；00:14:49 Eznibuil 获得接管角色。
- 三份客户端 latest.log 都止于环境重启前，缺少该轮实际游戏/计算进程日志，不能据此断言客户端具体异常。
- 代码：EXIT_BEGIN → merges.abortInvolving → splits.abortInvolving → cancel(false) → SPLIT_FAILED，正常退出误用了异常断线处理。

## 实施与验证

1. `server/SplitCoordinatorTest` 复现分离准备与主退出相撞。修改 `SplitCoordinator`、`MergeCoordinator`、`P2PServerService`，为正常退出提供独立接替入口；保留组/区块、取消旧分离预约，阻止迟到 commit 和超时踢人。覆盖重复请求、提交已完成以及真正掉线。
2. `group/GroupHandoffPreparationTest` 复现同源分离等待无法升级退出等待。修改该状态类和 `client/session/WorldSession`，等待升级不再要求已有游戏世界；迟到旧事务失败与准备消息不能覆盖新等待。
3. 独立复核 `group/SplitMonitor`、元数据控制队列及加载区域规划，针对证实的卡点补行为回归并最小修复。
4. 运行定向回归、完整 `gradlew.bat build`、Java 17 真实 worker 冒烟；审查差异并在 `docs/validation/2026-11-03-split-exit-handoff.md` 记录结果、产物和仍需三机验证的范围。

现有 `docs/ENVIRONMENT_RUNTIME.md`、`player_to_player-prompt.txt` 用户修改保留；本次不提交或发布。
