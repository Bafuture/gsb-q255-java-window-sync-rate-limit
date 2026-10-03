# 分布式限流的滑动窗口同步

Pair-wise GSB 标注任务仓库（第 16 批 / 255）。

| 项目 | 内容 |
|------|------|
| 任务类型 | Feature 迭代 |
| 任务难度 | 困难 |
| 语言/框架 | Java, Maven, JUnit 5 |
| 环境可复现等级 | 无外部依赖 |
| 构建方式 | Maven（含 mvnw wrapper，无需本机安装 Maven） |

> 本仓库是**初始环境快照**：只有工程骨架，不含任何实现代码。
> 分支说明：`main` 为初始环境；`A`、`B` 为两次独立执行各自的工作分支，均从 `main` 的同一个提交拉出。

## 运行方式

```bash
./mvnw -q verify
```

## 任务提示词

以下为本题完整的 User Prompt 原文，两次执行必须使用完全相同的文本。

我们多节点共享限流额度，各节点本地滑动窗口的边界不一致，导致整体偶尔超出配额。请从零实现一个滑动窗口配额同步组件。仓库目前只有一个空的 Maven 工程（pom.xml 只声明 JUnit 5 与 AssertJ），节点与通信用内存对象模拟。要求：1) 支持把全局时间切成固定小格，各节点按格统计请求数并同步到中心；2) 支持窗口对齐：所有节点使用同一时间基准，需说明如何处理节点时钟偏差；3) 支持额度分配：中心按各节点历史用量分配当前窗口额度，节点超出本地额度要拒绝；4) 正确性：并发压测下全局通过量不得超过窗口配额，需有测试证明并给出误差；5) 支持同步延迟容忍：同步消息延迟或丢失时，节点要用保守策略（例如预留余量）避免超发；6) 支持节点动态加入退出，退出节点的未用额度要能回收；7) 提供统计：各节点额度、实际用量、拒绝数、同步延迟与超发误差；8) 测试覆盖窗口对齐、额度分配、并发不超发、延迟与丢失容忍、节点增删与统计；`mvn -q verify` 一条命令跑通。

## 提交要求

1. 在本仓库中完成提示词要求的全部内容。
2. `./mvnw -q verify` 必须通过。
3. 完成后在所属分支（A 或 B）上提交，产物快照的父提交必须是初始环境快照。

---

## 实现说明（分支 A）

代码位于 `src/main/java/com/example/gsb/ratelimit/`，测试位于 `src/test/java/com/example/gsb/ratelimit/`。

### 架构

- `WindowConfig`：小格长度 `slotMillis`、每窗口格数 `slotsPerWindow`、窗口配额 `windowQuota`、中心安全余量 `safetyHeadroom`、节点预留 `nodeReserve`。
- `Coordinator`（中心）：接收按格汇总的用量上报；窗口滚动或成员变化时按各节点**历史用量加权**分配当前窗口额度，下发 `QuotaGrant`；只分配 `windowQuota - safetyHeadroom`。
- `Node`（节点）：本地按小格维护滑动窗口计数（只保留最近 `slotsPerWindow` 格）；以 grant 为权威，超过本地有效额度（`grant - nodeReserve`）即拒绝；当前窗口无有效 grant 时 **fail-closed**。
- `MessageBus`：内存消息总线，支持固定延迟与确定性丢失（`droppingEveryNth`），统计投递/丢失/延迟。
- `Cluster`：仿真装配，用 `ManualTimeSource` 手动推进时间，保证测试确定性。

### 窗口对齐与时钟偏差处理

所有节点以**中心时间**为唯一时间基准。中心在下发的 `QuotaGrant` 中携带发出时刻的中心小格序号 `centerSlotIndex`，节点据此计算整数格偏移 `slotOffset = centerSlotIndex - 本地小格序号`，之后本地小格一律换算为 `原始小格 + slotOffset`。效果：

- 整数格级别（含跨多格）的时钟偏差被**完全校正**；
- 不足一格的残余偏差（本地格边界与中心格边界错开）无法用整数偏移消除，理论上界为 **±1 格**，由中心安全余量与节点预留吸收；窗口中点处 ±1 格不会跨越窗口边界，因此窗口序号在窗口内部一致。

### 不超发的论证与误差口径

中心每窗口分配总额恒等于 `windowQuota - safetyHeadroom`；节点严格按 `grant - nodeReserve` 放行，超本地额度即拒绝；grant 延迟或丢失时节点 fail-closed（旧窗口 grant 不延续）。因此任意窗口：

```
全局通过量 ≤ Σ(各节点有效额度) ≤ windowQuota - safetyHeadroom < windowQuota
```

超发误差 = 全局通过量 − 窗口配额 ≤ 0（恒成立，误差上界即安全余量 + 各节点预留）。并发压测（16 线程 × 1 万次 × 4 节点，共 16 万次尝试）实测：通过量 9500 = 可分配额度，误差 −500（−5%），利用率 95%，见 `ConcurrencyNoOvershootTest` 输出。

### 延迟/丢失容忍

- 中心侧：`safetyHeadroom` 永不分出去，吸收边界效应；
- 节点侧：`nodeReserve` 预留永不发出；
- grant 未到达/丢失：fail-closed，宁可拒绝也不超发；
- 上报丢失：中心按最近一次已知用量加权，不影响额度总量不变式。

### 节点增删

加入即发 `JoinRequest`，中心立即重平衡；退出发 `LeaveNotice`，中心回收其全部额度（含未用部分）并在存活节点间重新分配；退出节点本地残留的旧 grant 在下一窗口因窗口序号不匹配自动失效（fail-closed）。

### 统计

`Cluster.stats()` 汇总：各节点额度/有效额度/实际用量/拒绝数/同步 RTT、总线投递与丢失及延迟、中心授予总量、集群超发误差与利用率。

### 测试覆盖（19 个用例）

| 测试类 | 覆盖要求 |
|---|---|
| `WindowAlignmentTest` | 窗口对齐、时钟偏差校正、按格统计与滑动淘汰 |
| `QuotaAllocationTest` | 按历史用量分配、总额不变式、超本地额度拒绝、预留不发出 |
| `ConcurrencyNoOvershootTest` | 并发压测全局不超发并打印误差 |
| `SyncDelayLossToleranceTest` | 同步延迟/丢失容忍、fail-closed |
| `NodeMembershipTest` | 节点加入/退出、额度回收再分配 |
| `StatsReportingTest` | 各项统计字段与上报链路 |
