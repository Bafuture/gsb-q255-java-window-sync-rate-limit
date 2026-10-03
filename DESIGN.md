# 滑动窗口配额同步组件设计说明

## 架构

```
RateLimiterNode  ──UsageReport/Join/Leave/TimeSync──▶  CentralCoordinator
     ▲  ◀──AllocationUpdate/JoinResponse/TimeSyncResponse──  │
     └──────────────  InMemoryTransport（可注入延迟与丢包） ──┘
```

- `TimeGrid`：把全局时间轴切成固定小格（cell），每 `cellsPerWindow` 格构成一个窗口。
- `RateLimiterNode`：按格统计请求数，逐格上报；本地做真正的滑动窗口判定
  （最近 `cellsPerWindow` 格用量之和 < 本地额度才放行）。
- `CentralCoordinator`：汇总各格用量，按上一窗口历史用量比例（最大余数法）分配当前窗口额度。
- `InMemoryTransport`：内存消息通道，支持固定延迟、确定性丢包（每 N 条丢 1 条）与全丢，
  用于仿真同步延迟与消息丢失。

## 窗口对齐与节点时钟偏差

所有节点以**中心时钟**为唯一时间基准：

1. 加入时 `JoinResponse` 携带中心时间，节点做粗对齐；
2. 之后用 NTP 风格双向校时精对齐：`offset = centerTime - (t0 + t1) / 2`，
   残余误差不超过往返时延的一半（对称延迟下误差为 0，测试已验证）；
3. 生产环境应周期性重校以抵消时钟漂移。

关键点：**正确性不依赖对齐精度**。额度是硬上限且总额 ≤ 全局配额，
时钟偏差只影响利用率（请求被算到相邻格），永远不会导致超发。

## 不超发不变式

中心保证对任意窗口 w：

```
Σ 节点有效额度(w) ≤ globalQuota - fallbackReserve
fallbackReserve = maxNodes × minQuota   （预留给兜底与新节点加入）
```

- 额度按窗口编号生效，过期额度自动失效；
- 当前窗口额度未下发（延迟/丢失）时，节点退化为 `fallbackQuota`（≤ minQuota），
  全部节点同时兜底也不会突破 reserve；
- 因此**任意延迟、任意丢包下全局通过量 ≤ globalQuota，超发误差恒为 0**，
  代价是同步异常时利用率下降（保守策略）。

## 额度分配

- 每窗口翻转时按上一窗口各节点用量比例分配：先给每节点 `minQuota` 保底，
  剩余部分按权重以最大余数法分满 `distributableQuota`；
- 新节点中途加入：从未分配余量中先拿 ≤ minQuota 的保守额度，下一窗口参与正常分配；
- 节点退出（`LeaveNotice`）：中心立即回收其额度并对当前窗口重新分配；
  退出节点不再产生流量，不变式保持成立；
- 集群满（达到 maxNodes）时拒绝加入，未接入节点本地一律拒绝请求。

## 统计

- 节点侧 `NodeStats`：当前窗口额度、兜底额度、窗口用量、累计通过/拒绝、
  时钟偏移、校时 RTT、额度下发延迟；
- 中心侧 `GlobalStats`：全局/可分配/已分配额度、兜底预留、活跃节点数、
  当前窗口已上报用量、超发量与超发误差百分比、上报平均/最大延迟、
  重分配与额度回收次数、各节点额度与用量明细。

## 误差结论

并发压测（4 节点 × 8 线程 × 2 万次请求，共 64 万次）与延迟/丢失场景下，
实测全局通过量 = 已分配额度 900 ≤ 配额 1000，**超发量 0，超发误差 0.0000%**。
唯一"误差"是保守方向（少发），上界为 `fallbackReserve / globalQuota`（本配置 10%）。
