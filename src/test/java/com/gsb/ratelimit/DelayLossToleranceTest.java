package com.gsb.ratelimit;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 要求 5：同步消息延迟或丢失时，节点使用保守兜底额度，绝不超发。
 */
class DelayLossToleranceTest {

    private static final long LATENCY_MS = 30;

    private final ManualClock clock = new ManualClock(0);
    private final TimeGrid grid = new TimeGrid(100, 4);
    private final InMemoryTransport transport = new InMemoryTransport(clock);
    private final CentralCoordinator coordinator;
    private final List<RateLimiterNode> nodes = new ArrayList<>();

    DelayLossToleranceTest() {
        transport.setLatencyMillis(LATENCY_MS);
        coordinator = new CentralCoordinator(transport, clock, grid, 1000, 10, 10);
        for (int i = 0; i < 3; i++) {
            RateLimiterNode node = new RateLimiterNode("n" + i, transport, clock, grid, 10);
            node.join();
            nodes.add(node);
        }
        TestSupport.flushAll(transport, clock, LATENCY_MS);
        nodes.forEach(RateLimiterNode::timeSync);
        TestSupport.flushAll(transport, clock, LATENCY_MS);
    }

    private void hammer(RateLimiterNode node) {
        for (int i = 0; i < 500; i++) {
            node.tryAcquire();
        }
    }

    @Test
    void delayedSyncNeverCausesOvershoot() {
        hammerAll();
        flushAllAndReport();
        assertThat(coordinator.stats().reportedUsageCurrentWindow()).isLessThanOrEqualTo(1000);
        assertThat(coordinator.stats().overshoot()).isZero();

        clock.advanceMillis(grid.windowMillis()); // 进入窗口 1
        coordinator.tick(); // 基于窗口 0 历史重新分配
        TestSupport.flushAll(transport, clock, LATENCY_MS);
        // 节点统计应能观察到额度下发延迟
        assertThat(nodes.get(0).stats().lastAllocationLatencyMillis()).isGreaterThanOrEqualTo(LATENCY_MS);

        long admitted1 = hammerAll();
        flushAllAndReport();
        assertThat(admitted1).isLessThanOrEqualTo(coordinator.globalQuota());
        assertThat(coordinator.stats().overshoot()).isZero();

        // 中心记录的上报延迟
        assertThat(coordinator.stats().avgReportLatencyMillis()).isGreaterThanOrEqualTo((double) LATENCY_MS);
        assertThat(transport.droppedCount()).isZero();
    }

    @Test
    void messageLossDegradesToFallbackQuotaWithoutOvershoot() {
        transport.setDropEvery(3); // 约 1/3 消息丢失（含上报与额度下发）
        hammerAll();
        flushAllAndReport();

        clock.advanceMillis(grid.windowMillis());
        coordinator.tick();
        TestSupport.flushAll(transport, clock, LATENCY_MS);

        long admitted1 = hammerAll();
        // 核心不变式：无论丢失哪些消息，全局通过量都不超过窗口配额
        assertThat(admitted1).isLessThanOrEqualTo(coordinator.globalQuota());
        // 未收到窗口 1 额度的节点退化为兜底额度（每节点 10）
        long fallbackOnly = nodes.stream()
                .filter(n -> !n.currentWindowQuotaValid())
                .count();
        assertThat(fallbackOnly).isPositive();
        flushAllAndReport();
        assertThat(coordinator.stats().overshoot()).isZero();
        assertThat(transport.droppedCount()).isPositive();
    }

    @Test
    void totalAllocationLossFallsBackEntirely() {
        hammerAll();
        flushAllAndReport();

        clock.advanceMillis(grid.windowMillis());
        transport.setDropAll(true); // 窗口 1 的额度下发全部丢失
        coordinator.tick();
        TestSupport.flushAll(transport, clock, LATENCY_MS);

        long admitted1 = hammerAll();
        // 当前窗口额度全部丢失 -> 全部节点退化为兜底额度 10，合计 30 <= 兜底预留
        assertThat(admitted1).isEqualTo(3 * 10L);
        assertThat(admitted1).isLessThanOrEqualTo(coordinator.fallbackReserve());

        transport.setDropAll(false);
        clock.advanceMillis(grid.windowMillis()); // 窗口 2
        coordinator.tick();
        TestSupport.flushAll(transport, clock, LATENCY_MS);
        long admitted2 = hammerAll();
        assertThat(admitted2).isGreaterThan(0);
        assertThat(admitted2).isLessThanOrEqualTo(coordinator.globalQuota());
        assertThat(coordinator.stats().overshoot()).isZero();
    }

    private long hammerAll() {
        long before = nodes.stream().mapToLong(n -> n.stats().totalAdmitted()).sum();
        nodes.forEach(this::hammer);
        long after = nodes.stream().mapToLong(n -> n.stats().totalAdmitted()).sum();
        return after - before;
    }

    private void flushAllAndReport() {
        nodes.forEach(RateLimiterNode::flush);
        TestSupport.flushAll(transport, clock, LATENCY_MS);
    }
}
