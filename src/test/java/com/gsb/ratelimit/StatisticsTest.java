package com.gsb.ratelimit;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** 要求 7：统计各节点额度、实际用量、拒绝数、同步延迟与超发误差。 */
class StatisticsTest {

    @Test
    void statsCoverQuotaUsageRejectionLatencyAndOvershoot() {
        ManualClock clock = new ManualClock(0);
        TimeGrid grid = new TimeGrid(100, 4);
        InMemoryTransport transport = new InMemoryTransport(clock);
        transport.setLatencyMillis(30);
        CentralCoordinator coordinator =
                new CentralCoordinator(transport, clock, grid, 1000, 10, 10);

        List<RateLimiterNode> nodes = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            RateLimiterNode node = new RateLimiterNode("n" + i, transport, clock, grid, 10);
            node.join();
            nodes.add(node);
        }
        TestSupport.flushAll(transport, clock, 30);
        nodes.forEach(RateLimiterNode::timeSync);
        TestSupport.flushAll(transport, clock, 30);
        coordinator.reallocateNow();
        TestSupport.flushAll(transport, clock, 30);

        for (int i = 0; i < 1000; i++) {
            nodes.forEach(RateLimiterNode::tryAcquire);
        }
        nodes.forEach(RateLimiterNode::flush);
        TestSupport.flushAll(transport, clock, 30);

        // 节点侧统计
        for (RateLimiterNode node : nodes) {
            NodeStats stats = node.stats();
            assertThat(stats.accepted()).isTrue();
            assertThat(stats.currentWindowQuota()).isEqualTo(450);
            assertThat(stats.windowUsage()).isEqualTo(450);
            assertThat(stats.totalAdmitted()).isEqualTo(450);
            assertThat(stats.totalRejected()).isEqualTo(550);
            assertThat(stats.lastSyncRttMillis()).isGreaterThanOrEqualTo(0);
            assertThat(stats.lastAllocationLatencyMillis()).isGreaterThanOrEqualTo(30);
        }

        // 中心侧统计
        GlobalStats global = coordinator.stats();
        assertThat(global.windowQuota()).isEqualTo(1000);
        assertThat(global.allocatedQuota()).isEqualTo(900);
        assertThat(global.fallbackReserve()).isEqualTo(100);
        assertThat(global.activeNodes()).isEqualTo(2);
        assertThat(global.reportedUsageCurrentWindow()).isEqualTo(900);
        assertThat(global.reportedUsageByNode()).containsEntry("n0", 450L)
                .containsEntry("n1", 450L);
        assertThat(global.quotaByNode()).containsEntry("n0", 450L)
                .containsEntry("n1", 450L);
        assertThat(global.avgReportLatencyMillis()).isGreaterThanOrEqualTo(30.0);
        assertThat(global.maxReportLatencyMillis()).isGreaterThanOrEqualTo(30);
        assertThat(global.reallocations()).isGreaterThanOrEqualTo(1);

        // 超发误差
        assertThat(global.overshoot()).isZero();
        assertThat(global.overshootErrorPct()).isZero();
        System.out.printf("[Stats] admitted=%d rejected=%d avgLatency=%.1fms overshoot=%d error=%.4f%%%n",
                global.reportedUsageCurrentWindow(),
                nodes.stream().mapToLong(n -> n.stats().totalRejected()).sum(),
                global.avgReportLatencyMillis(), global.overshoot(), global.overshootErrorPct());
    }
}
