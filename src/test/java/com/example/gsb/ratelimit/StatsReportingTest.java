package com.example.gsb.ratelimit;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/** 统计：各节点额度、实际用量、拒绝数、同步延迟与超发误差。 */
class StatsReportingTest {

    @Test
    void clusterAndNodeStatsArePopulated() {
        WindowConfig config = new WindowConfig(100L, 10, 1000L, 100L, 10L);
        Cluster cluster = Cluster.create(config, 50L);
        Node a = cluster.addNode();
        Node b = cluster.addNode();
        cluster.tick();

        for (int i = 0; i < 12; i++) {
            cluster.advance(100L);
            for (int j = 0; j < 200; j++) {
                a.tryAcquire();
                b.tryAcquire();
            }
        }

        Stats.ClusterStats stats = cluster.stats();
        System.out.println("==== 集群统计 ====");
        System.out.println("窗口序号       = " + stats.windowIndex());
        System.out.println("总授予额度     = " + stats.totalGranted());
        System.out.println("总通过量       = " + stats.totalAllowed());
        System.out.println("总拒绝数       = " + stats.totalRejected());
        System.out.printf("超发误差       = %.0f (利用率 %.2f%%)%n",
                stats.overshoot(), 100.0 * stats.utilization());
        for (Stats.NodeStats ns : stats.nodes()) {
            System.out.printf("  %s: 额度=%d 有效=%d 已用=%d 拒绝=%d 平均同步RTT=%.1fms%n",
                    ns.nodeId(), ns.grantedQuota(), ns.effectiveQuota(),
                    ns.usedInWindow(), ns.rejectedTotal(), ns.avgSyncRttMillis());
        }
        System.out.printf("总线: 投递=%d 丢失=%d 平均延迟=%.1fms 最大延迟=%dms%n",
                stats.bus().delivered(), stats.bus().dropped(),
                stats.bus().avgLatencyMillis(), stats.bus().maxLatencyMillis());
        System.out.println("=================");

        // 节点级统计
        assertThat(stats.nodes()).hasSize(2);
        for (Stats.NodeStats ns : stats.nodes()) {
            assertThat(ns.grantedQuota()).isPositive();
            assertThat(ns.usedInWindow()).isLessThanOrEqualTo(ns.effectiveQuota());
            assertThat(ns.rejectedTotal()).isPositive();
            assertThat(ns.avgSyncRttMillis()).isGreaterThanOrEqualTo(0.0);
        }
        // 集群级统计
        assertThat(stats.totalGranted()).isLessThanOrEqualTo(stats.allocatableQuota());
        assertThat(stats.overshoot()).isLessThanOrEqualTo(0.0);
        assertThat(stats.utilization()).isBetween(0.0, 1.0);
        assertThat(stats.totalAllowed()).isPositive();
        // 中心与总线统计
        assertThat(stats.coordinator().nodeCount()).isEqualTo(2);
        assertThat(stats.coordinator().totalGranted()).isEqualTo(stats.totalGranted());
        assertThat(stats.bus().delivered()).isPositive();
        assertThat(stats.bus().maxLatencyMillis()).isBetween(50L, 150L);
    }

    @Test
    void usageReportsReachCoordinator() {
        WindowConfig config = new WindowConfig(100L, 10, 1000L, 100L, 10L);
        Cluster cluster = Cluster.create(config);
        Node node = cluster.addNode();
        cluster.tick();
        for (int i = 0; i < 42; i++) {
            node.tryAcquire();
        }
        cluster.tick();
        assertThat(cluster.coordinator().stats().totalReportedUsage()).isEqualTo(42L);
    }
}
