package com.example.gsb.ratelimit;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/** 额度分配：按历史用量加权分配，总额不超过可分配额度，节点超本地额度即拒绝。 */
class QuotaAllocationTest {

    private static WindowConfig config() {
        return new WindowConfig(100L, 10, 1000L, 100L, 10L);
    }

    @Test
    void grantsSumToAllocatableQuota() {
        Cluster cluster = Cluster.create(config());
        Node a = cluster.addNode();
        Node b = cluster.addNode();
        Node c = cluster.addNode();
        cluster.tick();

        Stats.ClusterStats stats = cluster.stats();
        assertThat(stats.totalGranted()).isEqualTo(900L);
        assertThat(stats.totalGranted()).isLessThanOrEqualTo(stats.windowQuota());
        // 无历史用量时均分
        assertThat(a.stats().grantedQuota()).isEqualTo(300L);
        assertThat(b.stats().grantedQuota()).isEqualTo(300L);
        assertThat(c.stats().grantedQuota()).isEqualTo(300L);
    }

    @Test
    void allocationIsProportionalToHistoricalUsage() {
        Cluster cluster = Cluster.create(config());
        Node heavy = cluster.addNode();
        Node light = cluster.addNode();
        cluster.tick();

        // heavy 打满本地额度，light 不用
        while (heavy.tryAcquire()) {
            // drain
        }
        cluster.tick(); // 上报用量

        cluster.advance(1000L); // 进入下一窗口，触发按历史用量重新分配
        cluster.tick();

        Stats.NodeStats heavyStats = heavy.stats();
        Stats.NodeStats lightStats = light.stats();
        assertThat(heavyStats.grantedQuota()).isGreaterThan(lightStats.grantedQuota());
        assertThat(heavyStats.grantedQuota() + lightStats.grantedQuota()).isEqualTo(900L);
        // 轻量节点也至少有保底 1
        assertThat(lightStats.grantedQuota()).isGreaterThanOrEqualTo(1L);
    }

    @Test
    void nodeRejectsBeyondLocalEffectiveQuota() {
        Cluster cluster = Cluster.create(config());
        Node node = cluster.addNode();
        cluster.tick();

        long effective = node.stats().effectiveQuota(); // 1000 - 100(headroom 不分) -> 900 grant, -10 reserve
        assertThat(effective).isEqualTo(890L);

        long allowed = 0;
        while (node.tryAcquire()) {
            allowed++;
        }
        assertThat(allowed).isEqualTo(effective);
        assertThat(node.tryAcquire()).isFalse();
        assertThat(node.stats().rejectedTotal()).isPositive();
    }

    @Test
    void reserveIsNeverSpent() {
        Cluster cluster = Cluster.create(config());
        Node node = cluster.addNode();
        cluster.tick();
        long used = 0;
        while (node.tryAcquire()) {
            used++;
        }
        // 节点预留 10 个余量，永远不会被发出
        assertThat(node.stats().grantedQuota() - used).isEqualTo(10L);
    }
}
