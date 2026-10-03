package com.example.gsb.ratelimit;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/** 节点动态加入/退出：加入即参与分配，退出节点的未用额度被回收再分配。 */
class NodeMembershipTest {

    private static WindowConfig config() {
        return new WindowConfig(100L, 10, 1000L, 100L, 10L);
    }

    @Test
    void joiningNodeGetsShareImmediately() {
        Cluster cluster = Cluster.create(config());
        Node a = cluster.addNode();
        Node b = cluster.addNode();
        cluster.tick();
        assertThat(a.stats().grantedQuota()).isEqualTo(450L);
        assertThat(b.stats().grantedQuota()).isEqualTo(450L);

        Node c = cluster.addNode();
        cluster.tick();
        assertThat(cluster.coordinator().members()).containsExactlyInAnyOrder(
                a.id(), b.id(), c.id());
        assertThat(a.stats().grantedQuota()).isEqualTo(300L);
        assertThat(b.stats().grantedQuota()).isEqualTo(300L);
        assertThat(c.stats().grantedQuota()).isEqualTo(300L);
        assertThat(cluster.stats().totalGranted()).isEqualTo(900L);
    }

    @Test
    void leavingNodeQuotaIsReclaimedAndRedistributed() {
        Cluster cluster = Cluster.create(config());
        Node a = cluster.addNode();
        Node b = cluster.addNode();
        Node c = cluster.addNode();
        cluster.tick();
        assertThat(cluster.stats().totalGranted()).isEqualTo(900L);

        cluster.removeNode(b.id());
        cluster.tick();

        assertThat(cluster.coordinator().members()).containsExactlyInAnyOrder(a.id(), c.id());
        assertThat(cluster.coordinator().grantOf(b.id())).isZero();
        // 退出节点的 300 额度被回收，重新分给存活节点
        assertThat(a.stats().grantedQuota()).isEqualTo(450L);
        assertThat(c.stats().grantedQuota()).isEqualTo(450L);
        assertThat(cluster.coordinator().stats().totalGranted()).isEqualTo(900L);
    }

    @Test
    void departedNodeGrantExpiresAtNextWindow() {
        Cluster cluster = Cluster.create(config());
        Node a = cluster.addNode();
        Node b = cluster.addNode();
        cluster.tick();
        // b 本地仍持有旧 grant，本窗口内还能用（额度已被中心回收前发出的部分）
        assertThat(b.tryAcquire()).isTrue();

        cluster.removeNode(b.id());
        cluster.tick();
        cluster.advance(1000L); // 窗口滚动，旧 grant 失效
        cluster.tick();

        // b 的 grant 属于旧窗口，fail-closed
        assertThat(b.tryAcquire()).isFalse();
    }

    @Test
    void rebalancingHappensOnMembershipChanges() {
        Cluster cluster = Cluster.create(config());
        cluster.addNode();
        cluster.tick();
        int before = cluster.coordinator().stats().rebalanceCount();
        Node late = cluster.addNode();
        cluster.tick();
        cluster.removeNode(late.id());
        cluster.tick();
        assertThat(cluster.coordinator().stats().rebalanceCount()).isGreaterThan(before);
    }
}
