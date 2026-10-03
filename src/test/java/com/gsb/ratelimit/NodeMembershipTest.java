package com.gsb.ratelimit;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** 要求 6：节点动态加入与退出，退出节点的未用额度被回收并重新分配。 */
class NodeMembershipTest {

    private final ManualClock clock = new ManualClock(0);
    private final TimeGrid grid = new TimeGrid(100, 4);
    private final InMemoryTransport transport = new InMemoryTransport(clock);
    private final CentralCoordinator coordinator =
            new CentralCoordinator(transport, clock, grid, 1000, 10, 10);

    private RateLimiterNode join(String id) {
        RateLimiterNode node = new RateLimiterNode(id, transport, clock, grid, 10);
        node.join();
        return node;
    }

    @Test
    void lateJoinGetsQuotaFromHeadroom() {
        join("a");
        join("b");
        coordinator.reallocateNow(); // a、b 分满 distributable
        assertThat(coordinator.stats().allocatedQuota()).isEqualTo(900);

        RateLimiterNode c = join("c"); // 中途加入：可分配余量已用完，先拿 0
        assertThat(c.stats().currentWindowQuota()).isEqualTo(0);
        assertThat(coordinator.stats().allocatedQuota()).isLessThanOrEqualTo(900);

        coordinator.reallocateNow(); // 重新均衡后 c 获得正常额度
        assertThat(c.stats().currentWindowQuota()).isGreaterThanOrEqualTo(10);
        GlobalStats stats = coordinator.stats();
        assertThat(stats.allocatedQuota()).isLessThanOrEqualTo(stats.distributableQuota());
        assertThat(stats.activeNodes()).isEqualTo(3);
    }

    @Test
    void leavingNodeQuotaIsReclaimedAndRedistributed() {
        RateLimiterNode a = join("a");
        RateLimiterNode b = join("b");
        RateLimiterNode c = join("c");
        coordinator.reallocateNow();
        long quotaBefore = b.stats().currentWindowQuota();
        assertThat(quotaBefore).isEqualTo(300);

        long reclaimsBefore = coordinator.stats().reclaimedQuotaEvents();
        a.leave();

        assertThat(coordinator.activeNodeCount()).isEqualTo(2);
        assertThat(coordinator.stats().reclaimedQuotaEvents()).isEqualTo(reclaimsBefore + 1);
        // a 的额度被回收并分给 b、c
        assertThat(b.stats().currentWindowQuota()).isEqualTo(450);
        assertThat(c.stats().currentWindowQuota()).isEqualTo(450);
        GlobalStats stats = coordinator.stats();
        assertThat(stats.allocatedQuota()).isEqualTo(900);
        assertThat(stats.allocatedQuota()).isLessThanOrEqualTo(stats.distributableQuota());
        // 退出后 a 不再计入
        assertThat(stats.quotaByNode()).doesNotContainKey("a");
    }

    @Test
    void clusterRejectsJoinBeyondCapacity() {
        ManualClock clock2 = new ManualClock(0);
        InMemoryTransport transport2 = new InMemoryTransport(clock2);
        CentralCoordinator small =
                new CentralCoordinator(transport2, clock2, grid, 100, 10, 2);
        RateLimiterNode n1 = new RateLimiterNode("x", transport2, clock2, grid, 10);
        RateLimiterNode n2 = new RateLimiterNode("y", transport2, clock2, grid, 10);
        RateLimiterNode n3 = new RateLimiterNode("z", transport2, clock2, grid, 10);
        n1.join();
        n2.join();
        n3.join();

        assertThat(n3.stats().accepted()).isFalse();
        assertThat(n3.tryAcquire()).isFalse(); // 未接入的节点一律拒绝，不影响全局不变式
        assertThat(small.activeNodeCount()).isEqualTo(2);
    }
}
