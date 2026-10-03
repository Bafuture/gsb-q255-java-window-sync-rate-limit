package com.gsb.ratelimit;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** 要求 3：中心按各节点历史用量分配当前窗口额度，节点超出本地额度必须拒绝。 */
class QuotaAllocationTest {

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
    void quotaIsProportionalToHistoricalUsage() {
        RateLimiterNode a = join("a");
        RateLimiterNode b = join("b");
        RateLimiterNode c = join("c");
        coordinator.reallocateNow(); // 无历史时均分：各 300

        for (int i = 0; i < 1000; i++) {
            a.tryAcquire();
        }
        for (int i = 0; i < 1000; i++) {
            b.tryAcquire();
        }
        // c 不产生流量
        assertThat(a.stats().totalAdmitted()).isEqualTo(300);
        assertThat(b.stats().totalAdmitted()).isEqualTo(300);
        a.flush();
        b.flush();
        c.flush();

        clock.advanceMillis(grid.windowMillis()); // 进入窗口 1
        coordinator.tick();

        NodeStats sa = a.stats();
        NodeStats sb = b.stats();
        NodeStats sc = c.stats();
        // 历史用量 a == b > c == 0，因此 a、b 额度相等且远大于 c 的保底额度
        assertThat(sa.currentWindowQuota()).isEqualTo(sb.currentWindowQuota());
        assertThat(sa.currentWindowQuota()).isGreaterThan(400);
        assertThat(sc.currentWindowQuota()).isGreaterThanOrEqualTo(10);
        assertThat(sc.currentWindowQuota()).isLessThan(100);

        long total = sa.currentWindowQuota() + sb.currentWindowQuota() + sc.currentWindowQuota();
        assertThat(total).isLessThanOrEqualTo(coordinator.distributableQuota());
        assertThat(total).isEqualTo(coordinator.distributableQuota()); // 最大余数法分满
    }

    @Test
    void nodeRejectsBeyondLocalQuota() {
        RateLimiterNode a = join("a");
        coordinator.reallocateNow();
        long quota = a.stats().currentWindowQuota();
        assertThat(quota).isEqualTo(900);

        long admitted = 0;
        for (int i = 0; i < quota + 500; i++) {
            if (a.tryAcquire()) {
                admitted++;
            }
        }
        assertThat(admitted).isEqualTo(quota);
        assertThat(a.stats().totalRejected()).isEqualTo(500);
    }

    @Test
    void sumOfQuotasNeverExceedsGlobalQuotaAcrossReallocations() {
        join("a");
        join("b");
        for (int round = 0; round < 5; round++) {
            coordinator.reallocateNow();
            GlobalStats stats = coordinator.stats();
            assertThat(stats.allocatedQuota()).isLessThanOrEqualTo(stats.distributableQuota());
            assertThat(stats.allocatedQuota() + stats.fallbackReserve())
                    .isLessThanOrEqualTo(stats.windowQuota());
            clock.advanceMillis(grid.windowMillis());
            coordinator.tick();
        }
    }
}
