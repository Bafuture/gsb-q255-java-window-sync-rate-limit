package com.gsb.ratelimit;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 要求 2：窗口对齐。所有节点以中心时钟为基准；本地时钟存在偏差时，
 * 通过加入响应粗对齐 + NTP 风格双向校时精对齐。
 */
class WindowAlignmentTest {

    private final ManualClock centerClock = new ManualClock(10_000);
    private final TimeGrid grid = new TimeGrid(100, 4);

    @Test
    void skewedClocksAlignToCenterWithZeroLatency() {
        InMemoryTransport transport = new InMemoryTransport(centerClock);
        CentralCoordinator coordinator = new CentralCoordinator(
                transport, centerClock, grid, 1000, 10, 10);
        RateLimiterNode fast = new RateLimiterNode("fast", transport,
                new OffsetClock(centerClock, 250), grid, 10);
        RateLimiterNode slow = new RateLimiterNode("slow", transport,
                new OffsetClock(centerClock, -180), grid, 10);

        fast.join();
        slow.join();
        fast.timeSync();
        slow.timeSync();

        for (RateLimiterNode node : java.util.List.of(fast, slow)) {
            assertThat(node.centerNowMillis()).isEqualTo(centerClock.nowMillis());
            assertThat(node.currentCell()).isEqualTo(grid.cellIndex(centerClock.nowMillis()));
            assertThat(node.currentWindow()).isEqualTo(grid.windowIndex(centerClock.nowMillis()));
        }
    }

    @Test
    void twoWayTimeSyncRemovesSymmetricNetworkLatency() {
        InMemoryTransport transport = new InMemoryTransport(centerClock);
        transport.setLatencyMillis(40);
        new CentralCoordinator(transport, centerClock, grid, 1000, 10, 10);
        RateLimiterNode skewed = new RateLimiterNode("skewed", transport,
                new OffsetClock(centerClock, 300), grid, 10);

        skewed.join();
        TestSupport.flushAll(transport, centerClock, 40);
        skewed.timeSync();
        TestSupport.flushAll(transport, centerClock, 40);

        assertThat(skewed.centerNowMillis()).isEqualTo(centerClock.nowMillis());
        assertThat(skewed.stats().lastSyncRttMillis()).isEqualTo(80);
        assertThat(skewed.stats().clockOffsetMillis()).isEqualTo(-300);
    }

    @Test
    void nodesMapSameInstantToSameCellAfterSync() {
        InMemoryTransport transport = new InMemoryTransport(centerClock);
        new CentralCoordinator(transport, centerClock, grid, 1000, 10, 10);
        RateLimiterNode nodeA = new RateLimiterNode("a", transport,
                new OffsetClock(centerClock, 250), grid, 10);
        RateLimiterNode nodeB = new RateLimiterNode("b", transport,
                new OffsetClock(centerClock, -250), grid, 10);

        // 未校时时两个节点对同一中心时刻的小格归属不一致
        long unsyncedCellA = grid.cellIndex(centerClock.nowMillis() + 250);
        long unsyncedCellB = grid.cellIndex(centerClock.nowMillis() - 250);

        nodeA.join();
        nodeB.join();

        long centerCell = grid.cellIndex(centerClock.nowMillis());
        assertThat(unsyncedCellA).isNotEqualTo(unsyncedCellB);
        assertThat(nodeA.currentCell()).isEqualTo(centerCell);
        assertThat(nodeB.currentCell()).isEqualTo(centerCell);
        assertThat(nodeA.currentCell()).isEqualTo(nodeB.currentCell());
    }
}
