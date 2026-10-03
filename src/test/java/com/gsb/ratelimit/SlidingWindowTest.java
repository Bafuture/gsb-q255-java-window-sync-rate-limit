package com.gsb.ratelimit;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** 要求 1：时间切格 + 真正的滑动窗口语义（窗口随小格滑动，旧格过期后额度恢复）。 */
class SlidingWindowTest {

    @Test
    void windowSlidesByCellAndOldCellsExpire() {
        ManualClock clock = new ManualClock(0);
        TimeGrid grid = new TimeGrid(100, 4);
        InMemoryTransport transport = new InMemoryTransport(clock);
        // 不接入中心，直接使用兜底额度 10，验证纯本地滑动窗口语义
        RateLimiterNode node = new RateLimiterNode("n", transport, clock, grid, 10);

        for (int i = 0; i < 10; i++) {
            assertThat(node.tryAcquire()).isTrue();
        }
        assertThat(node.tryAcquire()).isFalse(); // 窗口内已用满

        clock.advanceMillis(100); // 滑动 1 格，最旧的一格仍在窗口内
        assertThat(node.tryAcquire()).isFalse();

        clock.advanceMillis(300); // 滑动到第 4 格，最初一格（cell 0）滑出窗口
        for (int i = 0; i < 10; i++) {
            assertThat(node.tryAcquire()).isTrue();
        }
        assertThat(node.tryAcquire()).isFalse();
    }

    @Test
    void usageIsReportedPerCell() {
        ManualClock clock = new ManualClock(0);
        TimeGrid grid = new TimeGrid(100, 4);
        InMemoryTransport transport = new InMemoryTransport(clock);
        CentralCoordinator coordinator =
                new CentralCoordinator(transport, clock, grid, 1000, 10, 10);
        RateLimiterNode node = new RateLimiterNode("n", transport, clock, grid, 10);
        node.join();
        coordinator.reallocateNow();

        for (int i = 0; i < 7; i++) {
            node.tryAcquire();
        }
        clock.advanceMillis(100); // 触发上一格上报
        node.tryAcquire();
        node.flush();

        GlobalStats stats = coordinator.stats();
        assertThat(stats.reportedUsageByNode().get("n")).isEqualTo(8);
    }
}
