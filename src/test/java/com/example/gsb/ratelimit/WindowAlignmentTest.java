package com.example.gsb.ratelimit;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/**
 * 窗口对齐：所有节点以中心时间基准对齐小格与窗口。
 *
 * <p>时钟偏差处理：grant 携带中心小格序号，节点据此计算整数格偏移 slotOffset，
 * 可完全校正“整数格”级别的偏差；不足一格的残余偏差（本地格边界与中心格边界
 * 错开）在构造上无法靠整数偏移消除，上界为 1 格，由中心安全余量与节点预留吸收。</p>
 */
class WindowAlignmentTest {

    private static WindowConfig config() {
        return new WindowConfig(100L, 10, 1000L, 100L, 10L);
    }

    @Test
    void nodeWithoutSkewMatchesCenterExactly() {
        Cluster cluster = Cluster.create(config());
        Node node = cluster.addNode();
        cluster.tick();

        for (int i = 0; i < 300; i++) {
            cluster.advance(7L); // 非对齐步长，覆盖各种边界位置
            long centerSlot = cluster.nowMillis() / 100L;
            assertThat(node.currentAlignedSlot()).isEqualTo(centerSlot);
        }
    }

    @Test
    void skewedNodesAlignWithinOneSlot() {
        Cluster cluster = Cluster.create(config());
        Node ahead = cluster.addNodeWithSkew(237L);   // 快 237ms（跨 2 格多）
        Node behind = cluster.addNodeWithSkew(-88L);  // 慢 88ms
        cluster.tick();

        for (int i = 0; i < 300; i++) {
            cluster.advance(7L);
            long centerSlot = cluster.nowMillis() / 100L;
            assertThat(Math.abs(ahead.currentAlignedSlot() - centerSlot))
                    .isLessThanOrEqualTo(1);
            assertThat(Math.abs(behind.currentAlignedSlot() - centerSlot))
                    .isLessThanOrEqualTo(1);
        }
    }

    @Test
    void multiSlotSkewIsFullyCorrectedAwayFromBoundaries() {
        Cluster cluster = Cluster.create(config());
        // 偏差为整数格（300ms = 3 格）：应被 slotOffset 完全校正
        Node ahead = cluster.addNodeWithSkew(300L);
        cluster.tick();

        for (int i = 0; i < 100; i++) {
            cluster.advance(7L);
            long centerSlot = cluster.nowMillis() / 100L;
            assertThat(ahead.currentAlignedSlot()).isEqualTo(centerSlot);
        }
    }

    @Test
    void allNodesShareTheSameWindowIndexAtWindowMidpoint() {
        Cluster cluster = Cluster.create(config());
        Node a = cluster.addNodeWithSkew(130L);
        Node b = cluster.addNodeWithSkew(-260L);
        cluster.tick();

        for (int w = 0; w < 3; w++) {
            cluster.advance(500L); // 窗口中点：距窗口边界 5 格，±1 格残余偏差不越界
            long centerWindow = cluster.coordinator().windowIndex(cluster.nowMillis());
            assertThat(a.currentAlignedWindow()).isEqualTo(centerWindow);
            assertThat(b.currentAlignedWindow()).isEqualTo(centerWindow);
            cluster.advance(500L);
        }
    }

    @Test
    void skewCorrectionOffsetIsRecorded() {
        Cluster cluster = Cluster.create(config());
        Node ahead = cluster.addNodeWithSkew(250L);
        cluster.tick();
        // 250ms 偏差 -> 本地小格比中心快 2~3 格，校正量应为 -2 或 -3
        assertThat(ahead.slotOffset()).isBetween(-3L, -2L);
    }

    @Test
    void perSlotCountsTrackUsageAndEvictOldSlots() {
        Cluster cluster = Cluster.create(config());
        Node node = cluster.addNode();
        cluster.tick();

        // 前 5 格每格放行 3 个：滑动窗口未溢出，格计数与窗口用量一致
        for (int i = 0; i < 5; i++) {
            cluster.advance(100L);
            for (int j = 0; j < 3; j++) {
                assertThat(node.tryAcquire()).isTrue();
            }
        }
        Stats.NodeStats early = node.stats();
        long earlyCounted = early.slotCounts().values().stream()
                .mapToLong(Long::longValue).sum();
        assertThat(earlyCounted).isEqualTo(15L);
        assertThat(earlyCounted).isEqualTo(early.usedInWindow());

        // 继续跑满两个窗口：滑动窗口只保留最近 10 格
        for (int i = 0; i < 15; i++) {
            cluster.advance(100L);
            for (int j = 0; j < 3; j++) {
                node.tryAcquire();
            }
        }
        Stats.NodeStats later = node.stats();
        long counted = later.slotCounts().values().stream()
                .mapToLong(Long::longValue).sum();
        assertThat(later.slotCounts().size()).isLessThanOrEqualTo(10);
        assertThat(counted).isEqualTo(30L); // 最近 10 格 x 每格 3 个
        assertThat(counted).isLessThanOrEqualTo(later.allowedTotal());
    }
}
