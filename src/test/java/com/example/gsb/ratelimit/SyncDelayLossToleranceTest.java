package com.example.gsb.ratelimit;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/** 同步延迟与丢失容忍：fail-closed + 预留余量保证任何情况下不超发。 */
class SyncDelayLossToleranceTest {

    @Test
    void delayedAndDroppedMessagesNeverCauseOvershoot() {
        // 窗口 1s = 10 格，配额 1000，中心余量 100，节点各预留 10
        WindowConfig config = new WindowConfig(100L, 10, 1000L, 100L, 10L);
        Cluster cluster = Cluster.create(config, 150L); // 所有消息延迟 150ms
        List<Node> nodes = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            nodes.add(cluster.addNode());
        }
        cluster.tick(); // 先完成加入（此时还没开始丢消息）
        cluster.bus().droppingEveryNth(3); // 之后每 3 条丢 1 条

        List<Long> perWindowAllowed = new ArrayList<>();
        long previousTotalAllowed = nodes.stream()
                .mapToLong(n -> n.stats().allowedTotal()).sum();

        for (int w = 0; w < 5; w++) {
            for (int slot = 0; slot < config.slotsPerWindow(); slot++) {
                cluster.advance(config.slotMillis());
                for (Node node : nodes) {
                    for (int attempt = 0; attempt < 50; attempt++) {
                        node.tryAcquire();
                    }
                }
            }
            long totalAllowed = nodes.stream()
                    .mapToLong(n -> n.stats().allowedTotal()).sum();
            perWindowAllowed.add(totalAllowed - previousTotalAllowed);
            previousTotalAllowed = totalAllowed;
        }

        Stats.BusStats busStats = cluster.bus().stats();
        System.out.println("==== 延迟/丢失容忍 ====");
        System.out.println("逐窗口通过量 = " + perWindowAllowed);
        System.out.println("投递数       = " + busStats.delivered());
        System.out.println("丢失数       = " + busStats.dropped());
        System.out.printf("平均同步延迟 = %.1fms, 最大 = %dms%n",
                busStats.avgLatencyMillis(), busStats.maxLatencyMillis());
        System.out.println("总拒绝数     = " + cluster.stats().totalRejected());
        System.out.println("=====================");

        // 逐窗口均不超发
        for (long allowed : perWindowAllowed) {
            assertThat(allowed).isLessThanOrEqualTo(config.windowQuota());
            assertThat(allowed).isLessThanOrEqualTo(config.allocatableQuota());
        }
        long totalAllowed = perWindowAllowed.stream().mapToLong(Long::longValue).sum();
        assertThat(totalAllowed).isPositive();
        assertThat(totalAllowed).isLessThanOrEqualTo(5L * config.windowQuota());

        // 丢失确实发生；延迟被记录
        assertThat(busStats.dropped()).isPositive();
        assertThat(busStats.maxLatencyMillis()).isBetween(150L, 250L);

        // 消息丢失使部分窗口的 grant 没到达，节点 fail-closed，可见吞吐受损
        assertThat(perWindowAllowed.stream().anyMatch(
                a -> a < config.allocatableQuota())).isTrue();
        assertThat(cluster.stats().totalRejected()).isPositive();
    }

    @Test
    void nodeWithNoGrantFailsClosedBeforeFirstSync() {
        WindowConfig config = new WindowConfig(100L, 10, 1000L, 100L, 10L);
        Cluster cluster = Cluster.create(config, 500L); // 延迟超过窗口起始阶段
        Node node = cluster.addNode();
        cluster.tick(); // grant 已发送但尚未到期
        assertThat(node.tryAcquire()).isFalse();
        assertThat(node.stats().rejectedTotal()).isPositive();
    }
}
