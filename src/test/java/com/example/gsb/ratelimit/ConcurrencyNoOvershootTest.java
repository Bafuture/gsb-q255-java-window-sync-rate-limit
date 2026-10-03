package com.example.gsb.ratelimit;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

/** 并发压测：全局通过量严格不超过窗口配额，并给出超发误差。 */
class ConcurrencyNoOvershootTest {

    @Test
    void concurrentTrafficNeverExceedsWindowQuota() throws InterruptedException {
        // 窗口配额 10000，中心安全余量 500（只分配 9500），节点无额外预留
        WindowConfig config = new WindowConfig(100L, 10, 10_000L, 500L, 0L);
        Cluster cluster = Cluster.create(config);
        List<Node> nodes = List.of(cluster.addNode(), cluster.addNode(),
                cluster.addNode(), cluster.addNode());
        cluster.tick();

        int threads = 16;
        int perThreadAttempts = 10_000;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threads);
        AtomicLong returnValueAllowed = new AtomicLong();

        for (int t = 0; t < threads; t++) {
            Node target = nodes.get(t % nodes.size());
            pool.submit(() -> {
                try {
                    start.await();
                    long localAllowed = 0;
                    for (int i = 0; i < perThreadAttempts; i++) {
                        if (target.tryAcquire()) {
                            localAllowed++;
                        }
                    }
                    returnValueAllowed.addAndGet(localAllowed);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    done.countDown();
                }
            });
        }

        start.countDown();
        assertThat(done.await(30, TimeUnit.SECONDS)).isTrue();
        pool.shutdown();

        long allowedFromReturnValues = returnValueAllowed.get();
        long allowedFromNodeStats = nodes.stream()
                .mapToLong(n -> n.stats().usedInWindow())
                .sum();

        System.out.println("==== 并发不超发压测 ====");
        System.out.println("窗口配额       = " + config.windowQuota());
        System.out.println("可分配额度     = " + config.allocatableQuota());
        System.out.println("总尝试数       = " + (long) threads * perThreadAttempts);
        System.out.println("全局通过量     = " + allowedFromReturnValues);
        System.out.println("拒绝数         = " + cluster.stats().totalRejected());
        System.out.printf("超发误差(绝对) = %d%n",
                allowedFromReturnValues - config.windowQuota());
        System.out.printf("超发误差(相对) = %.4f%%%n",
                100.0 * (allowedFromReturnValues - config.windowQuota())
                        / config.windowQuota());
        System.out.printf("配额利用率     = %.2f%%%n",
                100.0 * allowedFromReturnValues / config.windowQuota());
        System.out.println("=======================");

        assertThat(allowedFromNodeStats).isEqualTo(allowedFromReturnValues);
        // 关键正确性断言：绝不超发
        assertThat(allowedFromReturnValues).isLessThanOrEqualTo(config.windowQuota());
        // 构造上每个节点严格按本地额度放行，总和恰好等于已分配额度
        assertThat(allowedFromReturnValues).isEqualTo(config.allocatableQuota());
        assertThat(cluster.stats().totalRejected()).isPositive();
        // 误差口径（相对，负数表示未用满）
        double relativeError =
                (double) (allowedFromReturnValues - config.windowQuota())
                        / config.windowQuota();
        assertThat(relativeError).isLessThanOrEqualTo(0.0);
    }
}
