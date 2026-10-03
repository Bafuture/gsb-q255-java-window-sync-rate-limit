package com.gsb.ratelimit;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 要求 4：并发压测下全局通过量不得超过窗口配额。
 * 时间冻结在同一窗口内，多线程同时冲击各节点，证明无超发。
 */
class ConcurrencyNoOvershootTest {

    @Test
    void globalAdmissionsNeverExceedWindowQuota() throws InterruptedException {
        ManualClock clock = new ManualClock(0);
        TimeGrid grid = new TimeGrid(100, 4);
        InMemoryTransport transport = new InMemoryTransport(clock);
        CentralCoordinator coordinator =
                new CentralCoordinator(transport, clock, grid, 1000, 10, 10);

        int nodeCount = 4;
        List<RateLimiterNode> nodes = new java.util.ArrayList<>();
        for (int i = 0; i < nodeCount; i++) {
            RateLimiterNode node = new RateLimiterNode("n" + i, transport, clock, grid, 10);
            node.join();
            nodes.add(node);
        }
        coordinator.reallocateNow(); // 等历史用量 -> 每个节点均分 distributable

        long allocated = coordinator.stats().allocatedQuota();
        assertThat(allocated).isEqualTo(900);

        int threadsPerNode = 8;
        int attemptsPerThread = 20_000;
        ExecutorService pool = Executors.newFixedThreadPool(nodeCount * threadsPerNode);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(nodeCount * threadsPerNode);
        ConcurrentLinkedQueue<Throwable> errors = new ConcurrentLinkedQueue<>();
        AtomicLong globalAdmitted = new AtomicLong();

        for (RateLimiterNode node : nodes) {
            for (int t = 0; t < threadsPerNode; t++) {
                pool.submit(() -> {
                    try {
                        start.await();
                        long local = 0;
                        for (int i = 0; i < attemptsPerThread; i++) {
                            if (node.tryAcquire()) {
                                local++;
                            }
                        }
                        globalAdmitted.addAndGet(local);
                    } catch (Throwable e) {
                        errors.add(e);
                    } finally {
                        done.countDown();
                    }
                });
            }
        }

        start.countDown();
        assertThat(done.await(60, TimeUnit.SECONDS)).isTrue();
        pool.shutdownNow();
        assertThat(errors).isEmpty();

        long totalAttempts = (long) nodeCount * threadsPerNode * attemptsPerThread;
        long totalRejected = nodes.stream().mapToLong(n -> n.stats().totalRejected()).sum();
        long totalAdmittedFromNodes = nodes.stream().mapToLong(n -> n.stats().totalAdmitted()).sum();

        assertThat(totalAdmittedFromNodes).isEqualTo(globalAdmitted.get());
        assertThat(totalAdmittedFromNodes + totalRejected).isEqualTo(totalAttempts);
        assertThat(totalAdmittedFromNodes).isEqualTo(allocated);
        assertThat(totalAdmittedFromNodes).isLessThanOrEqualTo(coordinator.globalQuota());

        // 误差（超发率）：超发量 / 窗口配额，必须为 0
        long overshoot = Math.max(0, totalAdmittedFromNodes - coordinator.globalQuota());
        double overshootErrorPct = overshoot * 100.0 / coordinator.globalQuota();
        System.out.printf(
                "[Concurrency] attempts=%d admitted=%d rejected=%d quota=%d overshoot=%d error=%.4f%%%n",
                totalAttempts, totalAdmittedFromNodes, totalRejected,
                coordinator.globalQuota(), overshoot, overshootErrorPct);
        assertThat(overshoot).isZero();
    }

}
