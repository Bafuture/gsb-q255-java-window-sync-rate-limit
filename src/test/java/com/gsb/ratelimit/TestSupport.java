package com.gsb.ratelimit;

/** 测试辅助：驱动延迟消息通道直到队列为空。 */
final class TestSupport {

    private TestSupport() {
    }

    static void flushAll(InMemoryTransport transport, ManualClock clock, long stepMillis) {
        int guard = 0;
        while (transport.pendingCount() > 0) {
            clock.advanceMillis(stepMillis);
            transport.pump();
            if (++guard > 10_000) {
                throw new IllegalStateException("flushAll did not converge");
            }
        }
    }
}
