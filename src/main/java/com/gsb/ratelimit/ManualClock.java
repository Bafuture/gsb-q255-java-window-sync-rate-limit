package com.gsb.ratelimit;

import java.util.concurrent.atomic.AtomicLong;

/** 可手动推进的时钟，用于确定性测试与仿真。线程安全。 */
public final class ManualClock implements Clock {

    private final AtomicLong now;

    public ManualClock(long startMillis) {
        this.now = new AtomicLong(startMillis);
    }

    @Override
    public long nowMillis() {
        return now.get();
    }

    public void advanceMillis(long delta) {
        now.addAndGet(delta);
    }

    public void setMillis(long value) {
        now.set(value);
    }
}
