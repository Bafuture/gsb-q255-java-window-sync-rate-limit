package com.gsb.ratelimit;

/** 包装另一个时钟并叠加固定偏差，用于模拟节点时钟与中心时钟不一致。 */
public final class OffsetClock implements Clock {

    private final Clock delegate;
    private final long offsetMillis;

    public OffsetClock(Clock delegate, long offsetMillis) {
        this.delegate = delegate;
        this.offsetMillis = offsetMillis;
    }

    @Override
    public long nowMillis() {
        return delegate.nowMillis() + offsetMillis;
    }
}
