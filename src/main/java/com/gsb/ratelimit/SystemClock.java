package com.gsb.ratelimit;

/** 真实系统时钟。 */
public final class SystemClock implements Clock {

    @Override
    public long nowMillis() {
        return System.currentTimeMillis();
    }
}
