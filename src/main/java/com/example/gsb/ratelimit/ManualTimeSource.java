package com.example.gsb.ratelimit;

/** 可手动推进的时钟，测试专用，保证窗口边界行为完全确定。 */
public final class ManualTimeSource implements TimeSource {

    private long now;

    public ManualTimeSource(long startMillis) {
        this.now = startMillis;
    }

    @Override
    public synchronized long nowMillis() {
        return now;
    }

    public synchronized void advance(long deltaMillis) {
        if (deltaMillis < 0) {
            throw new IllegalArgumentException("time cannot go backwards");
        }
        now += deltaMillis;
    }
}
