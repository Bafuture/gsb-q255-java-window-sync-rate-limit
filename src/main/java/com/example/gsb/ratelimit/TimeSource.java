package com.example.gsb.ratelimit;

/** 时间源抽象：生产环境用系统时钟，测试用可手动推进的时钟。 */
@FunctionalInterface
public interface TimeSource {

    long nowMillis();

    /** 包装出一个带固定偏移（时钟偏差）的时间源，用于模拟节点时钟倾斜。 */
    static TimeSource offset(TimeSource delegate, long offsetMillis) {
        return () -> delegate.nowMillis() + offsetMillis;
    }
}
