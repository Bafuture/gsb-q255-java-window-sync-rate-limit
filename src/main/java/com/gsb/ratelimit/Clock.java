package com.gsb.ratelimit;

/** 时间源抽象：节点与中心可以各自持有不同（甚至有偏差）的时钟。 */
public interface Clock {

    long nowMillis();
}
