package com.example.gsb.ratelimit;

/**
 * 滑动窗口配置。
 *
 * @param slotMillis     小格（slot）长度，全局时间的最小计数粒度
 * @param slotsPerWindow 每个配额窗口包含的小格数，窗口按格对齐滚动
 * @param windowQuota    每个窗口的全局总额度（所有节点共享）
 * @param safetyHeadroom 中心侧安全余量：中心只分配 windowQuota - safetyHeadroom，
 *                       余量用于吸收同步延迟、时钟偏差等边界效应
 * @param nodeReserve    节点侧预留余量：节点实际可用额度 = 授予额度 - nodeReserve，
 *                       即使同步消息延迟/丢失，节点也不会把预留部分发出去
 */
public record WindowConfig(long slotMillis, int slotsPerWindow, long windowQuota,
                           long safetyHeadroom, long nodeReserve) {

    public WindowConfig {
        if (slotMillis <= 0 || slotsPerWindow <= 0) {
            throw new IllegalArgumentException("slotMillis/slotsPerWindow must be positive");
        }
        if (windowQuota <= 0 || safetyHeadroom < 0 || nodeReserve < 0) {
            throw new IllegalArgumentException("invalid quota parameters");
        }
        if (safetyHeadroom >= windowQuota) {
            throw new IllegalArgumentException("safetyHeadroom must be < windowQuota");
        }
    }

    public long windowMillis() {
        return slotMillis * slotsPerWindow;
    }

    /** 中心在一个窗口内实际会分配出去的总额度。 */
    public long allocatableQuota() {
        return windowQuota - safetyHeadroom;
    }
}
