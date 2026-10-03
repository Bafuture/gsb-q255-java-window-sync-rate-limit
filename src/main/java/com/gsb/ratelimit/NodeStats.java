package com.gsb.ratelimit;

/** 节点本地统计快照。 */
public record NodeStats(
        String nodeId,
        boolean accepted,
        long currentWindowQuota,
        long fallbackQuota,
        long windowUsage,
        long totalAdmitted,
        long totalRejected,
        long clockOffsetMillis,
        long lastSyncRttMillis,
        long lastAllocationLatencyMillis) {
}
