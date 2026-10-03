package com.gsb.ratelimit;

import java.util.Map;

/** 中心视角的全局统计快照。 */
public record GlobalStats(
        long windowQuota,
        long distributableQuota,
        long allocatedQuota,
        long fallbackReserve,
        int activeNodes,
        long currentWindow,
        long reportedUsageCurrentWindow,
        long overshoot,
        double overshootErrorPct,
        double avgReportLatencyMillis,
        long maxReportLatencyMillis,
        long reallocations,
        long reclaimedQuotaEvents,
        Map<String, Long> quotaByNode,
        Map<String, Long> reportedUsageByNode) {
}
