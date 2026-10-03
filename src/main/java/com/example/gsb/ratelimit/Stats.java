package com.example.gsb.ratelimit;

import java.util.List;
import java.util.Map;

/** 统计快照。 */
public final class Stats {

    private Stats() {
    }

    /** 单个节点的统计。 */
    public record NodeStats(String nodeId,
                            long windowIndex,
                            long grantedQuota,
                            long effectiveQuota,
                            long usedInWindow,
                            long allowedTotal,
                            long rejectedTotal,
                            long slotOffset,
                            double avgSyncRttMillis,
                            long lastSyncRttMillis,
                            Map<Long, Long> slotCounts) {
    }

    /** 消息总线统计。 */
    public record BusStats(long delivered, long dropped,
                           double avgLatencyMillis, long maxLatencyMillis) {
    }

    /** 中心统计。 */
    public record CoordinatorStats(long windowIndex,
                                   int nodeCount,
                                   long allocatableQuota,
                                   long totalGranted,
                                   long totalReportedUsage,
                                   long safetyHeadroom,
                                   int rebalanceCount) {
    }

    /** 集群整体统计，超发误差 = 实际通过量 - 窗口配额（不大于 0 即未超发）。 */
    public record ClusterStats(long windowIndex,
                               long windowQuota,
                               long allocatableQuota,
                               long totalGranted,
                               long totalAllowed,
                               long totalRejected,
                               double overshoot,
                               double utilization,
                               List<NodeStats> nodes,
                               CoordinatorStats coordinator,
                               BusStats bus) {
    }
}
