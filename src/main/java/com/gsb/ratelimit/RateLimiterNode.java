package com.gsb.ratelimit;

import java.util.HashMap;
import java.util.Map;

/**
 * 限流节点：
 * <ul>
 *   <li>以中心时钟为基准（NTP 风格的双向校时）按小格统计请求数并逐格上报；</li>
 *   <li>真正的滑动窗口判定：当前小格向前数 cellsPerWindow 格的用量之和不得超过当前窗口额度；</li>
 *   <li>当当前窗口的额度下发缺失（延迟/丢失）时，使用保守兜底额度 fallbackQuota；</li>
 *   <li>额度是硬上限：即使同步完全中断也不会超发，只会降低利用率。</li>
 * </ul>
 */
public final class RateLimiterNode {

    private final String id;
    private final Transport transport;
    private final Clock localClock;
    private final TimeGrid grid;
    private final long fallbackQuota;

    private final Object lock = new Object();
    private final Map<Long, Long> cellCounts = new HashMap<>();

    private volatile long clockOffsetMillis; // 中心时间 - 本地时间
    private long quotaWindow = -1;
    private long windowQuota;
    private long lastReportedCell = Long.MIN_VALUE;
    private long totalAdmitted;
    private long totalRejected;
    private boolean registered;
    private volatile boolean accepted = true;
    private volatile long lastSyncRttMillis = -1;
    private volatile long lastAllocationLatencyMillis = -1;

    public RateLimiterNode(String id, Transport transport, Clock localClock,
                           TimeGrid grid, long fallbackQuota) {
        if (fallbackQuota < 0) {
            throw new IllegalArgumentException("fallbackQuota must be >= 0");
        }
        this.id = id;
        this.transport = transport;
        this.localClock = localClock;
        this.grid = grid;
        this.fallbackQuota = fallbackQuota;
    }

    public String id() {
        return id;
    }

    /** 注册并向中心申请加入。 */
    public void join() {
        transport.register(id, this::handleMessage);
        registered = true;
        transport.send(CentralCoordinator.ID, new Message.JoinRequest(id));
    }

    /** 主动退出，中心会回收未用额度。 */
    public void leave() {
        transport.send(CentralCoordinator.ID, new Message.LeaveNotice(id));
        if (registered) {
            transport.unregister(id);
            registered = false;
        }
    }

    /**
     * 双向校时（NTP 简化版）：
     * offset = centerTime - (t0 + t1)/2，剩余误差不超过往返时延的一半。
     * 周期性调用以抵消节点时钟漂移。
     */
    public void timeSync() {
        transport.send(CentralCoordinator.ID,
                new Message.TimeSyncRequest(id, localClock.nowMillis()));
    }

    public long centerNowMillis() {
        return localClock.nowMillis() + clockOffsetMillis;
    }

    public long currentCell() {
        return grid.cellIndex(centerNowMillis());
    }

    public long currentWindow() {
        return grid.windowIndex(centerNowMillis());
    }

    public boolean currentWindowQuotaValid() {
        return quotaWindow == currentWindow();
    }

    /**
     * 尝试通过一个请求。成功返回 true；超出本地窗口额度返回 false。
     * 整个判定在锁内完成，保证高并发下本地额度（进而全局额度）绝不超发。
     */
    public boolean tryAcquire() {
        synchronized (lock) {
            long now = centerNowMillis();
            long cell = grid.cellIndex(now);
            long window = grid.windowIndexOfCell(cell);
            reportCompletedCellsLocked(cell);

            long quota;
            if (!accepted) {
                quota = 0;
            } else if (window == quotaWindow) {
                quota = windowQuota;
            } else {
                // 当前窗口的额度还没下发（延迟/丢失）：走保守兜底
                quota = fallbackQuota;
            }

            long oldest = cell - grid.cellsPerWindow() + 1;
            long used = 0;
            for (long c = oldest; c <= cell; c++) {
                used += cellCounts.getOrDefault(c, 0L);
            }
            if (used >= quota) {
                totalRejected++;
                return false;
            }
            cellCounts.merge(cell, 1L, Long::sum);
            totalAdmitted++;
            return true;
        }
    }

    /** 立即上报所有已结束小格以及当前小格的快照（测试与优雅停机时使用）。 */
    public void flush() {
        synchronized (lock) {
            long cell = currentCell();
            reportCompletedCellsLocked(cell);
            long count = cellCounts.getOrDefault(cell, 0L);
            transport.send(CentralCoordinator.ID,
                    new Message.UsageReport(id, cell, grid.windowIndexOfCell(cell), count, centerNowMillis()));
            lastReportedCell = cell;
        }
    }

    public NodeStats stats() {
        synchronized (lock) {
            long window = currentWindow();
            long first = grid.firstCellOfWindow(window);
            long windowUsage = 0;
            for (long c = first; c < first + grid.cellsPerWindow(); c++) {
                windowUsage += cellCounts.getOrDefault(c, 0L);
            }
            long effectiveQuota = (window == quotaWindow) ? windowQuota : 0;
            return new NodeStats(id, accepted, effectiveQuota, fallbackQuota, windowUsage,
                    totalAdmitted, totalRejected, clockOffsetMillis,
                    lastSyncRttMillis, lastAllocationLatencyMillis);
        }
    }

    private void handleMessage(Message message) {
        if (message instanceof Message.JoinResponse response) {
            accepted = response.accepted();
            clockOffsetMillis = response.centerTimeMillis() - localClock.nowMillis();
            applyAllocation(response.windowIndex(), response.quota(), response.centerTimeMillis());
        } else if (message instanceof Message.TimeSyncResponse response) {
            long t1 = localClock.nowMillis();
            long rtt = t1 - response.t0LocalMillis();
            lastSyncRttMillis = Math.max(0, rtt);
            clockOffsetMillis = response.centerTimeMillis()
                    - (response.t0LocalMillis() + t1) / 2;
        } else if (message instanceof Message.AllocationUpdate update) {
            applyAllocation(update.windowIndex(), update.quota(), update.issuedAtCenterMillis());
        }
    }

    private void applyAllocation(long window, long quota, long issuedAtCenterMillis) {
        synchronized (lock) {
            if (window >= quotaWindow) {
                quotaWindow = window;
                windowQuota = quota;
            }
            lastAllocationLatencyMillis = Math.max(0, centerNowMillis() - issuedAtCenterMillis);
        }
    }

    private void reportCompletedCellsLocked(long currentCell) {
        if (lastReportedCell == Long.MIN_VALUE) {
            lastReportedCell = currentCell - 1;
        }
        for (long cell = lastReportedCell + 1; cell < currentCell; cell++) {
            long count = cellCounts.getOrDefault(cell, 0L);
            transport.send(CentralCoordinator.ID, new Message.UsageReport(
                    id, cell, grid.windowIndexOfCell(cell), count, centerNowMillis()));
            lastReportedCell = cell;
        }
        if (currentCell - 1 > lastReportedCell) {
            lastReportedCell = currentCell - 1;
        }
        long oldest = currentCell - grid.cellsPerWindow() + 1;
        cellCounts.keySet().removeIf(cell -> cell < oldest);
    }
}
