package com.gsb.ratelimit;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 中心协调器：
 * <ul>
 *   <li>维护全局窗口配额，并按各节点上一窗口的历史用量比例分配当前窗口额度；</li>
 *   <li>不变式：任意窗口下，所有节点有效额度之和 &lt;= globalQuota - fallbackReserve，
 *       其中 fallbackReserve = maxNodes * minQuota 预留给同步丢失时的保守兜底与新节点加入；</li>
 *   <li>节点退出时回收其额度并立即重新分配。</li>
 * </ul>
 */
public final class CentralCoordinator {

    public static final String ID = "coordinator";

    private final Transport transport;
    private final Clock clock;
    private final TimeGrid grid;
    private final long globalQuota;
    private final long minQuota;
    private final int maxNodes;

    private final Object lock = new Object();
    private final Map<String, NodeState> nodes = new LinkedHashMap<>();

    private long currentWindow = -1;
    private long reallocations;
    private long reclaimedQuotaEvents;
    private long reportLatencyTotal;
    private long reportLatencyMax;
    private long reportLatencyCount;

    private static final class NodeState {
        final String id;
        long quota;
        final Map<Long, Long> usageByCell = new HashMap<>();

        NodeState(String id) {
            this.id = id;
        }
    }

    public CentralCoordinator(Transport transport, Clock clock, TimeGrid grid,
                              long globalQuota, long minQuota, int maxNodes) {
        if (maxNodes <= 0) {
            throw new IllegalArgumentException("maxNodes must be > 0");
        }
        if (minQuota <= 0) {
            throw new IllegalArgumentException("minQuota must be > 0");
        }
        // 需要同时容纳：全部节点的兜底预留 + 至少一份可分配额度
        if (globalQuota < 2L * maxNodes * minQuota) {
            throw new IllegalArgumentException(
                    "globalQuota must be >= 2 * maxNodes * minQuota to keep the reserve invariant");
        }
        this.transport = transport;
        this.clock = clock;
        this.grid = grid;
        this.globalQuota = globalQuota;
        this.minQuota = minQuota;
        this.maxNodes = maxNodes;
        transport.register(ID, this::handle);
    }

    public long globalQuota() {
        return globalQuota;
    }

    public long fallbackReserve() {
        return (long) maxNodes * minQuota;
    }

    public long distributableQuota() {
        return globalQuota - fallbackReserve();
    }

    /** 推进中心状态：检测窗口翻转并在必要时重新分配。 */
    public void tick() {
        synchronized (lock) {
            tickLocked();
        }
    }

    /** 强制对当前窗口重新分配（例如新节点加入后希望立即均衡）。 */
    public void reallocateNow() {
        synchronized (lock) {
            tickLocked();
            reallocateLocked(currentWindow);
        }
    }

    public int activeNodeCount() {
        synchronized (lock) {
            return nodes.size();
        }
    }

    public long quotaOf(String nodeId) {
        synchronized (lock) {
            NodeState state = nodes.get(nodeId);
            return state == null ? 0 : state.quota;
        }
    }

    public GlobalStats stats() {
        synchronized (lock) {
            tickLocked();
            Map<String, Long> quotas = new LinkedHashMap<>();
            Map<String, Long> usage = new LinkedHashMap<>();
            long windowUsageTotal = 0;
            for (NodeState state : nodes.values()) {
                quotas.put(state.id, state.quota);
                long nodeWindowUsage = usageOfWindowLocked(state, currentWindow);
                usage.put(state.id, nodeWindowUsage);
                windowUsageTotal += nodeWindowUsage;
            }
            long overshoot = Math.max(0, windowUsageTotal - globalQuota);
            double errorPct = globalQuota == 0 ? 0.0 : overshoot * 100.0 / globalQuota;
            double avgLatency = reportLatencyCount == 0
                    ? 0.0 : (double) reportLatencyTotal / reportLatencyCount;
            return new GlobalStats(
                    globalQuota, distributableQuota(), allocatedLocked(), fallbackReserve(),
                    nodes.size(), currentWindow, windowUsageTotal, overshoot, errorPct,
                    avgLatency, reportLatencyMax, reallocations, reclaimedQuotaEvents,
                    quotas, usage);
        }
    }

    private void handle(Message message) {
        if (message instanceof Message.JoinRequest request) {
            onJoin(request.nodeId());
        } else if (message instanceof Message.LeaveNotice notice) {
            onLeave(notice.nodeId());
        } else if (message instanceof Message.TimeSyncRequest request) {
            transport.send(request.nodeId(),
                    new Message.TimeSyncResponse(request.nodeId(), request.t0LocalMillis(), clock.nowMillis()));
        } else if (message instanceof Message.UsageReport report) {
            onReport(report);
        }
    }

    private void onJoin(String nodeId) {
        synchronized (lock) {
            tickLocked();
            if (nodes.containsKey(nodeId)) {
                return;
            }
            if (nodes.size() >= maxNodes) {
                transport.send(nodeId, new Message.JoinResponse(
                        nodeId, false, currentWindow, 0, clock.nowMillis()));
                return;
            }
            NodeState state = new NodeState(nodeId);
            // 新节点先从未分配余量中拿一份保守额度，下一窗口再按历史用量重新分配
            long headroom = distributableQuota() - allocatedLocked();
            state.quota = Math.max(0, Math.min(minQuota, headroom));
            nodes.put(nodeId, state);
            long now = clock.nowMillis();
            transport.send(nodeId, new Message.JoinResponse(nodeId, true, currentWindow, state.quota, now));
            transport.send(nodeId, new Message.AllocationUpdate(nodeId, currentWindow, state.quota, now));
        }
    }

    private void onLeave(String nodeId) {
        synchronized (lock) {
            tickLocked();
            if (nodes.remove(nodeId) != null) {
                reclaimedQuotaEvents++;
                reallocateLocked(currentWindow);
            }
        }
    }

    private void onReport(Message.UsageReport report) {
        synchronized (lock) {
            tickLocked();
            NodeState state = nodes.get(report.nodeId());
            if (state == null) {
                return;
            }
            long latency = Math.max(0, clock.nowMillis() - report.sentAtCenterMillis());
            reportLatencyTotal += latency;
            reportLatencyCount++;
            reportLatencyMax = Math.max(reportLatencyMax, latency);
            // count 为该格累计值，取最大保证幂等
            state.usageByCell.merge(report.cellIndex(), report.count(), Math::max);
        }
    }

    private void tickLocked() {
        long window = grid.windowIndex(clock.nowMillis());
        if (window != currentWindow) {
            currentWindow = window;
            pruneLocked();
            reallocateLocked(window);
        }
    }

    private void pruneLocked() {
        long oldestCell = grid.firstCellOfWindow(currentWindow - 1);
        for (NodeState state : nodes.values()) {
            state.usageByCell.keySet().removeIf(cell -> cell < oldestCell);
        }
    }

    /**
     * 按上一窗口历史用量比例分配：先给每个节点 minQuota 保底，
     * 剩余部分按权重以最大余数法分配，保证总和恰好 &lt;= distributableQuota。
     */
    private void reallocateLocked(long window) {
        if (nodes.isEmpty()) {
            return;
        }
        long distributable = distributableQuota();
        int n = nodes.size();
        long remaining = distributable - minQuota * n; // 构造不变式保证 >= 0
        long previousWindow = window - 1;

        List<NodeState> ordered = new ArrayList<>(nodes.values());
        long totalWeight = 0;
        Map<String, Long> weights = new HashMap<>();
        for (NodeState state : ordered) {
            long weight = Math.max(1, usageOfWindowLocked(state, previousWindow));
            weights.put(state.id, weight);
            totalWeight += weight;
        }

        Map<String, Long> shares = new HashMap<>();
        long assigned = 0;
        for (NodeState state : ordered) {
            long share = remaining * weights.get(state.id) / totalWeight;
            shares.put(state.id, share);
            assigned += share;
        }
        // 最大余数法分发取整剩余
        long leftover = remaining - assigned;
        final long totalWeightFinal = totalWeight;
        ordered.sort(Comparator.comparingLong(
                (NodeState s) -> remaining * weights.get(s.id) % totalWeightFinal).reversed());
        for (int i = 0; i < leftover; i++) {
            NodeState state = ordered.get(i % ordered.size());
            shares.merge(state.id, 1L, Long::sum);
        }

        long now = clock.nowMillis();
        for (NodeState state : nodes.values()) {
            state.quota = minQuota + shares.get(state.id);
            transport.send(state.id, new Message.AllocationUpdate(state.id, window, state.quota, now));
        }
        reallocations++;
    }

    private long usageOfWindowLocked(NodeState state, long window) {
        long first = grid.firstCellOfWindow(window);
        long total = 0;
        for (long cell = first; cell < first + grid.cellsPerWindow(); cell++) {
            total += state.usageByCell.getOrDefault(cell, 0L);
        }
        return total;
    }

    private long allocatedLocked() {
        long total = 0;
        for (NodeState state : nodes.values()) {
            total += state.quota;
        }
        return total;
    }
}
