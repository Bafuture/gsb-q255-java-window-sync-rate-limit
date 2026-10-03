package com.example.gsb.ratelimit;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 配额中心。
 *
 * <ol>
 *   <li>接收各节点按格汇总的用量上报；</li>
 *   <li>窗口滚动或成员变化时，按各节点历史用量比例分配当前窗口额度并下发
 *       {@link Message.QuotaGrant}；</li>
 *   <li>中心始终只分配 windowQuota - safetyHeadroom（安全余量不分配）；</li>
 *   <li>节点退出时其未用额度立即回收，并在存活节点间重新分配。</li>
 * </ol>
 * 所有公开方法线程安全。
 */
public final class Coordinator {

    public static final String ID = "coordinator";

    private final WindowConfig config;
    private final TimeSource clock;
    private final MessageBus bus;

    private final Set<String> nodes = new LinkedHashSet<>();
    private final Map<String, Long> grants = new LinkedHashMap<>();
    private final Map<String, Long> reportedUsage = new LinkedHashMap<>();
    private final Map<String, Long> historyUsage = new LinkedHashMap<>();

    private long currentWindow = Long.MIN_VALUE;
    private int rebalanceCount;

    public Coordinator(WindowConfig config, TimeSource clock, MessageBus bus) {
        this.config = config;
        this.clock = clock;
        this.bus = bus;
        bus.register(ID, this::onMessage);
    }

    /** 驱动一次：检测窗口滚动，必要时重新分配。 */
    public synchronized void tick() {
        long window = windowIndex(clock.nowMillis());
        if (currentWindow == Long.MIN_VALUE) {
            currentWindow = window;
            rebalance();
            return;
        }
        if (window > currentWindow) {
            historyUsage.clear();
            historyUsage.putAll(reportedUsage);
            reportedUsage.clear();
            currentWindow = window;
            rebalance();
        }
    }

    private synchronized void onMessage(String from, Message message) {
        if (message instanceof Message.JoinRequest join) {
            if (nodes.add(join.nodeId()) && currentWindow != Long.MIN_VALUE) {
                rebalance();
            }
        } else if (message instanceof Message.LeaveNotice leave) {
            if (nodes.remove(leave.nodeId())) {
                grants.remove(leave.nodeId());
                reportedUsage.remove(leave.nodeId());
                historyUsage.remove(leave.nodeId());
                rebalance();
            }
        } else if (message instanceof Message.UsageReport report) {
            if (!nodes.contains(report.nodeId())) {
                return;
            }
            if (report.windowIndex() == currentWindow) {
                reportedUsage.put(report.nodeId(), report.usedInWindow());
            } else if (report.windowIndex() < currentWindow) {
                historyUsage.merge(report.nodeId(), report.usedInWindow(), Math::max);
            }
        }
    }

    /** 按历史用量加权的比例分配；总授予额度恒等于 allocatableQuota。 */
    private void rebalance() {
        grants.clear();
        if (nodes.isEmpty()) {
            rebalanceCount++;
            return;
        }
        List<String> members = new ArrayList<>(nodes);
        long allocatable = config.allocatableQuota();
        long totalWeight = members.stream().mapToLong(this::weightOf).sum();
        long centerSlot = Math.floorDiv(clock.nowMillis(), config.slotMillis());

        long assigned = 0;
        for (int i = 0; i < members.size(); i++) {
            String nodeId = members.get(i);
            long share;
            if (i == members.size() - 1) {
                share = allocatable - assigned;
            } else {
                share = allocatable * weightOf(nodeId) / totalWeight;
                if (share < 1) {
                    share = 1;
                }
            }
            grants.put(nodeId, share);
            assigned += share;
            bus.send(ID, nodeId, new Message.QuotaGrant(
                    nodeId, currentWindow, share, centerSlot));
        }
        rebalanceCount++;
    }

    private long weightOf(String nodeId) {
        long known = Math.max(
                historyUsage.getOrDefault(nodeId, 0L),
                reportedUsage.getOrDefault(nodeId, 0L));
        return Math.max(1L, known);
    }

    public synchronized long currentWindow() {
        return currentWindow;
    }

    public synchronized long windowIndex(long timeMillis) {
        return Math.floorDiv(timeMillis, config.windowMillis());
    }

    public synchronized Set<String> members() {
        return new LinkedHashSet<>(nodes);
    }

    public synchronized long grantOf(String nodeId) {
        return grants.getOrDefault(nodeId, 0L);
    }

    public synchronized Stats.CoordinatorStats stats() {
        long totalGranted = grants.values().stream().mapToLong(Long::longValue).sum();
        long totalReported = reportedUsage.values().stream().mapToLong(Long::longValue).sum();
        return new Stats.CoordinatorStats(currentWindow, nodes.size(),
                config.allocatableQuota(), totalGranted, totalReported,
                config.safetyHeadroom(), rebalanceCount);
    }
}
