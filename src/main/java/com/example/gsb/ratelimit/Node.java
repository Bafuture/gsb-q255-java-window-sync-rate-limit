package com.example.gsb.ratelimit;

import java.util.Collections;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

/**
 * 限流节点。
 *
 * <ul>
 *   <li>本地按小格维护滑动窗口计数（最近 slotsPerWindow 格）；</li>
 *   <li>以中心下发的 {@link Message.QuotaGrant} 为权威，超过本地有效额度即拒绝；</li>
 *   <li>时钟偏差：用 grant 携带的中心小格序号计算偏移 slotOffset，本地小格 =
 *       原始小格 + slotOffset，从而对齐到中心时间基准；</li>
 *   <li>保守策略：有效额度 = 授予额度 - nodeReserve；当前窗口没有有效 grant
 *       （首次启动 / grant 延迟 / grant 丢失）时 fail-closed，一律拒绝。</li>
 * </ul>
 * 所有公开方法线程安全，可直接用于并发压测。
 */
public final class Node {

    private final String id;
    private final WindowConfig config;
    private final TimeSource clock;
    private final MessageBus bus;

    private long slotOffset;
    private long grantedWindow = Long.MIN_VALUE;
    private long grantedQuota;

    private long usedInWindow;
    private long allowedTotal;
    private long rejectedTotal;

    private final NavigableMap<Long, Long> slotCounts = new TreeMap<>();

    private long lastReportSentAt = Long.MIN_VALUE;
    private long lastRtt = Long.MIN_VALUE;
    private long rttSum;
    private long rttCount;

    public Node(String id, WindowConfig config, TimeSource localClock, MessageBus bus) {
        this.id = id;
        this.config = config;
        this.clock = localClock;
        this.bus = bus;
        bus.register(id, this::onMessage);
    }

    public String id() {
        return id;
    }

    /**
     * 尝试放行一个请求。
     *
     * @return true 表示放行并计入本窗口用量；false 表示拒绝（超本地额度或当前窗口无授权）
     */
    public synchronized boolean tryAcquire() {
        long slot = alignedSlot(clock.nowMillis());
        long window = Math.floorDiv(slot, config.slotsPerWindow());
        if (window != grantedWindow) {
            rejectedTotal++;
            return false;
        }
        if (usedInWindow >= effectiveQuota()) {
            rejectedTotal++;
            return false;
        }
        usedInWindow++;
        allowedTotal++;
        slotCounts.merge(slot, 1L, Long::sum);
        evictOldSlots(slot);
        return true;
    }

    private void evictOldSlots(long currentSlot) {
        long oldestKept = currentSlot - config.slotsPerWindow() + 1;
        slotCounts.headMap(oldestKept, false).clear();
    }

    private long effectiveQuota() {
        return Math.max(0L, grantedQuota - config.nodeReserve());
    }

    private long rawSlot(long timeMillis) {
        return Math.floorDiv(timeMillis, config.slotMillis());
    }

    /** 加上时钟偏差校正后的小格序号。 */
    public synchronized long alignedSlot(long timeMillis) {
        return rawSlot(timeMillis) + slotOffset;
    }

    /** 按节点本地时钟取当前时刻，再校正到中心小格序号。 */
    public synchronized long currentAlignedSlot() {
        return alignedSlot(clock.nowMillis());
    }

    /** 按节点本地时钟取当前时刻，再校正到中心窗口序号。 */
    public synchronized long currentAlignedWindow() {
        return Math.floorDiv(currentAlignedSlot(), config.slotsPerWindow());
    }

    /** 加上时钟偏差校正后的窗口序号。 */
    public synchronized long alignedWindow(long timeMillis) {
        return Math.floorDiv(alignedSlot(timeMillis), config.slotsPerWindow());
    }

    public synchronized long slotOffset() {
        return slotOffset;
    }

    /** 向中心发送加入请求。 */
    public void join() {
        bus.send(id, Coordinator.ID, new Message.JoinRequest(id));
    }

    /** 向中心发送退出通知。 */
    public void leave() {
        bus.send(id, Coordinator.ID, new Message.LeaveNotice(id));
    }

    /** 上报当前窗口用量。 */
    public synchronized void flushReport() {
        if (grantedWindow == Long.MIN_VALUE) {
            return;
        }
        bus.send(id, Coordinator.ID,
                new Message.UsageReport(id, grantedWindow, usedInWindow));
        lastReportSentAt = clock.nowMillis();
    }

    private synchronized void onMessage(String from, Message message) {
        if (message instanceof Message.QuotaGrant grant
                && grant.nodeId().equals(id)) {
            long now = clock.nowMillis();
            slotOffset = grant.centerSlotIndex() - rawSlot(now);
            if (grant.windowIndex() != grantedWindow) {
                grantedWindow = grant.windowIndex();
                usedInWindow = 0;
            }
            grantedQuota = grant.quota();
            if (lastReportSentAt != Long.MIN_VALUE) {
                lastRtt = now - lastReportSentAt;
                rttSum += Math.max(0, lastRtt);
                rttCount++;
            }
        }
    }

    public synchronized Stats.NodeStats stats() {
        Map<Long, Long> snapshot =
                Collections.unmodifiableMap(new TreeMap<>(slotCounts));
        return new Stats.NodeStats(id, grantedWindow, grantedQuota, effectiveQuota(),
                usedInWindow, allowedTotal, rejectedTotal, slotOffset,
                rttCount == 0 ? 0.0 : (double) rttSum / rttCount,
                lastRtt == Long.MIN_VALUE ? -1 : lastRtt,
                snapshot);
    }
}
