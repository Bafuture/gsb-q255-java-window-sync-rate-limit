package com.example.gsb.ratelimit;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * 测试/仿真用集群装配：把 {@link Coordinator}、若干 {@link Node}、{@link MessageBus}
 * 和一个可手动推进的时钟连在一起。
 *
 * <p>{@link #tick()} 一次驱动一轮：投递到期消息 -> 中心检测窗口滚动 -> 各节点上报
 * -> 再投递一轮（零延迟消息当场到达，带延迟的消息等到期后到达）。</p>
 */
public final class Cluster {

    private final WindowConfig config;
    private final ManualTimeSource clock;
    private final MessageBus bus;
    private final Coordinator coordinator;
    private final Map<String, Node> nodes = new TreeMap<>();
    private int sequence;

    private Cluster(WindowConfig config, long busDelayMillis) {
        this.config = config;
        this.clock = new ManualTimeSource(0L);
        this.bus = new MessageBus(this.clock, busDelayMillis);
        this.coordinator = new Coordinator(config, this.clock, this.bus);
    }

    public static Cluster create(WindowConfig config) {
        return new Cluster(config, 0L);
    }

    public static Cluster create(WindowConfig config, long busDelayMillis) {
        return new Cluster(config, busDelayMillis);
    }

    public ManualTimeSource clock() {
        return clock;
    }

    public MessageBus bus() {
        return bus;
    }

    public Coordinator coordinator() {
        return coordinator;
    }

    /** 使用与中心一致的时钟加入节点。 */
    public Node addNode() {
        return addNodeWithSkew(0L);
    }

    /** 带本地时钟偏差（毫秒）加入节点，偏差由 grant 携带的中心时间校正。 */
    public Node addNodeWithSkew(long skewMillis) {
        String nodeId = "node-" + (++sequence);
        Node node = new Node(nodeId, config, TimeSource.offset(clock, skewMillis), bus);
        nodes.put(nodeId, node);
        node.join();
        return node;
    }

    public void removeNode(String nodeId) {
        Node node = nodes.remove(nodeId);
        if (node != null) {
            node.leave();
        }
    }

    public List<Node> nodes() {
        return new ArrayList<>(nodes.values());
    }

    public long nowMillis() {
        return clock.nowMillis();
    }

    public int tick() {
        int delivered = bus.pump();
        coordinator.tick();
        for (Node node : nodes.values()) {
            node.flushReport();
        }
        delivered += bus.pump();
        return delivered;
    }

    /** 推进时间并驱动一轮同步。 */
    public int advance(long deltaMillis) {
        clock.advance(deltaMillis);
        return tick();
    }

    /** 跑完一个窗口（按小格步进），保证窗口内多次同步。 */
    public void runWindow() {
        for (int i = 0; i < config.slotsPerWindow(); i++) {
            advance(config.slotMillis());
        }
    }

    public Stats.ClusterStats stats() {
        long window = coordinator.currentWindow();
        List<Stats.NodeStats> nodeStats = nodes.values().stream()
                .map(Node::stats)
                .toList();
        long totalAllowed = nodeStats.stream()
                .mapToLong(Stats.NodeStats::usedInWindow)
                .sum();
        long totalRejected = nodeStats.stream()
                .mapToLong(Stats.NodeStats::rejectedTotal)
                .sum();
        long totalGranted = nodeStats.stream()
                .mapToLong(Stats.NodeStats::grantedQuota)
                .sum();
        Stats.CoordinatorStats coordinatorStats = coordinator.stats();
        return new Stats.ClusterStats(window, config.windowQuota(),
                config.allocatableQuota(), totalGranted, totalAllowed, totalRejected,
                (double) totalAllowed - config.windowQuota(),
                (double) totalAllowed / config.windowQuota(),
                nodeStats, coordinatorStats, bus.stats());
    }
}
