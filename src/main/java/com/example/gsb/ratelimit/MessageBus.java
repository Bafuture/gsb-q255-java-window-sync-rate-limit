package com.example.gsb.ratelimit;

import java.util.Comparator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * 内存消息总线，模拟节点与中心之间的通信。
 *
 * <ul>
 *   <li>支持固定传输延迟：消息在 delayMillis 之后才会被投递；</li>
 *   <li>支持丢失：通过 DropPredicate 判定是否丢弃；</li>
 *   <li>统计投递延迟、投递数与丢失数。</li>
 * </ul>
 */
public final class MessageBus {

    @FunctionalInterface
    public interface Handler {
        void onMessage(String from, Message message);
    }

    @FunctionalInterface
    public interface DropPredicate {
        boolean shouldDrop(String from, String to, Message message);
    }

    private record Envelope(String from, String to, Message message,
                            long sentAt, long deliverAt) {
    }

    private final TimeSource clock;
    private volatile long delayMillis;
    private volatile DropPredicate dropPredicate = (from, to, message) -> false;

    private final Map<String, Handler> handlers = new ConcurrentHashMap<>();
    private final PriorityBlockingQueue<Envelope> queue =
            new PriorityBlockingQueue<>(64, Comparator.comparingLong(Envelope::deliverAt));

    private final LongAdder delivered = new LongAdder();
    private final LongAdder dropped = new LongAdder();
    private final LongAdder totalLatency = new LongAdder();
    private final AtomicLong maxLatency = new AtomicLong();

    public MessageBus(TimeSource clock) {
        this(clock, 0L);
    }

    public MessageBus(TimeSource clock, long delayMillis) {
        this.clock = clock;
        this.delayMillis = delayMillis;
    }

    public MessageBus withDelay(long delayMillis) {
        this.delayMillis = delayMillis;
        return this;
    }

    public MessageBus dropping(DropPredicate predicate) {
        this.dropPredicate = predicate;
        return this;
    }

    /** 每 n 条消息丢弃 1 条（n>1 时生效），用于确定性地模拟丢消息。 */
    public MessageBus droppingEveryNth(int n) {
        if (n <= 1) {
            throw new IllegalArgumentException("n must be > 1");
        }
        DropPredicate[] holder = new DropPredicate[1];
        LongAdder counter = new LongAdder();
        holder[0] = (from, to, message) -> {
            counter.increment();
            return counter.sum() % n == 0;
        };
        return dropping(holder[0]);
    }

    public void register(String endpointId, Handler handler) {
        handlers.put(endpointId, handler);
    }

    public void unregister(String endpointId) {
        handlers.remove(endpointId);
    }

    /** 发送消息；被丢弃或按延迟排队。线程安全。 */
    public void send(String from, String to, Message message) {
        long now = clock.nowMillis();
        if (dropPredicate.shouldDrop(from, to, message)) {
            dropped.increment();
            return;
        }
        queue.offer(new Envelope(from, to, message, now, now + delayMillis));
    }

    /** 投递所有已到期的消息。由驱动循环（测试或节点调度器）周期性调用。 */
    public int pump() {
        int count = 0;
        while (true) {
            Envelope head = queue.peek();
            if (head == null || head.deliverAt() > clock.nowMillis()) {
                return count;
            }
            Envelope env = queue.poll();
            Handler handler = handlers.get(env.to());
            if (handler != null) {
                long latency = clock.nowMillis() - env.sentAt();
                delivered.increment();
                totalLatency.add(latency);
                maxLatency.accumulateAndGet(latency, Math::max);
                handler.onMessage(env.from(), env.message());
                count++;
            }
        }
    }

    public long queueSize() {
        return queue.size();
    }

    public Stats.BusStats stats() {
        long d = delivered.sum();
        return new Stats.BusStats(d, dropped.sum(),
                d == 0 ? 0.0 : (double) totalLatency.sum() / d,
                maxLatency.get());
    }
}
