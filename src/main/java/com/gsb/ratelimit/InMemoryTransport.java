package com.gsb.ratelimit;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * 内存消息通道：latencyMillis == 0 时同步投递；
 * 否则按 transport 时钟延迟投递，需要调用 {@link #pump()} 驱动物理投递。
 * 支持确定性丢包（每 N 条丢 1 条）与全部丢弃，用于模拟同步延迟与消息丢失。
 */
public final class InMemoryTransport implements Transport {

    private record Pending(long deliverAt, long seq, String to, Message message)
            implements Comparable<Pending> {
        @Override
        public int compareTo(Pending other) {
            int cmp = Long.compare(deliverAt, other.deliverAt);
            return cmp != 0 ? cmp : Long.compare(seq, other.seq);
        }
    }

    private final Clock clock;
    private final Map<String, Consumer<Message>> handlers = new ConcurrentHashMap<>();
    private final PriorityBlockingQueue<Pending> queue = new PriorityBlockingQueue<>();
    private final AtomicLong sequence = new AtomicLong();
    private final AtomicLong sent = new AtomicLong();
    private final AtomicLong dropped = new AtomicLong();
    private final AtomicLong delivered = new AtomicLong();

    private volatile long latencyMillis;
    private volatile int dropEvery; // >0 时每 N 条丢 1 条
    private volatile boolean dropAll;

    public InMemoryTransport(Clock clock) {
        this.clock = clock;
    }

    public void setLatencyMillis(long latencyMillis) {
        this.latencyMillis = latencyMillis;
    }

    public void setDropEvery(int dropEvery) {
        this.dropEvery = dropEvery;
    }

    public void setDropAll(boolean dropAll) {
        this.dropAll = dropAll;
    }

    @Override
    public void send(String to, Message message) {
        long n = sent.incrementAndGet();
        if (dropAll || (dropEvery > 0 && n % dropEvery == 0)) {
            dropped.incrementAndGet();
            return;
        }
        if (latencyMillis <= 0) {
            deliver(to, message);
            return;
        }
        queue.add(new Pending(clock.nowMillis() + latencyMillis, sequence.incrementAndGet(), to, message));
    }

    /** 投递所有已到期的消息，返回投递条数。 */
    public int pump() {
        int count = 0;
        while (true) {
            Pending head = queue.peek();
            if (head == null || head.deliverAt() > clock.nowMillis()) {
                break;
            }
            queue.poll();
            deliver(head.to(), head.message());
            count++;
        }
        return count;
    }

    public int pendingCount() {
        return queue.size();
    }

    public long droppedCount() {
        return dropped.get();
    }

    public long deliveredCount() {
        return delivered.get();
    }

    @Override
    public void register(String id, Consumer<Message> handler) {
        handlers.put(id, handler);
    }

    @Override
    public void unregister(String id) {
        handlers.remove(id);
    }

    private void deliver(String to, Message message) {
        Consumer<Message> handler = handlers.get(to);
        if (handler == null) {
            dropped.incrementAndGet();
            return;
        }
        delivered.incrementAndGet();
        handler.accept(message);
    }
}
