package com.gsb.ratelimit;

import java.util.function.Consumer;

/** 节点与中心之间的消息通道抽象。 */
public interface Transport {

    void send(String to, Message message);

    void register(String id, Consumer<Message> handler);

    void unregister(String id);
}
