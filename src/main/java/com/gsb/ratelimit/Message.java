package com.gsb.ratelimit;

/** 节点与中心之间的同步消息。 */
public sealed interface Message {

    /** 节点 -> 中心：申请加入。 */
    record JoinRequest(String nodeId) implements Message {
    }

    /** 中心 -> 节点：加入结果。quota 为当前窗口的初始额度；accepted=false 表示集群已满。 */
    record JoinResponse(String nodeId, boolean accepted, long windowIndex, long quota, long centerTimeMillis)
            implements Message {
    }

    /** 节点 -> 中心：时间同步请求，t0 为节点本地发送时刻。 */
    record TimeSyncRequest(String nodeId, long t0LocalMillis) implements Message {
    }

    /** 中心 -> 节点：时间同步响应，回显 t0 并附上中心当前时间。 */
    record TimeSyncResponse(String nodeId, long t0LocalMillis, long centerTimeMillis) implements Message {
    }

    /** 节点 -> 中心：某个小格的用量上报。count 为该格的累计值（单调不减）。 */
    record UsageReport(String nodeId, long cellIndex, long windowIndex, long count, long sentAtCenterMillis)
            implements Message {
    }

    /** 中心 -> 节点：额度下发，仅对 windowIndex 指定的窗口有效。 */
    record AllocationUpdate(String nodeId, long windowIndex, long quota, long issuedAtCenterMillis)
            implements Message {
    }

    /** 节点 -> 中心：主动退出，中心回收其未用额度。 */
    record LeaveNotice(String nodeId) implements Message {
    }
}
