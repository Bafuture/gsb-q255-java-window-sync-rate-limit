package com.example.gsb.ratelimit;

/** 节点与中心之间交换的消息。 */
public sealed interface Message {

    /** 节点 -> 中心：注册加入。 */
    record JoinRequest(String nodeId) implements Message {
    }

    /** 节点 -> 中心：主动退出，中心回收其未用额度。 */
    record LeaveNotice(String nodeId) implements Message {
    }

    /** 节点 -> 中心：上报当前窗口已用量（按格统计的汇总）。 */
    record UsageReport(String nodeId, long windowIndex, long usedInWindow) implements Message {
    }

    /**
     * 中心 -> 节点：下发窗口额度。
     *
     * @param centerSlotIndex 中心发出消息时所处的小格序号，节点用它校正本地时钟偏差
     */
    record QuotaGrant(String nodeId, long windowIndex, long quota, long centerSlotIndex)
            implements Message {
    }
}
