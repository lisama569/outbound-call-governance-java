package com.github.lisama569.outboundcallgovernancejava.governance;

/**
 * 调用被拒绝时的可区分原因。
 */
public enum RejectionReason {
    /** 目标配额窗口内额度耗尽 */
    QUOTA_EXHAUSTED,
    /** 目标并发上限已满 */
    CONCURRENCY_SATURATED,
    /** 熔断器打开（含半开时非探测请求） */
    CIRCUIT_OPEN,
    /** 重试耗时预算耗尽，不再发起新调用 */
    BUDGET_EXHAUSTED,
    /** 下游持续失败且重试耗尽 */
    DOWNSTREAM_FAILURE
}
