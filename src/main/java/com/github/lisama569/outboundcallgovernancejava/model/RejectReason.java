package com.github.lisama569.outboundcallgovernancejava.model;

/**
 * 出站调用被拒绝或失败的可区分原因。
 */
public enum RejectReason {
    /** 目标没有配置治理规则 */
    UNKNOWN_TARGET,
    /** 非法配置（字段缺失或取值非法） */
    INVALID_CONFIG,
    /** 回退配置被拒绝（目标不存在、回退环或回退目标非法） */
    FALLBACK_CONFIG_REJECTED,
    /** 时间窗配额已用尽 */
    QUOTA_EXCEEDED,
    /** 并发上限已用尽 */
    CONCURRENCY_LIMIT,
    /** 熔断打开，快速失败 */
    CIRCUIT_OPEN,
    /** 下游调用抛出异常（不可重试或重试后仍失败） */
    DOWNSTREAM_FAILURE,
    /** 耗时预算耗尽，不能再发起尝试 */
    RETRY_BUDGET_EXHAUSTED,
    /** 回退执行时没有可用的回退目标/值，或回退链失败 */
    FALLBACK_UNAVAILABLE
}
