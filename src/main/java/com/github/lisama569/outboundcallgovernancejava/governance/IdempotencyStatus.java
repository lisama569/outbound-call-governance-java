package com.github.lisama569.outboundcallgovernancejava.governance;

/**
 * 幂等判定结果：本次请求与实质执行的关系。
 */
public enum IdempotencyStatus {
    /** 本次请求触发了实质执行 */
    FRESH_EXECUTION,
    /** 相同幂等键正在执行中，本次请求挂接等待其结果 */
    JOINED_IN_PROGRESS,
    /** 相同幂等键已完成，本次请求直接回放已完成结果 */
    REPLAYED_AFTER_COMPLETION
}
