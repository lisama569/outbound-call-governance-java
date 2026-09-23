package com.github.lisama569.outboundcallgovernancejava.model;

/**
 * 幂等键下一次出站调用的最终状态。
 */
public enum CallStatus {
    /** 成功，返回值为下游真实结果或回退结果 */
    SUCCESS,
    /** 下游真实调用失败且没有可用回退 */
    FAILED,
    /** 被治理层拒绝（配额/并发/熔断/未知目标），reason 给出具体原因 */
    REJECTED,
    /** 成功，但结果来自回退 */
    FALLBACK
}
