package com.github.lisama569.outboundcallgovernancejava.governance;

/**
 * 单个调用目标的治理配置。
 *
 * @param quotaPerWindow          每个窗口允许的调用次数
 * @param windowMillis            配额窗口长度（毫秒）
 * @param maxConcurrency          并发上限
 * @param callTimeoutMillis       单次调用超时（毫秒）
 * @param maxAttempts             最大尝试次数（含首次）
 * @param backoffMillis           重试退避基数（毫秒）
 * @param retryBudgetMillis       整次请求（含全部重试）的耗时预算（毫秒）
 * @param circuitFailureThreshold 连续失败达到该次数后熔断
 * @param circuitResetMillis      熔断打开后进入半开探测的等待时间（毫秒）
 * @param fallback                降级配置（静态兜底值或降级目标；null 视为无降级）
 */
public record TargetConfig(
        int quotaPerWindow,
        long windowMillis,
        int maxConcurrency,
        long callTimeoutMillis,
        int maxAttempts,
        long backoffMillis,
        long retryBudgetMillis,
        int circuitFailureThreshold,
        long circuitResetMillis,
        FallbackConfig fallback) {

    /** 兼容旧的无降级配置构造：等价于 fallback 为 none。 */
    public TargetConfig(int quotaPerWindow, long windowMillis, int maxConcurrency,
                        long callTimeoutMillis, int maxAttempts, long backoffMillis,
                        long retryBudgetMillis, int circuitFailureThreshold,
                        long circuitResetMillis) {
        this(quotaPerWindow, windowMillis, maxConcurrency, callTimeoutMillis, maxAttempts,
                backoffMillis, retryBudgetMillis, circuitFailureThreshold, circuitResetMillis,
                FallbackConfig.none());
    }

    /** 归一化：null 降级配置视为无降级，保证 fallback() 永不返回 null。 */
    public TargetConfig {
        if (fallback == null) {
            fallback = FallbackConfig.none();
        }
    }

    public static TargetConfig defaultConfig() {
        return new TargetConfig(100, 60_000, 10, 2_000, 3, 100, 10_000, 3, 5_000);
    }
}
