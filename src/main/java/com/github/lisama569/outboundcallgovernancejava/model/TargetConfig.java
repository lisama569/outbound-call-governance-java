package com.github.lisama569.outboundcallgovernancejava.model;

/**
 * 单个调用目标的治理配置。
 *
 * @param quotaPerWindow        每个固定时间窗允许的真实调用数（&gt;0）
 * @param windowMillis          配额时间窗长度（毫秒，&gt;0）
 * @param maxConcurrency        同一目标的最大并发真实调用数（&gt;0）
 * @param timeoutMillis         单次下游调用超时（毫秒，&gt;0）
 * @param maxAttempts           最大尝试次数（含首次，&gt;0）
 * @param budgetMillis          重试总耗时预算（毫秒，&gt;0）
 * @param backoffMillis         首次重试退避基数（毫秒，&gt;=0）
 * @param failureThreshold      连续失败多少次后打开熔断（&gt;0）
 * @param openWaitMillis        熔断打开后等待多久允许一次探测（毫秒，&gt;0）
 * @param fallbackTarget        回退目标名，null 表示无回退目标
 * @param fallbackValue         静态回退值，非 null 时优先于回退目标
 */
public record TargetConfig(
        int quotaPerWindow,
        long windowMillis,
        int maxConcurrency,
        long timeoutMillis,
        int maxAttempts,
        long budgetMillis,
        long backoffMillis,
        int failureThreshold,
        long openWaitMillis,
        String fallbackTarget,
        String fallbackValue
) {
}
