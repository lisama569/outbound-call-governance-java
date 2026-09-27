package com.github.lisama569.outboundcallgovernancejava.governance;

/**
 * 单个调用目标的降级配置。
 *
 * <p>三种形态：
 * <ul>
 *   <li>{@link #none()}：不配置降级，主调用失败即返回失败；</li>
 *   <li>{@link #staticValue(String)}：静态兜底值，主调用失败时直接返回该值；</li>
 *   <li>{@link #target(String)}：降级目标，主调用失败时改调另一个目标
 *       （该目标自身仍受配额/并发/熔断/超时预算约束，且可继续链式降级）。</li>
 * </ul>
 *
 * @param kind           降级形态
 * @param staticValue    静态兜底值（kind 为 STATIC_VALUE 时有效）
 * @param fallbackTarget 降级目标名（kind 为 FALLBACK_TARGET 时有效）
 */
public record FallbackConfig(Kind kind, String staticValue, String fallbackTarget) {

    public enum Kind {
        NONE,
        STATIC_VALUE,
        FALLBACK_TARGET
    }

    public FallbackConfig {
        if (kind == null) {
            kind = Kind.NONE;
        }
    }

    public static FallbackConfig none() {
        return new FallbackConfig(Kind.NONE, null, null);
    }

    public static FallbackConfig staticValue(String value) {
        return new FallbackConfig(Kind.STATIC_VALUE, value, null);
    }

    public static FallbackConfig target(String target) {
        return new FallbackConfig(Kind.FALLBACK_TARGET, null, target);
    }
}
