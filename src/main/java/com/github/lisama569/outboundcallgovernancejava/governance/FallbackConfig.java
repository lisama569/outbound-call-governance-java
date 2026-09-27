package com.github.lisama569.outboundcallgovernancejava.governance;

/**
 * 单个调用目标的降级配置：静态兜底值 或 另一个目标作为降级目标，二者至多取其一。
 *
 * @param staticValue    静态兜底值（非空时主调用失败直接返回该值）
 * @param fallbackTarget 降级目标（非空时主调用失败转入该目标的受治理调用）
 */
public record FallbackConfig(String staticValue, String fallbackTarget) {

    /** 无降级。 */
    public static FallbackConfig none() {
        return new FallbackConfig(null, null);
    }

    /** 静态兜底值降级。 */
    public static FallbackConfig ofStaticValue(String staticValue) {
        return new FallbackConfig(staticValue, null);
    }

    /** 降级到另一个目标。 */
    public static FallbackConfig ofTarget(String fallbackTarget) {
        return new FallbackConfig(null, fallbackTarget);
    }

    /** 是否未配置降级。 */
    public boolean isNone() {
        return staticValue == null && fallbackTarget == null;
    }
}
