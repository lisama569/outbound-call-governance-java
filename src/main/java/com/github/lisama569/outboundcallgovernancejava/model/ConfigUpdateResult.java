package com.github.lisama569.outboundcallgovernancejava.model;

/**
 * 运行中配置变更的结果。
 *
 * @param target    目标名
 * @param accepted  是否生效
 * @param reason    被拒绝时的可区分原因（INVALID_CONFIG / FALLBACK_CONFIG_REJECTED），接受时为 null
 * @param detail    人类可读的细节
 */
public record ConfigUpdateResult(String target, boolean accepted, RejectReason reason, String detail) {

    public static ConfigUpdateResult ok(String target) {
        return new ConfigUpdateResult(target, true, null, "accepted");
    }

    public static ConfigUpdateResult reject(String target, RejectReason reason, String detail) {
        return new ConfigUpdateResult(target, false, reason, detail);
    }
}
