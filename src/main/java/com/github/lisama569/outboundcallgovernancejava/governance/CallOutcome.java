package com.github.lisama569.outboundcallgovernancejava.governance;

/**
 * 一次出站调用的统一结果，包含判定依据，便于解释与断言。
 */
public record CallOutcome(
        String requestId,
        String target,
        String idempotencyKey,
        Status status,
        RejectionReason rejectionReason,
        IdempotencyStatus idempotencyStatus,
        int attempts,
        CircuitState circuitStateAfter,
        String value,
        String detail,
        FallbackKind fallbackKind) {

    public enum Status {
        SUCCESS,
        REJECTED,
        FAILED
    }

    /**
     * 结果来源标识：区分真实成功与降级返回，以及降级使用的是静态值还是降级目标。
     */
    public enum FallbackKind {
        /** 未走降级路径（真实成功或真实失败） */
        NONE,
        /** 主调用失败，返回了配置的静态兜底值 */
        STATIC_VALUE,
        /** 主调用失败，结果来自降级目标（或降级目标也失败） */
        FALLBACK_TARGET
    }

    public static CallOutcome success(String requestId, String target, String key,
                                      IdempotencyStatus idem, int attempts,
                                      CircuitState circuit, String value) {
        return new CallOutcome(requestId, target, key, Status.SUCCESS, null,
                idem, attempts, circuit, value, "ok", FallbackKind.NONE);
    }

    /** 降级成功：status 为 SUCCESS，但 fallbackKind 标明结果来自降级路径。 */
    public static CallOutcome fallback(String requestId, String target, String key,
                                       IdempotencyStatus idem, int attempts,
                                       CircuitState circuit, String value,
                                       FallbackKind kind, String detail) {
        return new CallOutcome(requestId, target, key, Status.SUCCESS, null,
                idem, attempts, circuit, value, detail, kind);
    }

    public static CallOutcome rejected(String requestId, String target, String key,
                                       RejectionReason reason, IdempotencyStatus idem,
                                       CircuitState circuit, String detail) {
        return new CallOutcome(requestId, target, key, Status.REJECTED, reason,
                idem, 0, circuit, null, detail, FallbackKind.NONE);
    }

    public static CallOutcome failed(String requestId, String target, String key,
                                     RejectionReason reason, IdempotencyStatus idem,
                                     int attempts, CircuitState circuit, String detail) {
        return new CallOutcome(requestId, target, key, Status.FAILED, reason,
                idem, attempts, circuit, null, detail, FallbackKind.NONE);
    }

    /** 复制结果并覆盖降级标识（用于降级目标也失败时保留降级路径标记）。 */
    public CallOutcome withFallbackKind(FallbackKind kind, String detail) {
        return new CallOutcome(requestId, target, idempotencyKey, status, rejectionReason,
                idempotencyStatus, attempts, circuitStateAfter, value, detail, kind);
    }
}
