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

    /** 降级路径标识：NONE 为真实（主调用）结果，其余为降级返回。 */
    public enum FallbackKind {
        /** 未走降级，结果来自主调用本身 */
        NONE,
        /** 主调用失败，返回静态兜底值 */
        STATIC_VALUE,
        /** 主调用失败，转入降级目标（结果含降级目标自身的治理判定） */
        FALLBACK_TARGET
    }

    public static CallOutcome success(String requestId, String target, String key,
                                      IdempotencyStatus idem, int attempts,
                                      CircuitState circuit, String value) {
        return new CallOutcome(requestId, target, key, Status.SUCCESS, null,
                idem, attempts, circuit, value, "ok", FallbackKind.NONE);
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

    /** 静态兜底值降级成功。 */
    public static CallOutcome fallbackStatic(String requestId, String target, String key,
                                             IdempotencyStatus idem, int attempts,
                                             CircuitState circuit, String value, String detail) {
        return new CallOutcome(requestId, target, key, Status.SUCCESS, null,
                idem, attempts, circuit, value, detail, FallbackKind.STATIC_VALUE);
    }

    /**
     * 以降级目标的调用结果作为本次请求的最终结果。
     * 保留降级目标自身的状态/拒绝原因，仅把目标标识与降级路径标记回填到主目标视角。
     */
    public static CallOutcome fromFallbackTarget(CallOutcome primary, CallOutcome fallbackOutcome) {
        return new CallOutcome(primary.requestId(), primary.target(), primary.idempotencyKey(),
                fallbackOutcome.status(), fallbackOutcome.rejectionReason(),
                primary.idempotencyStatus(),
                primary.attempts() + fallbackOutcome.attempts(),
                fallbackOutcome.circuitStateAfter(),
                fallbackOutcome.value(),
                "fallback via target: " + fallbackOutcome.detail(),
                FallbackKind.FALLBACK_TARGET);
    }

    /** 复制结果并覆盖幂等状态（挂接/回放场景）。 */
    public CallOutcome withIdempotencyStatus(IdempotencyStatus status) {
        return new CallOutcome(requestId, target, idempotencyKey, this.status, rejectionReason,
                status, attempts, circuitStateAfter, value, detail, fallbackKind);
    }

    /** 预算不足以发起降级调用：不再发起任何新调用，最终结果为降级也失败。 */
    public static CallOutcome fallbackNotAttempted(CallOutcome primary, String detail) {
        return new CallOutcome(primary.requestId(), primary.target(), primary.idempotencyKey(),
                Status.FAILED, RejectionReason.BUDGET_EXHAUSTED, primary.idempotencyStatus(),
                primary.attempts(), primary.circuitStateAfter(), null, detail,
                FallbackKind.FALLBACK_TARGET);
    }
}
