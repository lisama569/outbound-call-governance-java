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
        String detail) {

    public enum Status {
        SUCCESS,
        REJECTED,
        FAILED
    }

    public static CallOutcome success(String requestId, String target, String key,
                                      IdempotencyStatus idem, int attempts,
                                      CircuitState circuit, String value) {
        return new CallOutcome(requestId, target, key, Status.SUCCESS, null,
                idem, attempts, circuit, value, "ok");
    }

    public static CallOutcome rejected(String requestId, String target, String key,
                                       RejectionReason reason, IdempotencyStatus idem,
                                       CircuitState circuit, String detail) {
        return new CallOutcome(requestId, target, key, Status.REJECTED, reason,
                idem, 0, circuit, null, detail);
    }

    public static CallOutcome failed(String requestId, String target, String key,
                                     RejectionReason reason, IdempotencyStatus idem,
                                     int attempts, CircuitState circuit, String detail) {
        return new CallOutcome(requestId, target, key, Status.FAILED, reason,
                idem, attempts, circuit, null, detail);
    }
}
