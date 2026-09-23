package com.github.lisama569.outboundcallgovernancejava.model;

/**
 * 一次治理出站调用的结果，稳定且可解释。
 *
 * @param status      调用状态
 * @param value       成功时的返回值（真实结果或回退值）
 * @param reason      REJECTED / FAILED 时的具体原因
 * @param requestId   请求标识
 * @param target      实际提供结果的目标名
 * @param attempts    真实下游尝试次数
 * @param elapsedMillis 总耗时（毫秒）
 * @param fallback    结果是否来自回退
 */
public record CallOutcome(
        CallStatus status,
        String value,
        RejectReason reason,
        String requestId,
        String target,
        int attempts,
        long elapsedMillis,
        boolean fallback
) {

    public static CallOutcome success(String value, String requestId, String target,
                                      int attempts, long elapsedMillis) {
        return new CallOutcome(CallStatus.SUCCESS, value, null, requestId, target,
                attempts, elapsedMillis, false);
    }

    public static CallOutcome fallback(String value, String requestId, String target,
                                       int attempts, long elapsedMillis) {
        return new CallOutcome(CallStatus.FALLBACK, value, null, requestId, target,
                attempts, elapsedMillis, true);
    }

    public static CallOutcome failed(RejectReason reason, String requestId, String target,
                                     int attempts, long elapsedMillis) {
        return new CallOutcome(CallStatus.FAILED, null, reason, requestId, target,
                attempts, elapsedMillis, false);
    }

    public static CallOutcome rejected(RejectReason reason, String requestId, String target,
                                       long elapsedMillis) {
        return new CallOutcome(CallStatus.REJECTED, null, reason, requestId, target,
                0, elapsedMillis, false);
    }

    /** 幂等跟随者拿到的是 LEADER 的结果，但保留自己的请求标识以便追踪。 */
    public CallOutcome withRequestId(String newRequestId) {
        return new CallOutcome(status, value, reason, newRequestId, target,
                attempts, elapsedMillis, fallback);
    }
}
