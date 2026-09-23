package com.github.lisama569.outboundcallgovernancejava.governance;

/**
 * 一次出站调用的上下文（请求标识、幂等键、负载）。
 */
public class CallContext {

    private final String requestId;
    private final String idempotencyKey;
    private final String payload;

    public CallContext(String requestId, String idempotencyKey, String payload) {
        this.requestId = requestId;
        this.idempotencyKey = idempotencyKey;
        this.payload = payload;
    }

    public String requestId() {
        return requestId;
    }

    public String idempotencyKey() {
        return idempotencyKey;
    }

    public String payload() {
        return payload;
    }
}
