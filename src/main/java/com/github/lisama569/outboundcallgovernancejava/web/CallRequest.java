package com.github.lisama569.outboundcallgovernancejava.web;

/** 调用请求体。 */
public record CallRequest(String requestId, String idempotencyKey, String payload) {
}
