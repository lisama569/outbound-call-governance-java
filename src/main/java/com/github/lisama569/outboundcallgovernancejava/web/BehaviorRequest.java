package com.github.lisama569.outboundcallgovernancejava.web;

/**
 * 模拟下游行为配置。
 * mode: OK / TIMEOUT / FAIL / FAIL_FIRST_N / SLOW
 */
public record BehaviorRequest(String mode, Long latencyMillis, Integer failFirstN) {
}
