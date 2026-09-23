package com.github.lisama569.outboundcallgovernancejava.model;

/**
 * 熔断器状态机：CLOSED -> OPEN -> HALF_OPEN -> CLOSED / OPEN。
 */
public enum CircuitState {
    CLOSED,
    OPEN,
    HALF_OPEN
}
