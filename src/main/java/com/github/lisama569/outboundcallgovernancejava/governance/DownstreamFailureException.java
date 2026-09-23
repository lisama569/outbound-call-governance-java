package com.github.lisama569.outboundcallgovernancejava.governance;

/** 下游可重试故障（如 5xx、连接重置）。 */
public class DownstreamFailureException extends RuntimeException {
    public DownstreamFailureException(String message) {
        super(message);
    }
}
