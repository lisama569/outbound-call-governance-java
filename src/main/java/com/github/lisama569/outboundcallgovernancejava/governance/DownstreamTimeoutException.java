package com.github.lisama569.outboundcallgovernancejava.governance;

/** 下游调用超时（可重试）。 */
public class DownstreamTimeoutException extends RuntimeException {
    public DownstreamTimeoutException(String message) {
        super(message);
    }
}
