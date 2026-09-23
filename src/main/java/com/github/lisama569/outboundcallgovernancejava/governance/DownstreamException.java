package com.github.lisama569.outboundcallgovernancejava.governance;

/**
 * 下游调用异常，标记是否可重试。
 */
public class DownstreamException extends Exception {
    private final boolean retryable;

    public DownstreamException(String message, boolean retryable) {
        super(message);
        this.retryable = retryable;
    }

    public boolean retryable() {
        return retryable;
    }
}
