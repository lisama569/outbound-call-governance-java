package com.github.lisama569.outboundcallgovernancejava.model;

/**
 * 配置变更等场景抛出的受检异常，携带可区分原因。
 */
public class GovernanceRejectionException extends RuntimeException {

    private final RejectReason reason;

    public GovernanceRejectionException(RejectReason reason, String message) {
        super(message);
        this.reason = reason;
    }

    public RejectReason reason() {
        return reason;
    }
}
