package com.github.lisama569.outboundcallgovernancejava.governance;

/**
 * 配置更新被拒绝的结果，原因可区分。
 */
public record ConfigRejection(Reason reason, String message) {

    public enum Reason {
        /** 配置值本身非法（如负数、零） */
        INVALID_CONFIG,
        /** 配置值合法但与当前运行状态冲突（如回退配额/并发到低于在途用量） */
        ROLLBACK_CONFLICT
    }
}
