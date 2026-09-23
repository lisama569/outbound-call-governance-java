package com.github.lisama569.outboundcallgovernancejava.governance;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/**
 * 幂等注册表：同一幂等键只产生一次实质执行。
 * - 首个请求触发执行，期间到达的重复请求挂接到同一在途执行（JOINED_IN_PROGRESS）；
 * - 执行完成后结果保留，后续重复请求直接回放在途已完成结果（REPLAYED_AFTER_COMPLETION）；
 * - 所有挂接/回放请求拿到与首次执行一致的结果。
 */
class IdempotencyRegistry {

    private final ConcurrentHashMap<String, CompletableFuture<CallOutcome>> byKey =
            new ConcurrentHashMap<>();

    /**
     * 对给定幂等键执行或挂接。
     *
     * @param key      幂等键
     * @param executor 实质执行逻辑（仅首个请求会触发）
     * @return 执行结果与幂等状态
     */
    Execution execute(String key, Supplier<CallOutcome> executor) {
        CompletableFuture<CallOutcome> fresh = new CompletableFuture<>();
        CompletableFuture<CallOutcome> existing = byKey.putIfAbsent(key, fresh);
        if (existing == null) {
            // 本请求负责实质执行
            try {
                fresh.complete(executor.get());
            } catch (RuntimeException e) {
                byKey.remove(key, fresh);
                fresh.completeExceptionally(e);
                throw e;
            }
            return new Execution(fresh.join(), IdempotencyStatus.FRESH_EXECUTION);
        }
        IdempotencyStatus status = existing.isDone()
                ? IdempotencyStatus.REPLAYED_AFTER_COMPLETION
                : IdempotencyStatus.JOINED_IN_PROGRESS;
        return new Execution(existing.join(), status);
    }

    /** 测试/运维用：清除指定键的记录。 */
    void evict(String key) {
        byKey.remove(key);
    }

    record Execution(CallOutcome outcome, IdempotencyStatus status) {
    }
}
