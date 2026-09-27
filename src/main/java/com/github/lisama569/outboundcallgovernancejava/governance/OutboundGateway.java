package com.github.lisama569.outboundcallgovernancejava.governance;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * 出站调用统一入口。
 * 编排顺序：幂等去重 -> 配额 -> 并发 -> 熔断许可 -> 超时/重试（受耗时预算约束）。
 * 每个判定节点打印请求标识、判定依据与熔断状态。
 */
@Service
public class OutboundGateway {

    private static final Logger log = LoggerFactory.getLogger(OutboundGateway.class);

    private final GovernanceConfigRegistry configRegistry;
    private final DownstreamClient downstreamClient;
    private final IdempotencyRegistry idempotencyRegistry = new IdempotencyRegistry();
    private final ExecutorService executor = Executors.newCachedThreadPool();

    public OutboundGateway(GovernanceConfigRegistry configRegistry,
                           DownstreamClient downstreamClient) {
        this.configRegistry = configRegistry;
        this.downstreamClient = downstreamClient;
    }

    /**
     * 发起一次受治理的出站调用。
     *
     * @param target         调用目标
     * @param idempotencyKey 幂等键（相同键的并发/重试请求只产生一次实质执行）
     * @param payload        请求内容
     * @return 统一调用结果（含拒绝原因、幂等状态、熔断状态）
     */
    public CallOutcome call(String target, String idempotencyKey, String payload) {
        String requestId = UUID.randomUUID().toString();
        String key = (idempotencyKey == null || idempotencyKey.isBlank())
                ? requestId : idempotencyKey;
        String scopedKey = target + "|" + key;
        // 整次请求（含主调用与可能的降级调用）共享同一份耗时预算
        long deadline = System.currentTimeMillis()
                + configRegistry.configOf(target).retryBudgetMillis();
        Set<String> chain = new HashSet<>();
        chain.add(target);
        IdempotencyRegistry.Execution execution = idempotencyRegistry.execute(
                scopedKey, () -> doCall(requestId, target, key, payload, deadline, chain));
        CallOutcome outcome = execution.outcome();
        if (execution.status() != IdempotencyStatus.FRESH_EXECUTION) {
            log.info("requestId={} target={} key={} idempotent-join status={} originalRequestId={}",
                    requestId, target, key, execution.status(), outcome.requestId());
            outcome = new CallOutcome(outcome.requestId(), outcome.target(), outcome.idempotencyKey(),
                    outcome.status(), outcome.rejectionReason(), execution.status(),
                    outcome.attempts(), outcome.circuitStateAfter(), outcome.value(), outcome.detail(),
                    outcome.fallbackKind());
        }
        return outcome;
    }

    private CallOutcome doCall(String requestId, String target, String key, String payload,
                               long deadline, Set<String> chain) {
        GovernanceConfigRegistry.TargetState state = configRegistry.stateOf(target);
        TargetConfig config = state.config;
        QuotaLimiter limiter = state.limiter;
        CircuitBreaker breaker = state.breaker;

        // 1. 配额
        if (!limiter.tryAcquireQuota()) {
            log.info("requestId={} target={} key={} REJECTED reason={} basis=quota {}/{} circuit={}",
                    requestId, target, key, RejectionReason.QUOTA_EXHAUSTED,
                    limiter.currentWindowUsage(), limiter.quotaPerWindow(), breaker.state());
            return maybeFallback(requestId, target, key, payload, deadline, chain, config, breaker,
                    CallOutcome.rejected(requestId, target, key, RejectionReason.QUOTA_EXHAUSTED,
                            IdempotencyStatus.FRESH_EXECUTION, breaker.state(),
                            "quota exhausted for target " + target));
        }
        // 2. 并发
        if (!limiter.tryAcquireConcurrency()) {
            limiter.refundQuota();
            log.info("requestId={} target={} key={} REJECTED reason={} basis=in-flight {} circuit={}",
                    requestId, target, key, RejectionReason.CONCURRENCY_SATURATED,
                    limiter.currentInFlight(), breaker.state());
            return maybeFallback(requestId, target, key, payload, deadline, chain, config, breaker,
                    CallOutcome.rejected(requestId, target, key, RejectionReason.CONCURRENCY_SATURATED,
                            IdempotencyStatus.FRESH_EXECUTION, breaker.state(),
                            "concurrency limit reached for target " + target));
        }
        // 3. 熔断许可
        if (!breaker.tryPermit()) {
            limiter.refundQuota();
            limiter.releaseConcurrency();
            log.info("requestId={} target={} key={} REJECTED reason={} basis=circuit={} consecutiveFailures={}",
                    requestId, target, key, RejectionReason.CIRCUIT_OPEN,
                    breaker.state(), breaker.consecutiveFailures());
            return maybeFallback(requestId, target, key, payload, deadline, chain, config, breaker,
                    CallOutcome.rejected(requestId, target, key, RejectionReason.CIRCUIT_OPEN,
                            IdempotencyStatus.FRESH_EXECUTION, breaker.state(),
                            "circuit " + breaker.state() + " for target " + target));
        }

        try {
            // 4. 超时 + 重试（受耗时预算约束）
            CallOutcome outcome = executeWithRetry(requestId, target, key, payload,
                    config, breaker, deadline);
            if (outcome.status() == CallOutcome.Status.SUCCESS) {
                return outcome;
            }
            return maybeFallback(requestId, target, key, payload, deadline, chain,
                    config, breaker, outcome);
        } finally {
            limiter.releaseConcurrency();
        }
    }

    /**
     * 主路径未成功时按配置进入降级路径。
     * 降级不是旁路：降级目标走同一套 doCall，受其自身配额/并发/熔断/预算约束；
     * 整次请求共享 deadline，预算不足时不再发起降级调用。
     */
    private CallOutcome maybeFallback(String requestId, String target, String key, String payload,
                                      long deadline, Set<String> chain, TargetConfig config,
                                      CircuitBreaker breaker, CallOutcome primary) {
        FallbackConfig fallback = config.fallback();
        if (fallback == null || fallback.kind() == FallbackConfig.Kind.NONE) {
            return primary;
        }
        if (fallback.kind() == FallbackConfig.Kind.STATIC_VALUE) {
            // 静态兜底不发起调用，不受剩余预算限制
            log.info("requestId={} target={} key={} FALLBACK kind=STATIC_VALUE after={}",
                    requestId, target, key, primary.rejectionReason());
            return CallOutcome.fallback(requestId, target, key,
                    IdempotencyStatus.FRESH_EXECUTION, primary.attempts(), breaker.state(),
                    fallback.staticValue(), CallOutcome.FallbackKind.STATIC_VALUE,
                    "static fallback after primary " + primary.rejectionReason());
        }
        String fallbackTarget = fallback.fallbackTarget();
        long remaining = deadline - System.currentTimeMillis();
        long fallbackCallTimeout = configRegistry.configOf(fallbackTarget).callTimeoutMillis();
        if (remaining < fallbackCallTimeout) {
            // 剩余预算不足以完整跑一次降级调用：不再发起，就地收口为降级也失败，
            // 避免把预算花光之后还把整趟降级调用跑完、拖过整次请求的预算
            log.info("requestId={} target={} key={} FALLBACK_SKIPPED target={} reason=insufficient budget remainingMs={} requiredMs={}",
                    requestId, target, key, fallbackTarget, remaining, fallbackCallTimeout);
            return primary.withFallbackKind(CallOutcome.FallbackKind.FALLBACK_TARGET,
                    "fallback to " + fallbackTarget + " not attempted: remaining budget "
                            + remaining + "ms < fallback call timeout " + fallbackCallTimeout + "ms");
        }
        if (!chain.add(fallbackTarget)) {
            // 配置更新已拒绝成环，此处为运行期兜底防御
            log.info("requestId={} target={} key={} FALLBACK_SKIPPED target={} reason=cycle",
                    requestId, target, key, fallbackTarget);
            return primary.withFallbackKind(CallOutcome.FallbackKind.FALLBACK_TARGET,
                    "fallback to " + fallbackTarget + " not attempted: cycle in fallback chain");
        }
        log.info("requestId={} target={} key={} FALLBACK kind=FALLBACK_TARGET target={} after={} budgetRemainingMs={}",
                requestId, target, key, fallbackTarget, primary.rejectionReason(), remaining);
        CallOutcome fb = doCall(requestId, fallbackTarget, key, payload, deadline, chain);
        if (fb.status() == CallOutcome.Status.SUCCESS) {
            return fb.withFallbackKind(CallOutcome.FallbackKind.FALLBACK_TARGET,
                    "fallback via target " + fallbackTarget
                            + " after primary " + primary.rejectionReason());
        }
        // 降级目标也失败：不再继续回退（除非其自身配置了下一级降级，已在递归中处理）
        return fb.withFallbackKind(CallOutcome.FallbackKind.FALLBACK_TARGET,
                "fallback target " + fallbackTarget + " also failed: " + fb.detail());
    }

    private CallOutcome executeWithRetry(String requestId, String target, String key,
                                         String payload, TargetConfig config,
                                         CircuitBreaker breaker, long requestDeadline) {
        long start = System.currentTimeMillis();
        // 本目标的重试预算与整次请求的共享预算取更紧者
        long deadline = Math.min(requestDeadline, start + config.retryBudgetMillis());
        int attempts = 0;
        String lastError = null;
        boolean budgetExhausted = false;

        while (true) {
            // 任何一次尝试（含首次）发起前都必须确认预算未耗尽
            long remaining = deadline - System.currentTimeMillis();
            if (remaining <= 0) {
                budgetExhausted = true;
                break;
            }
            if (attempts > 0) {
                long backoff = Math.min(config.backoffMillis() * attempts, remaining);
                try {
                    Thread.sleep(backoff);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
                remaining = deadline - System.currentTimeMillis();
                if (remaining <= 0) {
                    budgetExhausted = true;
                    break;
                }
            }
            attempts++;
            // 单次调用超时不得越过剩余预算：整次请求的耗时预算优先于单次超时配置
            long attemptTimeout = Math.min(config.callTimeoutMillis(), remaining);
            log.info("requestId={} target={} key={} attempt={}/{} attemptTimeoutMs={} budgetRemainingMs={} circuit={}",
                    requestId, target, key, attempts, config.maxAttempts(),
                    attemptTimeout, remaining, breaker.state());
            try {
                String value = invokeWithTimeout(target, payload, attemptTimeout);
                if (System.currentTimeMillis() > deadline) {
                    // 结果回来时预算已超：按失败收口，不把超长等待的结果当正常返回
                    budgetExhausted = true;
                    lastError = "call returned after request budget exhausted";
                    break;
                }
                breaker.onSuccess();
                log.info("requestId={} target={} key={} SUCCESS attempts={} circuit={}",
                        requestId, target, key, attempts, breaker.state());
                return CallOutcome.success(requestId, target, key,
                        IdempotencyStatus.FRESH_EXECUTION, attempts, breaker.state(), value);
            } catch (DownstreamException e) {
                lastError = e.getMessage();
                log.info("requestId={} target={} key={} attempt={} failed retryable={} error={}",
                        requestId, target, key, attempts, e.retryable(), e.getMessage());
                if (deadline - System.currentTimeMillis() <= 0) {
                    // 本次尝试已耗尽预算（如单次超时被剩余预算截断后超时）
                    budgetExhausted = true;
                    break;
                }
                if (!e.retryable() || attempts >= config.maxAttempts()) {
                    break;
                }
            }
        }

        breaker.onFailure();
        RejectionReason reason = budgetExhausted
                ? RejectionReason.BUDGET_EXHAUSTED
                : RejectionReason.DOWNSTREAM_FAILURE;
        log.info("requestId={} target={} key={} FAILED reason={} attempts={} circuit={} consecutiveFailures={}",
                requestId, target, key, reason, attempts, breaker.state(), breaker.consecutiveFailures());
        return CallOutcome.failed(requestId, target, key, reason,
                IdempotencyStatus.FRESH_EXECUTION, attempts, breaker.state(),
                lastError == null ? "retry budget exhausted" : lastError);
    }

    private String invokeWithTimeout(String target, String payload, long timeoutMillis)
            throws DownstreamException {
        Future<String> future = executor.submit(() -> downstreamClient.invoke(target, payload));
        try {
            return future.get(timeoutMillis, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            future.cancel(true);
            throw new DownstreamException("call timed out after " + timeoutMillis + "ms", true);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            future.cancel(true);
            throw new DownstreamException("interrupted", false);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof DownstreamException de) {
                throw de;
            }
            throw new DownstreamException("downstream error: " + e.getCause(), false);
        }
    }

    /** 查询目标当前熔断状态（可观察性）。 */
    public CircuitState circuitStateOf(String target) {
        return configRegistry.stateOf(target).breaker.state();
    }

    /** 更新目标配置，运行中生效且只影响该目标。 */
    public Optional<ConfigRejection> updateConfig(String target, TargetConfig config) {
        Optional<ConfigRejection> result = configRegistry.update(target, config);
        result.ifPresentOrElse(
                r -> log.info("config update target={} REJECTED reason={} detail={}",
                        target, r.reason(), r.message()),
                () -> log.info("config update target={} ACCEPTED config={}", target, config));
        return result;
    }
}
