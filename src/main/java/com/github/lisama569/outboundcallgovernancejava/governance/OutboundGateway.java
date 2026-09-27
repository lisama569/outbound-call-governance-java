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
        IdempotencyRegistry.Execution execution = idempotencyRegistry.execute(
                scopedKey, () -> doCall(requestId, target, key, payload));
        CallOutcome outcome = execution.outcome();
        if (execution.status() != IdempotencyStatus.FRESH_EXECUTION) {
            log.info("requestId={} target={} key={} idempotent-join status={} originalRequestId={}",
                    requestId, target, key, execution.status(), outcome.requestId());
            outcome = outcome.withIdempotencyStatus(execution.status());
        }
        return outcome;
    }

    private CallOutcome doCall(String requestId, String target, String key, String payload) {
        // 整次请求（含主调用与降级链）的耗时预算起点：主目标自身的 retryBudgetMillis
        long deadline = System.currentTimeMillis()
                + configRegistry.configOf(target).retryBudgetMillis();
        return executeWithFallback(requestId, target, key, payload, deadline, new HashSet<>());
    }

    /**
     * 受治理执行 + 降级编排。主调用因下游持续失败、重试耗尽或熔断打开而失败时，
     * 按目标配置转入降级路径（静态兜底值或降级目标）；降级目标同样走完整治理链路，
     * 且整次请求不得超出 deadline——预算不足时不再发起任何新调用。
     */
    private CallOutcome executeWithFallback(String requestId, String target, String key,
                                            String payload, long deadline, Set<String> chain) {
        chain.add(target);
        CallOutcome primary = executeGoverned(requestId, target, key, payload, deadline);
        if (primary.status() == CallOutcome.Status.SUCCESS) {
            return primary;
        }
        RejectionReason reason = primary.rejectionReason();
        boolean fallbackable = reason == RejectionReason.CIRCUIT_OPEN
                || reason == RejectionReason.DOWNSTREAM_FAILURE
                || reason == RejectionReason.BUDGET_EXHAUSTED;
        FallbackConfig fallback = configRegistry.configOf(target).fallback();
        if (!fallbackable || fallback.isNone()) {
            return primary;
        }
        if (fallback.staticValue() != null) {
            log.info("requestId={} target={} key={} FALLBACK kind=STATIC_VALUE trigger={}",
                    requestId, target, key, reason);
            return CallOutcome.fallbackStatic(requestId, target, key,
                    primary.idempotencyStatus(), primary.attempts(), primary.circuitStateAfter(),
                    fallback.staticValue(), "static fallback after " + reason);
        }
        String fallbackTarget = fallback.fallbackTarget();
        long remaining = deadline - System.currentTimeMillis();
        if (remaining <= 0) {
            // 预算不足以再发起一次调用：不再发起，最终结果为降级也失败
            log.info("requestId={} target={} key={} FALLBACK kind=FALLBACK_TARGET fallbackTarget={} "
                            + "NOT-ATTEMPTED reason=budget exhausted trigger={}",
                    requestId, target, key, fallbackTarget, reason);
            return CallOutcome.fallbackNotAttempted(primary,
                    "fallback to " + fallbackTarget + " not attempted: request budget exhausted");
        }
        if (chain.contains(fallbackTarget)) {
            // 运行期防护：配置层已拒绝成环，此处兜底避免无限回退
            log.info("requestId={} target={} key={} FALLBACK kind=FALLBACK_TARGET fallbackTarget={} "
                            + "NOT-ATTEMPTED reason=cycle in fallback chain",
                    requestId, target, key, fallbackTarget);
            return CallOutcome.fallbackNotAttempted(primary,
                    "fallback to " + fallbackTarget + " not attempted: cycle in fallback chain");
        }
        log.info("requestId={} target={} key={} FALLBACK kind=FALLBACK_TARGET fallbackTarget={} "
                        + "trigger={} budgetRemainingMs={}",
                requestId, target, key, fallbackTarget, reason, remaining);
        CallOutcome fallbackOutcome =
                executeWithFallback(requestId, fallbackTarget, key, payload, deadline, chain);
        return CallOutcome.fromFallbackTarget(primary, fallbackOutcome);
    }

    /**
     * 单目标的受治理执行：配额 -> 并发 -> 熔断许可 -> 超时/重试。
     * 耗时预算由调用方给定（主调用为自身预算，降级调用为整次请求的剩余预算），
     * 单次超时与重试预算均被钳制在剩余预算之内。
     */
    private CallOutcome executeGoverned(String requestId, String target, String key,
                                        String payload, long deadline) {
        GovernanceConfigRegistry.TargetState state = configRegistry.stateOf(target);
        TargetConfig config = clampToRemainingBudget(state.config, deadline);
        QuotaLimiter limiter = state.limiter;
        CircuitBreaker breaker = state.breaker;

        // 1. 配额
        if (!limiter.tryAcquireQuota()) {
            log.info("requestId={} target={} key={} REJECTED reason={} basis=quota {}/{} circuit={}",
                    requestId, target, key, RejectionReason.QUOTA_EXHAUSTED,
                    limiter.currentWindowUsage(), limiter.quotaPerWindow(), breaker.state());
            return CallOutcome.rejected(requestId, target, key, RejectionReason.QUOTA_EXHAUSTED,
                    IdempotencyStatus.FRESH_EXECUTION, breaker.state(),
                    "quota exhausted for target " + target);
        }
        // 2. 并发
        if (!limiter.tryAcquireConcurrency()) {
            limiter.refundQuota();
            log.info("requestId={} target={} key={} REJECTED reason={} basis=in-flight {} circuit={}",
                    requestId, target, key, RejectionReason.CONCURRENCY_SATURATED,
                    limiter.currentInFlight(), breaker.state());
            return CallOutcome.rejected(requestId, target, key, RejectionReason.CONCURRENCY_SATURATED,
                    IdempotencyStatus.FRESH_EXECUTION, breaker.state(),
                    "concurrency limit reached for target " + target);
        }
        // 3. 熔断许可
        if (!breaker.tryPermit()) {
            limiter.refundQuota();
            limiter.releaseConcurrency();
            log.info("requestId={} target={} key={} REJECTED reason={} basis=circuit={} consecutiveFailures={}",
                    requestId, target, key, RejectionReason.CIRCUIT_OPEN,
                    breaker.state(), breaker.consecutiveFailures());
            return CallOutcome.rejected(requestId, target, key, RejectionReason.CIRCUIT_OPEN,
                    IdempotencyStatus.FRESH_EXECUTION, breaker.state(),
                    "circuit " + breaker.state() + " for target " + target);
        }

        try {
            // 4. 超时 + 重试（受耗时预算约束）
            return executeWithRetry(requestId, target, key, payload, config, breaker, deadline);
        } finally {
            limiter.releaseConcurrency();
        }
    }

    /**
     * 将单次超时与重试预算钳制在整次请求的剩余预算之内，
     * 保证主调用与降级调用整体不超出请求级 deadline。
     */
    private TargetConfig clampToRemainingBudget(TargetConfig config, long deadline) {
        long remaining = Math.max(1, deadline - System.currentTimeMillis());
        long callTimeout = Math.min(config.callTimeoutMillis(), remaining);
        long retryBudget = Math.min(config.retryBudgetMillis(), remaining);
        if (callTimeout == config.callTimeoutMillis() && retryBudget == config.retryBudgetMillis()) {
            return config;
        }
        return new TargetConfig(config.quotaPerWindow(), config.windowMillis(),
                config.maxConcurrency(), callTimeout, config.maxAttempts(), config.backoffMillis(),
                retryBudget, config.circuitFailureThreshold(), config.circuitResetMillis(),
                config.fallback());
    }

    private CallOutcome executeWithRetry(String requestId, String target, String key,
                                         String payload, TargetConfig config,
                                         CircuitBreaker breaker, long deadline) {
        int attempts = 0;
        String lastError = null;
        boolean budgetExhausted = false;

        while (true) {
            if (attempts > 0) {
                // 预算耗尽后不得继续发起调用
                long remaining = deadline - System.currentTimeMillis();
                if (remaining <= 0) {
                    budgetExhausted = true;
                    break;
                }
                long backoff = Math.min(config.backoffMillis() * attempts, remaining);
                try {
                    Thread.sleep(backoff);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
            attempts++;
            log.info("requestId={} target={} key={} attempt={}/{} budgetRemainingMs={} circuit={}",
                    requestId, target, key, attempts, config.maxAttempts(),
                    deadline - System.currentTimeMillis(), breaker.state());
            try {
                String value = invokeWithTimeout(target, payload, config.callTimeoutMillis());
                breaker.onSuccess();
                log.info("requestId={} target={} key={} SUCCESS attempts={} circuit={}",
                        requestId, target, key, attempts, breaker.state());
                return CallOutcome.success(requestId, target, key,
                        IdempotencyStatus.FRESH_EXECUTION, attempts, breaker.state(), value);
            } catch (DownstreamException e) {
                lastError = e.getMessage();
                log.info("requestId={} target={} key={} attempt={} failed retryable={} error={}",
                        requestId, target, key, attempts, e.retryable(), e.getMessage());
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
