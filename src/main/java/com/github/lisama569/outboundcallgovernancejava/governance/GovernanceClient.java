package com.github.lisama569.outboundcallgovernancejava.governance;

import com.github.lisama569.outboundcallgovernancejava.idempotency.IdempotencyStore;
import com.github.lisama569.outboundcallgovernancejava.model.CallOutcome;
import com.github.lisama569.outboundcallgovernancejava.model.CallStatus;
import com.github.lisama569.outboundcallgovernancejava.model.RejectReason;
import com.github.lisama569.outboundcallgovernancejava.model.TargetConfig;
import com.github.lisama569.outboundcallgovernancejava.quota.QuotaState;
import com.github.lisama569.outboundcallgovernancejava.time.Sleeper;
import com.github.lisama569.outboundcallgovernancejava.time.TimeSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 出站调用治理门面。为每次调用依次执行：
 * 幂等去重 -&gt; 熔断前检 -&gt; 配额/并发申请 -&gt; 超时 + 预算受限的重试退避 -&gt; 结果/回退。
 *
 * <p>重试语义：仅对超时（{@link DownstreamTimeoutException}）与可重试故障
 * （{@link DownstreamFailureException}）重试；指数退避 {@code backoffMillis*2^(n-1)}；
 * 每次尝试前都检查耗时预算，预算（连退避都放不下时）耗尽立即停止，不再发起调用；
 * 达到 maxAttempts 则以下游失败告终。两种终止原因通过
 * {@link RejectReason#RETRY_BUDGET_EXHAUSTED} 与 {@link RejectReason#DOWNSTREAM_FAILURE} 区分。</p>
 *
 * <p>日志统一打印请求标识、判定依据与熔断状态。</p>
 */
public class GovernanceClient implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(GovernanceClient.class);

    /** 回退链最大深度，防御性上限（成环在配置层已被拒绝）。 */
    static final int MAX_FALLBACK_DEPTH = 4;

    private final GovernanceRegistry registry;
    private final IdempotencyStore idempotencyStore;
    private final DownstreamInvoker invoker;
    private final TimeSource timeSource;
    private final Sleeper sleeper;
    private final ExecutorService timeoutExecutor;

    public GovernanceClient(GovernanceRegistry registry,
                            IdempotencyStore idempotencyStore,
                            DownstreamInvoker invoker,
                            TimeSource timeSource,
                            Sleeper sleeper) {
        this.registry = registry;
        this.idempotencyStore = idempotencyStore;
        this.invoker = invoker;
        this.timeSource = timeSource;
        this.sleeper = sleeper;
        AtomicInteger seq = new AtomicInteger();
        ThreadFactory tf = r -> {
            Thread t = new Thread(r, "downstream-timeout-" + seq.incrementAndGet());
            t.setDaemon(true);
            return t;
        };
        this.timeoutExecutor = Executors.newCachedThreadPool(tf);
    }

    /** 执行一次被治理的出站调用。 */
    public CallOutcome call(String target, CallContext ctx) {
        long start = timeSource.nowMillis();
        String requestId = ctx.requestId();
        log.info("[{}] 出站调用开始 target={} idempotencyKey={}", requestId, target, ctx.idempotencyKey());

        Optional<TargetState> targetOpt = registry.get(target);
        if (targetOpt.isEmpty()) {
            log.warn("[{}] 拒绝调用 target={} 判定依据=UNKNOWN_TARGET", requestId, target);
            return CallOutcome.rejected(RejectReason.UNKNOWN_TARGET, requestId, target, elapsed(start));
        }
        TargetState state = targetOpt.get();

        String key = ctx.idempotencyKey();
        if (key != null && !key.isBlank()) {
            try {
                long waitBudget = joinWaitBudget(state.config());
                IdempotencyStore.Attempt attempt = idempotencyStore.acquire(target, key, waitBudget);
                switch (attempt.status()) {
                    case COMPLETED -> {
                        log.info("[{}] 幂等命中已完成结果 target={} key={} 判定依据=IDEMPOTENT_COMPLETED",
                                requestId, target, key);
                        return attempt.completed().withRequestId(requestId);
                    }
                    case IN_FLIGHT -> {
                        // 区分“执行中”：日志打印持有者线程；跟随者不自行执行，等待唯一 LEADER 的一致结果
                        log.info("[{}] 幂等命中执行中 target={} key={} holder={} 判定依据=IDEMPOTENT_IN_FLIGHT",
                                requestId, target, key, attempt.holder().orElse("?"));
                        IdempotencyStore.Attempt joined =
                                idempotencyStore.acquire(target, key, joinWaitBudget(state.config()));
                        if (joined.status() == IdempotencyStore.AttemptStatus.COMPLETED
                                && joined.completed() != null) {
                            log.info("[{}] 幂等跟随完成 target={} key={} 判定依据=IDEMPOTENT_COMPLETED",
                                    requestId, target, key);
                            return joined.completed().withRequestId(requestId);
                        }
                        // LEADER 失败已释放占位：本次重复请求不再二次执行，返回可解释的失败
                        log.warn("[{}] 幂等执行方失败且占位已释放 target={} key={} 判定依据=DOWNSTREAM_FAILURE",
                                requestId, target, key);
                        return CallOutcome.failed(RejectReason.DOWNSTREAM_FAILURE, requestId,
                                target, 0, elapsed(start));
                    }
                    case LEADER -> {
                        CallOutcome outcome = runGuarded(state, ctx, start, 0, false);
                        if (outcome.status() == CallStatus.SUCCESS
                                || outcome.status() == CallStatus.FALLBACK) {
                            idempotencyStore.complete(target, key, outcome);
                        } else {
                            idempotencyStore.release(target, key);
                        }
                        return outcome;
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return CallOutcome.failed(RejectReason.DOWNSTREAM_FAILURE, requestId, target, 0, elapsed(start));
            }
        }
        return runGuarded(state, ctx, start, 0, false);
    }

    /** 带默认请求标识的便捷重载。 */
    public CallOutcome call(String target, String idempotencyKey, String payload) {
        return call(target, new CallContext("req-" + java.util.UUID.randomUUID(), idempotencyKey, payload));
    }

    /**
     * 在某个目标上执行“熔断前检 + 配额 + 超时重试 + 回退”完整治理。
     * @param depth 回退链深度
     * @param viaFallback 是否作为回退目标被调用
     */
    private CallOutcome runGuarded(TargetState state, CallContext ctx, long start,
                                   int depth, boolean viaFallback) {
        String target = state.target();
        String requestId = ctx.requestId();

        state.breaker().setTransitionListener((t, from, to, failures) ->
                log.info("[{}] 熔断状态迁移 target={} {} -> {} consecutiveFailures={} 判定依据=连续失败阈值",
                        requestId, t, from, to, failures));

        CallOutcome raw = runRetries(state, ctx, start);
        if (raw.status() == CallStatus.SUCCESS) {
            return raw;
        }
        return applyFallback(state, ctx, start, depth, raw, viaFallback);
    }

    /** 熔断前检 + 每尝试配额申请 + 超时 + 预算受限重试。 */
    private CallOutcome runRetries(TargetState state, CallContext ctx, long start) {
        String target = state.target();
        String requestId = ctx.requestId();
        TargetConfig cfg = state.config();

        // 入口熔断前检：OPEN 快速失败；等待期到达时这一调用原子成为 HALF_OPEN 的唯一探测。
        Optional<RejectReason> entryGate = state.breaker().beforeCall();
        if (entryGate.isPresent()) {
            log.warn("[{}] 快速失败 target={} 判定依据={} 熔断状态={}",
                    requestId, target, entryGate.get(), state.breaker().currentState());
            return CallOutcome.rejected(entryGate.get(), requestId, target, elapsed(start));
        }

        int attempts = 0;
        Exception lastError = null;
        while (attempts < cfg.maxAttempts()) {
            if (attempts > 0) {
                long remaining = cfg.budgetMillis() - (timeSource.nowMillis() - start);
                long backoff = Math.min(cfg.backoffMillis() * (1L << (attempts - 1)),
                        Math.max(remaining, 0));
                if (remaining <= 0 || remaining < backoff || remaining - backoff <= 0) {
                    log.warn("[{}] 重试预算耗尽 target={} attempts={} 已用={}ms 预算={}ms 判定依据=RETRY_BUDGET_EXHAUSTED 熔断状态={}",
                            requestId, target, attempts, timeSource.nowMillis() - start,
                            cfg.budgetMillis(), state.breaker().currentState());
                    return CallOutcome.failed(RejectReason.RETRY_BUDGET_EXHAUSTED, requestId,
                            target, attempts, elapsed(start));
                }
                log.info("[{}] 退避等待 target={} backoff={}ms attempt={} 熔断状态={}",
                        requestId, target, backoff, attempts + 1, state.breaker().currentState());
                try {
                    sleeper.sleep(backoff);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return CallOutcome.failed(RejectReason.DOWNSTREAM_FAILURE, requestId,
                            target, attempts, elapsed(start));
                }
                // 退避后仅被动观察：若熔断已被失败打开则停止重试；
                // 不在这里发起 OPEN->HALF_OPEN 迁移（唯一探测属于一次全新调用）
                Optional<RejectReason> gate = state.breaker().passiveCheck();
                if (gate.isPresent()) {
                    log.warn("[{}] 熔断打开，停止重试 target={} 判定依据={} 熔断状态={}",
                            requestId, target, gate.get(), state.breaker().currentState());
                    return CallOutcome.failed(RejectReason.DOWNSTREAM_FAILURE, requestId,
                            target, attempts, elapsed(start));
                }
            }

            // 每次真实尝试都申请配额与并发许可，用完即释放
            QuotaState.Acquire acquire = state.quota().tryAcquire();
            if (!acquire.granted()) {
                log.warn("[{}] 拒绝调用 target={} 判定依据={} attempt={} 熔断状态={}",
                        requestId, target, acquire.reason(), attempts + 1,
                        state.breaker().currentState());
                if (attempts == 0) {
                    return CallOutcome.rejected(acquire.reason(), requestId, target, elapsed(start));
                }
                return CallOutcome.failed(acquire.reason(), requestId, target, attempts, elapsed(start));
            }

            attempts++;
            Exception error;
            String value = null;
            try (QuotaState.QuotaTicket ignored = acquire.ticket()) {
                long remaining = cfg.budgetMillis() - (timeSource.nowMillis() - start);
                long waitMillis = Math.min(cfg.timeoutMillis(), Math.max(remaining, 1L));
                log.info("[{}] 发起下游调用 target={} attempt={} timeout={}ms 熔断状态={}",
                        requestId, target, attempts, waitMillis, state.breaker().currentState());
                value = invokeWithTimeout(target, ctx, waitMillis);
                error = null;
            } catch (Exception e) {
                error = e;
            }

            if (error == null) {
                state.breaker().recordSuccess();
                log.info("[{}] 调用成功 target={} attempts={} 熔断状态={}",
                        requestId, target, attempts, state.breaker().currentState());
                return CallOutcome.success(value, requestId, target, attempts, elapsed(start));
            }

            lastError = error;
            boolean retryable = error instanceof DownstreamTimeoutException
                    || error instanceof DownstreamFailureException;
            state.breaker().recordFailure();
            log.warn("[{}] 下游尝试失败 target={} attempt={} 判定依据={} retryable={} 熔断状态={}",
                    requestId, target, attempts, describe(error), retryable,
                    state.breaker().currentState());
            if (!retryable) {
                return CallOutcome.failed(RejectReason.DOWNSTREAM_FAILURE, requestId,
                        target, attempts, elapsed(start));
            }
            // 本次失败若已把熔断打开，则不再重试
            if (state.breaker().currentState() != com.github.lisama569.outboundcallgovernancejava.model.CircuitState.CLOSED) {
                log.warn("[{}] 熔断已打开，不再重试 target={} attempts={} 熔断状态={}",
                        requestId, target, attempts, state.breaker().currentState());
                return CallOutcome.failed(RejectReason.DOWNSTREAM_FAILURE, requestId,
                        target, attempts, elapsed(start));
            }
        }
        log.warn("[{}] 重试次数用尽 target={} attempts={} 判定依据=DOWNSTREAM_FAILURE 最后错误={}",
                requestId, target, attempts, describe(lastError));
        return CallOutcome.failed(RejectReason.DOWNSTREAM_FAILURE, requestId,
                target, attempts, elapsed(start));
    }

    /** 在独立线程执行下游调用，并以 get(timeout) 强制超时，超时后取消任务。 */
    private String invokeWithTimeout(String target, CallContext ctx, long timeoutMillis) throws Exception {
        Future<String> future = timeoutExecutor.submit(
                () -> invoker.invoke(target, ctx.requestId(), ctx.payload()));
        try {
            return future.get(timeoutMillis, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            future.cancel(true);
            throw new DownstreamTimeoutException(
                    "下游调用超时 target=" + target + " requestId=" + ctx.requestId()
                            + " timeoutMillis=" + timeoutMillis);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause() == null ? e : e.getCause();
            if (cause instanceof Exception ex) {
                throw ex;
            }
            throw new DownstreamFailureException(cause.toString());
        }
    }

    /** 失败或被拒绝后应用静态回退值或回退目标链。 */
    private CallOutcome applyFallback(TargetState state, CallContext ctx, long start,
                                      int depth, CallOutcome raw, boolean viaFallback) {
        String target = state.target();
        String requestId = ctx.requestId();
        TargetConfig cfg = state.config();

        if (cfg.fallbackValue() != null) {
            log.info("[{}] 使用静态回退值 target={} 原始判定={} 判定依据=FALLBACK_VALUE",
                    requestId, target, raw.reason());
            return CallOutcome.fallback(cfg.fallbackValue(), requestId, target,
                    raw.attempts(), elapsed(start));
        }
        String fbTarget = cfg.fallbackTarget();
        if (fbTarget == null || fbTarget.isBlank()) {
            if (viaFallback) {
                log.warn("[{}] 回退链终止且无可用回退 target={} 判定依据=FALLBACK_UNAVAILABLE",
                        requestId, target);
                return CallOutcome.failed(RejectReason.FALLBACK_UNAVAILABLE, requestId,
                        target, raw.attempts(), elapsed(start));
            }
            return raw;
        }
        if (depth >= MAX_FALLBACK_DEPTH) {
            log.warn("[{}] 回退链超过深度上限 target={} 判定依据=FALLBACK_UNAVAILABLE", requestId, target);
            return CallOutcome.failed(RejectReason.FALLBACK_UNAVAILABLE, requestId,
                    target, raw.attempts(), elapsed(start));
        }
        Optional<TargetState> fbOpt = registry.get(fbTarget);
        if (fbOpt.isEmpty()) {
            log.warn("[{}] 回退目标运行期缺失 target={} fallback={} 判定依据=FALLBACK_UNAVAILABLE",
                    requestId, target, fbTarget);
            return CallOutcome.failed(RejectReason.FALLBACK_UNAVAILABLE, requestId,
                    target, raw.attempts(), elapsed(start));
        }
        log.info("[{}] 降级到回退目标 from={} to={} 原始判定={} 判定依据=FALLBACK_TARGET",
                requestId, target, fbTarget, raw.reason());
        CallOutcome fbOutcome = runGuarded(fbOpt.get(), ctx, start, depth + 1, true);
        if (fbOutcome.status() == CallStatus.SUCCESS) {
            return CallOutcome.fallback(fbOutcome.value(), requestId, fbOutcome.target(),
                    fbOutcome.attempts(), elapsed(start));
        }
        if (fbOutcome.status() == CallStatus.FALLBACK) {
            return fbOutcome.withRequestId(requestId);
        }
        return fbOutcome;
    }

    private long elapsed(long start) {
        return timeSource.nowMillis() - start;
    }

    private static long joinWaitBudget(TargetConfig cfg) {
        return cfg.budgetMillis() + (long) cfg.timeoutMillis() * cfg.maxAttempts() + 2_000L;
    }

    private static String describe(Throwable t) {
        return t == null ? "null" : t.getClass().getSimpleName() + ":" + t.getMessage();
    }

    @Override
    public void close() {
        timeoutExecutor.shutdownNow();
    }
}
