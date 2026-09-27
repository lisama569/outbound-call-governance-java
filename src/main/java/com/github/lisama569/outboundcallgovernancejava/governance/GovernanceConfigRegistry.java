package com.github.lisama569.outboundcallgovernancejava.governance;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 运行中可更新的按目标配置注册表。
 * 每个目标持有独立的配置、配额限制器与熔断器，
 * 配置变更只影响对应目标；非法配置与回退冲突配置被拒绝并给出可区分原因。
 */
public class GovernanceConfigRegistry {

    /** 单目标的治理运行态：配置 + 配额/并发限制器 + 熔断器。 */
    static final class TargetState {
        final QuotaLimiter limiter;
        final CircuitBreaker breaker;
        volatile TargetConfig config;

        TargetState(TargetConfig config) {
            this.config = config;
            this.limiter = new QuotaLimiter(config);
            this.breaker = new CircuitBreaker(config);
        }
    }

    private final ConcurrentHashMap<String, TargetState> states = new ConcurrentHashMap<>();

    /** 获取目标当前配置（未配置时返回默认配置）。 */
    public TargetConfig configOf(String target) {
        return stateOf(target).config;
    }

    /** 获取目标运行态（不存在则按默认配置创建）。 */
    TargetState stateOf(String target) {
        return states.computeIfAbsent(target, t -> new TargetState(TargetConfig.defaultConfig()));
    }

    /**
     * 更新目标配置，运行中生效且只影响该目标。
     *
     * @return 空表示接受，否则为拒绝原因（非法配置 / 回退冲突可区分）
     */
    public Optional<ConfigRejection> update(String target, TargetConfig config) {
        Optional<ConfigRejection> invalid = validate(config);
        if (invalid.isPresent()) {
            return invalid;
        }
        Optional<ConfigRejection> badFallback = validateFallback(target, config.fallback());
        if (badFallback.isPresent()) {
            return badFallback;
        }
        TargetState state = stateOf(target);
        synchronized (state) {
            // 回退冲突：新上限低于当前在途用量，拒绝并保持旧配置
            if (config.maxConcurrency() < state.limiter.currentInFlight()) {
                return Optional.of(new ConfigRejection(
                        ConfigRejection.Reason.ROLLBACK_CONFLICT,
                        "maxConcurrency " + config.maxConcurrency()
                                + " below current in-flight " + state.limiter.currentInFlight()));
            }
            if (config.quotaPerWindow() < state.limiter.currentWindowUsage()) {
                return Optional.of(new ConfigRejection(
                        ConfigRejection.Reason.ROLLBACK_CONFLICT,
                        "quotaPerWindow " + config.quotaPerWindow()
                                + " below current window usage " + state.limiter.currentWindowUsage()));
            }
            state.limiter.reconfigure(config);
            state.breaker.reconfigure(config);
            state.config = config;
            return Optional.empty();
        }
    }

    /**
     * 校验降级配置（拒绝原因 INVALID_FALLBACK，与普通非法配置可区分）：
     * 引用不存在的目标、指向自身、或沿现有降级链会形成环。
     * 校验失败时不触碰任何运行态，该目标原配置与其他目标配置均不受影响。
     */
    private Optional<ConfigRejection> validateFallback(String target, FallbackConfig fallback) {
        if (fallback == null || fallback.kind() != FallbackConfig.Kind.FALLBACK_TARGET) {
            return Optional.empty();
        }
        String fallbackTarget = fallback.fallbackTarget();
        if (fallbackTarget == null || fallbackTarget.isBlank()) {
            return Optional.of(new ConfigRejection(ConfigRejection.Reason.INVALID_FALLBACK,
                    "fallback target must not be blank"));
        }
        if (fallbackTarget.equals(target)) {
            return Optional.of(new ConfigRejection(ConfigRejection.Reason.INVALID_FALLBACK,
                    "fallback target must not be the target itself: " + target));
        }
        if (!states.containsKey(fallbackTarget)) {
            return Optional.of(new ConfigRejection(ConfigRejection.Reason.INVALID_FALLBACK,
                    "fallback target does not exist: " + fallbackTarget));
        }
        // 环检测：假设本次更新生效，从降级目标沿现有链走，若回到 target 则成环
        Set<String> visited = new HashSet<>();
        visited.add(target);
        String cursor = fallbackTarget;
        while (cursor != null) {
            if (!visited.add(cursor)) {
                return Optional.of(new ConfigRejection(ConfigRejection.Reason.INVALID_FALLBACK,
                        "fallback chain would form a cycle involving " + cursor));
            }
            TargetState cursorState = states.get(cursor);
            if (cursorState == null) {
                break;
            }
            FallbackConfig cursorFallback = cursorState.config.fallback();
            cursor = (cursorFallback != null
                    && cursorFallback.kind() == FallbackConfig.Kind.FALLBACK_TARGET)
                    ? cursorFallback.fallbackTarget() : null;
        }
        return Optional.empty();
    }

    private Optional<ConfigRejection> validate(TargetConfig c) {
        String bad = null;
        if (c == null) {
            bad = "config must not be null";
        } else if (c.quotaPerWindow() <= 0) {
            bad = "quotaPerWindow must be positive: " + c.quotaPerWindow();
        } else if (c.windowMillis() <= 0) {
            bad = "windowMillis must be positive: " + c.windowMillis();
        } else if (c.maxConcurrency() <= 0) {
            bad = "maxConcurrency must be positive: " + c.maxConcurrency();
        } else if (c.callTimeoutMillis() <= 0) {
            bad = "callTimeoutMillis must be positive: " + c.callTimeoutMillis();
        } else if (c.maxAttempts() <= 0) {
            bad = "maxAttempts must be positive: " + c.maxAttempts();
        } else if (c.backoffMillis() < 0) {
            bad = "backoffMillis must not be negative: " + c.backoffMillis();
        } else if (c.retryBudgetMillis() <= 0) {
            bad = "retryBudgetMillis must be positive: " + c.retryBudgetMillis();
        } else if (c.circuitFailureThreshold() <= 0) {
            bad = "circuitFailureThreshold must be positive: " + c.circuitFailureThreshold();
        } else if (c.circuitResetMillis() <= 0) {
            bad = "circuitResetMillis must be positive: " + c.circuitResetMillis();
        }
        return bad == null
                ? Optional.empty()
                : Optional.of(new ConfigRejection(ConfigRejection.Reason.INVALID_CONFIG, bad));
    }
}
