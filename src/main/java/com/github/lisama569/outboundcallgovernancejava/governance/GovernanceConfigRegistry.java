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
        TargetState state = stateOf(target);
        synchronized (state) {
            Optional<ConfigRejection> badFallback = validateFallback(target, config.fallback());
            if (badFallback.isPresent()) {
                return badFallback;
            }
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
     * 校验降级配置（原因 INVALID_FALLBACK，与普通非法配置可区分）：
     * 静态值与降级目标互斥；降级目标必须已存在；不得自引用；不得沿既有降级链成环。
     * 被拒绝时本目标与其他目标的配置均不受影响（校验先于任何状态修改）。
     */
    private Optional<ConfigRejection> validateFallback(String target, FallbackConfig fallback) {
        if (fallback == null || fallback.isNone()) {
            return Optional.empty();
        }
        if (fallback.staticValue() != null && fallback.fallbackTarget() != null) {
            return Optional.of(new ConfigRejection(ConfigRejection.Reason.INVALID_FALLBACK,
                    "staticValue and fallbackTarget are mutually exclusive"));
        }
        String next = fallback.fallbackTarget();
        if (next == null) {
            return Optional.empty(); // 纯静态兜底值，无链路约束
        }
        if (next.equals(target)) {
            return Optional.of(new ConfigRejection(ConfigRejection.Reason.INVALID_FALLBACK,
                    "fallback target must not be itself: " + next));
        }
        // 沿“新配置生效后”的降级链走查：next 必须存在，且链不得回到 target 或成环
        Set<String> visited = new HashSet<>();
        visited.add(target);
        while (next != null) {
            if (!visited.add(next)) {
                return Optional.of(new ConfigRejection(ConfigRejection.Reason.INVALID_FALLBACK,
                        "fallback chain forms a cycle at target: " + next));
            }
            TargetState nextState = states.get(next);
            if (nextState == null) {
                return Optional.of(new ConfigRejection(ConfigRejection.Reason.INVALID_FALLBACK,
                        "fallback target does not exist: " + next));
            }
            next = nextState.config.fallback().fallbackTarget();
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
