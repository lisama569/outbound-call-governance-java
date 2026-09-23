package com.github.lisama569.outboundcallgovernancejava.governance;

import com.github.lisama569.outboundcallgovernancejava.model.ConfigUpdateResult;
import com.github.lisama569.outboundcallgovernancejava.model.RejectReason;
import com.github.lisama569.outboundcallgovernancejava.model.TargetConfig;
import com.github.lisama569.outboundcallgovernancejava.time.TimeSource;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 目标注册表：持有各目标的 {@link TargetState}，负责配置校验与运行中变更。
 *
 * <p>字段非法返回 {@link RejectReason#INVALID_CONFIG}；回退配置不合法（回退目标不存在
 * 或回退链成环）返回可区分的 {@link RejectReason#FALLBACK_CONFIG_REJECTED}。校验失败时
 * 保留旧配置、原子拒绝。单个目标的变更只影响该目标，未涉及目标完全不受影响。</p>
 */
public class GovernanceRegistry {

    private final ConcurrentHashMap<String, TargetState> targets = new ConcurrentHashMap<>();
    private final TimeSource timeSource;

    public GovernanceRegistry(TimeSource timeSource) {
        this.timeSource = timeSource;
    }

    /** 初始化或替换全部目标配置（启动时）；非法项抛出异常，调用方应保证启动配置合法。 */
    public void initAll(Map<String, TargetConfig> configs) {
        Map<String, TargetConfig> candidate = new LinkedHashMap<>(configs);
        for (Map.Entry<String, TargetConfig> e : candidate.entrySet()) {
            ConfigUpdateResult result = validate(e.getKey(), e.getValue(), candidate);
            if (!result.accepted()) {
                throw new IllegalArgumentException(
                        "非法启动配置 target=" + e.getKey() + " reason=" + result.reason()
                                + " detail=" + result.detail());
            }
        }
        ConcurrentHashMap<String, TargetState> rebuilt = new ConcurrentHashMap<>();
        for (Map.Entry<String, TargetConfig> e : candidate.entrySet()) {
            rebuilt.put(e.getKey(), new TargetState(e.getKey(), e.getValue(), timeSource));
        }
        targets.clear();
        targets.putAll(rebuilt);
    }

    /** 运行中更新单个目标：校验失败则保留旧配置并返回带原因的拒绝结果。 */
    public ConfigUpdateResult updateTarget(String target, TargetConfig config) {
        if (target == null || target.isBlank()) {
            return ConfigUpdateResult.reject(null, RejectReason.INVALID_CONFIG, "target 名称不能为空");
        }
        // 以“当前配置 + 待生效配置”为校验视图，使新目标也能被回退链引用
        Map<String, TargetConfig> view = currentConfigs();
        view.put(target, config);
        ConfigUpdateResult result = validate(target, config, view);
        if (!result.accepted()) {
            return result;
        }
        // compute 原子作用于单个目标；其余目标的状态不受影响
        targets.compute(target, (name, existing) ->
                existing == null
                        ? new TargetState(name, config, timeSource)
                        : mutate(existing, config));
        return ConfigUpdateResult.ok(target);
    }

    private static TargetState mutate(TargetState existing, TargetConfig config) {
        existing.applyConfig(config);
        return existing;
    }

    public Optional<TargetState> get(String target) {
        return Optional.ofNullable(targets.get(target));
    }

    public Map<String, TargetState> snapshot() {
        return Map.copyOf(targets);
    }

    private Map<String, TargetConfig> currentConfigs() {
        Map<String, TargetConfig> map = new HashMap<>();
        for (Map.Entry<String, TargetState> e : targets.entrySet()) {
            map.put(e.getKey(), e.getValue().config());
        }
        return map;
    }

    /**
     * 校验单个配置：先校验字段合法性（INVALID_CONFIG），再校验回退配置（FALLBACK_CONFIG_REJECTED）。
     */
    ConfigUpdateResult validate(String target, TargetConfig c, Map<String, TargetConfig> all) {
        if (c == null) {
            return ConfigUpdateResult.reject(target, RejectReason.INVALID_CONFIG, "config 不能为 null");
        }
        String fieldError = checkFields(c);
        if (fieldError != null) {
            return ConfigUpdateResult.reject(target, RejectReason.INVALID_CONFIG, fieldError);
        }
        String fallbackError = checkFallback(target, c, all);
        if (fallbackError != null) {
            return ConfigUpdateResult.reject(target, RejectReason.FALLBACK_CONFIG_REJECTED, fallbackError);
        }
        return ConfigUpdateResult.ok(target);
    }

    private static String checkFields(TargetConfig c) {
        if (c.quotaPerWindow() <= 0) {
            return "quotaPerWindow 必须 > 0";
        }
        if (c.windowMillis() <= 0) {
            return "windowMillis 必须 > 0";
        }
        if (c.maxConcurrency() <= 0) {
            return "maxConcurrency 必须 > 0";
        }
        if (c.timeoutMillis() <= 0) {
            return "timeoutMillis 必须 > 0";
        }
        if (c.maxAttempts() <= 0) {
            return "maxAttempts 必须 > 0";
        }
        if (c.budgetMillis() <= 0) {
            return "budgetMillis 必须 > 0";
        }
        if (c.backoffMillis() < 0) {
            return "backoffMillis 必须 >= 0";
        }
        if (c.failureThreshold() <= 0) {
            return "failureThreshold 必须 > 0";
        }
        if (c.openWaitMillis() <= 0) {
            return "openWaitMillis 必须 > 0";
        }
        return null;
    }

    private static String checkFallback(String target, TargetConfig c, Map<String, TargetConfig> all) {
        String fb = c.fallbackTarget();
        if (fb == null || fb.isBlank()) {
            return null;
        }
        if (fb.equals(target)) {
            return "回退目标不能指向自身: " + fb;
        }
        if (!all.containsKey(fb)) {
            return "回退目标不存在: " + fb;
        }
        // 沿回退链检测成环（静态回退值终止链条）
        Set<String> seen = new HashSet<>();
        seen.add(target);
        String cur = fb;
        while (cur != null) {
            if (!seen.add(cur)) {
                return "回退链存在环: " + String.join(" -> ", seen) + " -> " + cur;
            }
            TargetConfig next = all.get(cur);
            if (next == null || next.fallbackTarget() == null || next.fallbackTarget().isBlank()) {
                return null;
            }
            cur = next.fallbackTarget();
        }
        return null;
    }
}
