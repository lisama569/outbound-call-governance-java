package com.github.lisama569.outboundcallgovernancejava.governance;

import com.github.lisama569.outboundcallgovernancejava.circuit.CircuitBreaker;
import com.github.lisama569.outboundcallgovernancejava.model.TargetConfig;
import com.github.lisama569.outboundcallgovernancejava.quota.QuotaState;
import com.github.lisama569.outboundcallgovernancejava.time.TimeSource;

/**
 * 单个目标的全部治理运行时状态：配额 + 熔断器（幂等存储全局共享）。
 * 配置变更通过原子替换 config 引用并转发给各组件，只影响本目标。
 */
public final class TargetState {

    private final String target;
    private volatile TargetConfig config;
    private final QuotaState quota;
    private final CircuitBreaker breaker;

    public TargetState(String target, TargetConfig config, TimeSource timeSource) {
        this.target = target;
        this.config = config;
        this.quota = new QuotaState(config, timeSource);
        this.breaker = new CircuitBreaker(target, config, timeSource);
    }

    public String target() {
        return target;
    }

    public TargetConfig config() {
        return config;
    }

    public QuotaState quota() {
        return quota;
    }

    public CircuitBreaker breaker() {
        return breaker;
    }

    /** 原子地对单个目标应用新配置。 */
    public void applyConfig(TargetConfig newConfig) {
        this.config = newConfig;
        this.quota.updateConfig(newConfig);
        this.breaker.updateConfig(newConfig);
    }
}
