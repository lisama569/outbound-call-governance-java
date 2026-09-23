package com.github.lisama569.outboundcallgovernancejava.support;

import com.github.lisama569.outboundcallgovernancejava.governance.GovernanceClient;
import com.github.lisama569.outboundcallgovernancejava.governance.GovernanceRegistry;
import com.github.lisama569.outboundcallgovernancejava.idempotency.IdempotencyStore;
import com.github.lisama569.outboundcallgovernancejava.model.TargetConfig;
import com.github.lisama569.outboundcallgovernancejava.simulation.SimulatedDownstream;

import java.util.LinkedHashMap;
import java.util.Map;

/** 测试装配小工具：虚拟时钟驱动治理判定；退避用空睡 Sleeper，避免把虚拟时间推过熔断等待点。 */
public final class TestKit {

    /** 退避立即返回，不推进虚拟时间（下游瞬时失败时退避耗时不应影响熔断等待判定）。 */
    private static final class NoOpSleeper implements com.github.lisama569.outboundcallgovernancejava.time.Sleeper {
        @Override
        public void sleep(long millis) {
        }
    }

    public final VirtualClock clock;
    public final SimulatedDownstream downstream;
    public final GovernanceRegistry registry;
    public final IdempotencyStore idempotency;
    public final GovernanceClient client;

    private TestKit(VirtualClock clock, SimulatedDownstream downstream, GovernanceRegistry registry,
                    IdempotencyStore idempotency, GovernanceClient client) {
        this.clock = clock;
        this.downstream = downstream;
        this.registry = registry;
        this.idempotency = idempotency;
        this.client = client;
    }

    private static TestKit assemble(VirtualClock clock,
                                    com.github.lisama569.outboundcallgovernancejava.time.Sleeper sleeper) {
        SimulatedDownstream downstream = new SimulatedDownstream(clock);
        GovernanceRegistry registry = new GovernanceRegistry(clock);
        IdempotencyStore idempotency = new IdempotencyStore(clock, 600_000);
        GovernanceClient client = new GovernanceClient(registry, idempotency, downstream, clock, sleeper);
        return new TestKit(clock, downstream, registry, idempotency, client);
    }

    /** 退避立即返回（空睡），熔断等待点只受显式 clock.advance 控制。 */
    public static TestKit virtual() {
        return assemble(new VirtualClock(), new NoOpSleeper());
    }

    /**
     * 退避会推进虚拟时钟：用于“重试预算随退避耗时耗尽”的确定性测试。
     * 注意：不要用于依赖精确熔断等待点的测试（退避会消耗虚拟时间）。
     */
    public static TestKit virtualWithBackoffTime() {
        VirtualClock clock = new VirtualClock();
        return assemble(clock, clock::sleep);
    }

    public void init(String target, TargetConfig config) {
        Map<String, TargetConfig> all = new LinkedHashMap<>();
        all.put(target, config);
        registry.initAll(all);
    }

    public void init(Map<String, TargetConfig> configs) {
        registry.initAll(configs);
    }
}
