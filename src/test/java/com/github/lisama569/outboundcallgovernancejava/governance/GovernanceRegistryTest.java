package com.github.lisama569.outboundcallgovernancejava.governance;

import com.github.lisama569.outboundcallgovernancejava.model.ConfigUpdateResult;
import com.github.lisama569.outboundcallgovernancejava.model.RejectReason;
import com.github.lisama569.outboundcallgovernancejava.model.TargetConfig;
import com.github.lisama569.outboundcallgovernancejava.support.VirtualClock;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GovernanceRegistryTest {

    private TargetConfig cfg(int quota, String fallbackTarget) {
        return new TargetConfig(quota, 10_000, 4, 500, 3, 5_000, 10,
                3, 1000, fallbackTarget, null);
    }

    private GovernanceRegistry registryWith(String a, TargetConfig ac) {
        VirtualClock clock = new VirtualClock();
        GovernanceRegistry registry = new GovernanceRegistry(clock);
        Map<String, TargetConfig> all = new LinkedHashMap<>();
        all.put(a, ac);
        registry.initAll(all);
        return registry;
    }

    @Test
    void invalidFields_rejectedWithInvalidConfig_andOldConfigPreserved() {
        GovernanceRegistry registry = registryWith("a", cfg(2, null));
        TargetConfig bad = new TargetConfig(0, 10_000, 4, 500, 3, 5_000, 10,
                3, 1000, null, null);

        ConfigUpdateResult result = registry.updateTarget("a", bad);
        assertFalse(result.accepted());
        assertSame(RejectReason.INVALID_CONFIG, result.reason());

        assertEquals(2, registry.get("a").orElseThrow().config().quotaPerWindow(),
                "非法配置必须保留旧配置");
    }

    @Test
    void fallbackToUnknownTarget_rejectedWithFallbackConfigReason() {
        GovernanceRegistry registry = registryWith("a", cfg(2, null));
        ConfigUpdateResult result = registry.updateTarget("a", cfg(2, "ghost"));
        assertFalse(result.accepted());
        assertSame(RejectReason.FALLBACK_CONFIG_REJECTED, result.reason(),
                "回退目标不存在必须给出可区分原因");
    }

    @Test
    void fallbackSelfAndCycle_rejected() {
        VirtualClock clock = new VirtualClock();
        GovernanceRegistry registry = new GovernanceRegistry(clock);
        Map<String, TargetConfig> all = new LinkedHashMap<>();
        all.put("a", cfg(2, null));
        all.put("b", cfg(2, null));
        registry.initAll(all);

        ConfigUpdateResult self = registry.updateTarget("a", cfg(2, "a"));
        assertSame(RejectReason.FALLBACK_CONFIG_REJECTED, self.reason());

        // 先合法地让 a -> b
        assertTrue(registry.updateTarget("a", cfg(2, "b")).accepted());
        // 再尝试 b -> a 形成环，必须被拒绝
        ConfigUpdateResult cycle = registry.updateTarget("b", cfg(2, "a"));
        assertFalse(cycle.accepted());
        assertSame(RejectReason.FALLBACK_CONFIG_REJECTED, cycle.reason());
        // b 的回退配置保持为空
        assertTrue(registry.get("b").orElseThrow().config().fallbackTarget() == null);
    }

    @Test
    void validChainAccepted_andUpdateOneTargetDoesNotAffectOthers() {
        VirtualClock clock = new VirtualClock();
        GovernanceRegistry registry = new GovernanceRegistry(clock);
        Map<String, TargetConfig> all = new LinkedHashMap<>();
        all.put("a", cfg(2, "b"));
        all.put("b", cfg(5, null));
        registry.initAll(all);

        assertTrue(registry.updateTarget("a", cfg(9, "b")).accepted());
        assertEquals(9, registry.get("a").orElseThrow().config().quotaPerWindow());
        assertEquals(5, registry.get("b").orElseThrow().config().quotaPerWindow(),
                "更新 a 不得影响 b");

        // 静态回退值的目标也可被引用
        TargetConfig withValue = new TargetConfig(1, 10_000, 1, 500, 1, 1_000, 0,
                1, 1000, null, "static-value");
        assertTrue(registry.updateTarget("c", withValue).accepted());
        assertTrue(registry.updateTarget("a", cfg(9, "c")).accepted());
    }

    @Test
    void unknownTargetCallRejectedDistinguishably() {
        GovernanceRegistry registry = registryWith("a", cfg(2, null));
        assertTrue(registry.get("missing").isEmpty());
    }
}
