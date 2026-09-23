package com.github.lisama569.outboundcallgovernancejava.tests.governance;

import com.github.lisama569.outboundcallgovernancejava.governance.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 配额临界、并发上限与运行中配置变更（含非法配置与回退冲突的可区分原因）。
 */
class QuotaAndConfigTest {

    private GovernanceConfigRegistry registry;
    private SimulatedDownstream downstream;
    private OutboundGateway gateway;

    @BeforeEach
    void setUp() {
        registry = new GovernanceConfigRegistry();
        downstream = new SimulatedDownstream();
        gateway = new OutboundGateway(registry, downstream);
    }

    private TargetConfig config(int quota, int concurrency) {
        return new TargetConfig(quota, 60_000, concurrency, 1_000, 1, 0, 5_000, 3, 5_000);
    }

    @Test
    void quotaBoundaryRejectsWithDistinguishableReason() {
        assertTrue(registry.update("t1", config(2, 5)).isEmpty());

        CallOutcome o1 = gateway.call("t1", "k1", "p");
        CallOutcome o2 = gateway.call("t1", "k2", "p");
        CallOutcome o3 = gateway.call("t1", "k3", "p");

        assertEquals(CallOutcome.Status.SUCCESS, o1.status());
        assertEquals(CallOutcome.Status.SUCCESS, o2.status());
        assertEquals(CallOutcome.Status.REJECTED, o3.status());
        assertEquals(RejectionReason.QUOTA_EXHAUSTED, o3.rejectionReason());
        assertEquals(2, downstream.invocationCount("t1"));
    }

    @Test
    void concurrencyLimitRejectsWithDistinguishableReason() throws Exception {
        assertTrue(registry.update("t2", config(100, 1)).isEmpty());
        downstream.setLatency("t2", 400);

        var first = java.util.concurrent.CompletableFuture.supplyAsync(
                () -> gateway.call("t2", "k1", "p"));
        Thread.sleep(100); // 等第一个调用占住并发许可
        CallOutcome second = gateway.call("t2", "k2", "p");

        assertEquals(CallOutcome.Status.REJECTED, second.status());
        assertEquals(RejectionReason.CONCURRENCY_SATURATED, second.rejectionReason());
        assertEquals(CallOutcome.Status.SUCCESS, first.get().status());
        assertEquals(1, downstream.invocationCount("t2"));
    }

    @Test
    void runtimeConfigChangeTakesEffectAndOnlyAffectsThatTarget() {
        assertTrue(registry.update("a", config(1, 5)).isEmpty());
        assertTrue(registry.update("b", config(1, 5)).isEmpty());

        gateway.call("a", "k1", "p");
        gateway.call("b", "k1", "p");
        // 两者配额均已耗尽
        assertEquals(RejectionReason.QUOTA_EXHAUSTED,
                gateway.call("a", "k2", "p").rejectionReason());
        assertEquals(RejectionReason.QUOTA_EXHAUSTED,
                gateway.call("b", "k2", "p").rejectionReason());

        // 运行中放宽 a 的配额：a 恢复可用，b 不受影响
        assertTrue(registry.update("a", config(5, 5)).isEmpty());
        assertEquals(CallOutcome.Status.SUCCESS, gateway.call("a", "k3", "p").status());
        CallOutcome bAgain = gateway.call("b", "k2", "p");
        assertEquals(CallOutcome.Status.REJECTED, bAgain.status());
        assertEquals(RejectionReason.QUOTA_EXHAUSTED, bAgain.rejectionReason());
    }

    @Test
    void invalidConfigRejectedWithInvalidConfigReason() {
        Optional<ConfigRejection> rejection =
                registry.update("t3", config(-1, 5));
        assertTrue(rejection.isPresent());
        assertEquals(ConfigRejection.Reason.INVALID_CONFIG, rejection.get().reason());

        Optional<ConfigRejection> rejection2 =
                registry.update("t3", new TargetConfig(10, 0, 5, 1_000, 1, 0, 5_000, 3, 5_000));
        assertTrue(rejection2.isPresent());
        assertEquals(ConfigRejection.Reason.INVALID_CONFIG, rejection2.get().reason());
    }

    @Test
    void rollbackBelowCurrentUsageRejectedWithDistinctReason() throws Exception {
        assertTrue(registry.update("t4", config(10, 2)).isEmpty());
        downstream.setLatency("t4", 500);

        // 占住 2 个并发许可（在途=2，窗口用量=2）
        var inFlight1 = java.util.concurrent.CompletableFuture.supplyAsync(
                () -> gateway.call("t4", "k1", "p"));
        var inFlight2 = java.util.concurrent.CompletableFuture.supplyAsync(
                () -> gateway.call("t4", "k2", "p"));
        Thread.sleep(100);

        // 回退并发到低于在途数（合法正数但冲突） -> ROLLBACK_CONFLICT（与非法配置可区分）
        Optional<ConfigRejection> rejection = registry.update("t4", config(10, 1));
        assertTrue(rejection.isPresent());
        assertEquals(ConfigRejection.Reason.ROLLBACK_CONFLICT, rejection.get().reason());

        // 回退配额到低于当前窗口用量 -> ROLLBACK_CONFLICT
        Optional<ConfigRejection> rejection2 = registry.update("t4", config(1, 2));
        assertTrue(rejection2.isPresent());
        assertEquals(ConfigRejection.Reason.ROLLBACK_CONFLICT, rejection2.get().reason());

        // 被拒绝后旧配置仍然有效
        assertEquals(10, registry.configOf("t4").quotaPerWindow());
        assertEquals(2, registry.configOf("t4").maxConcurrency());
        assertEquals(CallOutcome.Status.SUCCESS, inFlight1.get().status());
        assertEquals(CallOutcome.Status.SUCCESS, inFlight2.get().status());
    }
}
