package com.github.lisama569.outboundcallgovernancejava.tests.governance;

import com.github.lisama569.outboundcallgovernancejava.governance.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 降级链路：静态兜底值、降级目标（受自身治理约束与整请求预算约束）、
 * 非法/成环降级配置的可区分拒绝。
 */
class FallbackTest {

    private GovernanceConfigRegistry registry;
    private SimulatedDownstream downstream;
    private OutboundGateway gateway;

    @BeforeEach
    void setUp() {
        registry = new GovernanceConfigRegistry();
        downstream = new SimulatedDownstream();
        gateway = new OutboundGateway(registry, downstream);
    }

    private TargetConfig config(int quota, int concurrency, int maxAttempts,
                                long callTimeoutMillis, long retryBudgetMillis,
                                int circuitFailureThreshold, FallbackConfig fallback) {
        return new TargetConfig(quota, 60_000, concurrency, callTimeoutMillis, maxAttempts,
                0, retryBudgetMillis, circuitFailureThreshold, 5_000, fallback);
    }

    /** 快速失败的主目标配置：不可重试失败、单次尝试。 */
    private TargetConfig failingFast(FallbackConfig fallback) {
        return config(100, 5, 1, 1_000, 5_000, 3, fallback);
    }

    @Test
    void staticFallbackOnDownstreamFailureAfterRetriesExhausted() {
        assertTrue(registry.update("p1", config(100, 5, 2, 1_000, 5_000, 3,
                FallbackConfig.ofStaticValue("cached-value"))).isEmpty());
        downstream.setMode("p1", SimulatedDownstream.Mode.FAIL_RETRYABLE);

        CallOutcome outcome = gateway.call("p1", "k1", "p");

        assertEquals(CallOutcome.Status.SUCCESS, outcome.status());
        assertEquals(CallOutcome.FallbackKind.STATIC_VALUE, outcome.fallbackKind());
        assertEquals("cached-value", outcome.value());
        assertEquals(2, outcome.attempts()); // 重试耗尽后才降级
        assertEquals(2, downstream.invocationCount("p1"));
    }

    @Test
    void staticFallbackWhenCircuitOpen() {
        assertTrue(registry.update("p2", config(100, 5, 1, 1_000, 5_000, 2,
                FallbackConfig.ofStaticValue("degraded"))).isEmpty());
        downstream.setMode("p2", SimulatedDownstream.Mode.FAIL_FATAL);

        gateway.call("p2", "k1", "p");
        gateway.call("p2", "k2", "p");
        assertEquals(CircuitState.OPEN, gateway.circuitStateOf("p2"));

        // 熔断挡下（未发起下游调用）→ 自动转入静态兜底
        CallOutcome outcome = gateway.call("p2", "k3", "p");
        assertEquals(CallOutcome.Status.SUCCESS, outcome.status());
        assertEquals(CallOutcome.FallbackKind.STATIC_VALUE, outcome.fallbackKind());
        assertEquals("degraded", outcome.value());
        assertEquals(2, downstream.invocationCount("p2"));
    }

    @Test
    void fallbackTargetInvokedOnPrimaryFailureAndResultDistinguishable() {
        assertTrue(registry.update("fb1", config(100, 5, 1, 1_000, 5_000, 3,
                FallbackConfig.none())).isEmpty());
        assertTrue(registry.update("p3", failingFast(
                FallbackConfig.ofTarget("fb1"))).isEmpty());
        downstream.setMode("p3", SimulatedDownstream.Mode.FAIL_FATAL);

        CallOutcome outcome = gateway.call("p3", "k1", "p");

        assertEquals(CallOutcome.Status.SUCCESS, outcome.status());
        assertEquals(CallOutcome.FallbackKind.FALLBACK_TARGET, outcome.fallbackKind());
        assertEquals("ok:fb1:p", outcome.value());
        assertEquals(1, downstream.invocationCount("fb1"));

        // 真实成功与降级返回可一眼区分
        CallOutcome direct = gateway.call("fb1", "k2", "p");
        assertEquals(CallOutcome.Status.SUCCESS, direct.status());
        assertEquals(CallOutcome.FallbackKind.NONE, direct.fallbackKind());
    }

    @Test
    void chainedFallbackAcrossTargets() {
        assertTrue(registry.update("c", failingFast(FallbackConfig.none())).isEmpty());
        assertTrue(registry.update("b", failingFast(FallbackConfig.ofTarget("c"))).isEmpty());
        assertTrue(registry.update("a", failingFast(FallbackConfig.ofTarget("b"))).isEmpty());
        downstream.setMode("a", SimulatedDownstream.Mode.FAIL_FATAL);
        downstream.setMode("b", SimulatedDownstream.Mode.FAIL_FATAL);

        CallOutcome outcome = gateway.call("a", "k1", "p");

        assertEquals(CallOutcome.Status.SUCCESS, outcome.status());
        assertEquals(CallOutcome.FallbackKind.FALLBACK_TARGET, outcome.fallbackKind());
        assertEquals("ok:c:p", outcome.value());
        assertEquals(1, downstream.invocationCount("b"));
        assertEquals(1, downstream.invocationCount("c"));
    }

    @Test
    void fallbackTargetRespectsOwnQuota() {
        assertTrue(registry.update("fb2", config(1, 5, 1, 1_000, 5_000, 3,
                FallbackConfig.none())).isEmpty());
        assertTrue(registry.update("p4", failingFast(
                FallbackConfig.ofTarget("fb2"))).isEmpty());
        downstream.setMode("p4", SimulatedDownstream.Mode.FAIL_FATAL);

        // 第一次：降级目标配额内，降级成功
        CallOutcome first = gateway.call("p4", "k1", "p");
        assertEquals(CallOutcome.Status.SUCCESS, first.status());
        assertEquals(CallOutcome.FallbackKind.FALLBACK_TARGET, first.fallbackKind());

        // 第二次：降级目标自身配额耗尽 → 降级也失败，原因可区分
        CallOutcome second = gateway.call("p4", "k2", "p");
        assertEquals(CallOutcome.Status.REJECTED, second.status());
        assertEquals(RejectionReason.QUOTA_EXHAUSTED, second.rejectionReason());
        assertEquals(CallOutcome.FallbackKind.FALLBACK_TARGET, second.fallbackKind());
        assertEquals(1, downstream.invocationCount("fb2"));
    }

    @Test
    void fallbackTargetRespectsOwnConcurrency() throws Exception {
        assertTrue(registry.update("fb3", config(100, 1, 1, 1_000, 5_000, 3,
                FallbackConfig.none())).isEmpty());
        assertTrue(registry.update("p5", failingFast(
                FallbackConfig.ofTarget("fb3"))).isEmpty());
        downstream.setMode("p5", SimulatedDownstream.Mode.FAIL_FATAL);
        downstream.setLatency("fb3", 400);

        // 占住降级目标的唯一并发许可
        var occupying = CompletableFuture.supplyAsync(() -> gateway.call("fb3", "k0", "p"));
        Thread.sleep(100);

        CallOutcome outcome = gateway.call("p5", "k1", "p");
        assertEquals(CallOutcome.Status.REJECTED, outcome.status());
        assertEquals(RejectionReason.CONCURRENCY_SATURATED, outcome.rejectionReason());
        assertEquals(CallOutcome.FallbackKind.FALLBACK_TARGET, outcome.fallbackKind());

        assertEquals(CallOutcome.Status.SUCCESS, occupying.get().status());
        assertEquals(1, downstream.invocationCount("fb3"));
    }

    @Test
    void fallbackTargetRespectsOwnCircuit() {
        assertTrue(registry.update("fb4", config(100, 5, 1, 1_000, 5_000, 2,
                FallbackConfig.none())).isEmpty());
        assertTrue(registry.update("p6", failingFast(
                FallbackConfig.ofTarget("fb4"))).isEmpty());
        downstream.setMode("p6", SimulatedDownstream.Mode.FAIL_FATAL);
        downstream.setMode("fb4", SimulatedDownstream.Mode.FAIL_FATAL);

        // 直接把降级目标打到熔断打开
        gateway.call("fb4", "k1", "p");
        gateway.call("fb4", "k2", "p");
        assertEquals(CircuitState.OPEN, gateway.circuitStateOf("fb4"));

        CallOutcome outcome = gateway.call("p6", "k3", "p");
        assertEquals(CallOutcome.Status.REJECTED, outcome.status());
        assertEquals(RejectionReason.CIRCUIT_OPEN, outcome.rejectionReason());
        assertEquals(CallOutcome.FallbackKind.FALLBACK_TARGET, outcome.fallbackKind());
        assertEquals(2, downstream.invocationCount("fb4")); // 熔断挡下，未放大调用量
    }

    @Test
    void budgetExhaustedBeforeFallbackMeansFallbackNotAttempted() {
        assertTrue(registry.update("fb5", config(100, 5, 1, 1_000, 5_000, 3,
                FallbackConfig.none())).isEmpty());
        // 主目标：单次超时 250ms 即耗尽整请求预算，失败时剩余预算 <= 0
        assertTrue(registry.update("p7", config(100, 5, 1, 300, 250, 3,
                FallbackConfig.ofTarget("fb5"))).isEmpty());
        downstream.setMode("p7", SimulatedDownstream.Mode.SLOW);
        downstream.setLatency("p7", 2_000);

        CallOutcome outcome = gateway.call("p7", "k1", "p");

        assertEquals(CallOutcome.Status.FAILED, outcome.status());
        assertEquals(RejectionReason.BUDGET_EXHAUSTED, outcome.rejectionReason());
        assertEquals(CallOutcome.FallbackKind.FALLBACK_TARGET, outcome.fallbackKind());
        assertEquals(0, downstream.invocationCount("fb5")); // 预算不足，不再发起降级调用
    }

    @Test
    void invalidFallbackConfigsRejectedWithDistinctReason() {
        assertTrue(registry.update("base", failingFast(FallbackConfig.none())).isEmpty());

        // 引用不存在的目标
        Optional<ConfigRejection> ghost = registry.update("base",
                failingFast(FallbackConfig.ofTarget("ghost")));
        assertTrue(ghost.isPresent());
        assertEquals(ConfigRejection.Reason.INVALID_FALLBACK, ghost.get().reason());
        assertNotEquals(ConfigRejection.Reason.INVALID_CONFIG, ghost.get().reason());

        // 自引用
        Optional<ConfigRejection> self = registry.update("base",
                failingFast(FallbackConfig.ofTarget("base")));
        assertTrue(self.isPresent());
        assertEquals(ConfigRejection.Reason.INVALID_FALLBACK, self.get().reason());

        // 静态值与降级目标同时设置
        Optional<ConfigRejection> both = registry.update("base",
                failingFast(new FallbackConfig("v", "base")));
        assertTrue(both.isPresent());
        assertEquals(ConfigRejection.Reason.INVALID_FALLBACK, both.get().reason());

        // 全部被拒绝后原配置保持无降级
        assertTrue(registry.configOf("base").fallback().isNone());
    }

    @Test
    void cyclicFallbackConfigsRejectedAndExistingConfigsUntouched() {
        assertTrue(registry.update("nodeA", failingFast(FallbackConfig.none())).isEmpty());
        // B -> A 合法（A 存在且无降级链）
        assertTrue(registry.update("nodeB", failingFast(FallbackConfig.ofTarget("nodeA"))).isEmpty());

        // A -> B 将形成 A->B->A 环，必须被拒绝
        Optional<ConfigRejection> cycle = registry.update("nodeA",
                failingFast(FallbackConfig.ofTarget("nodeB")));
        assertTrue(cycle.isPresent());
        assertEquals(ConfigRejection.Reason.INVALID_FALLBACK, cycle.get().reason());

        // 被拒绝后：A 仍无降级，B 的既有降级配置不受影响
        assertTrue(registry.configOf("nodeA").fallback().isNone());
        assertEquals("nodeA", registry.configOf("nodeB").fallback().fallbackTarget());

        // 既有行为不变：B 失败时仍能降级到 A
        downstream.setMode("nodeB", SimulatedDownstream.Mode.FAIL_FATAL);
        CallOutcome outcome = gateway.call("nodeB", "k1", "p");
        assertEquals(CallOutcome.Status.SUCCESS, outcome.status());
        assertEquals(CallOutcome.FallbackKind.FALLBACK_TARGET, outcome.fallbackKind());
        assertEquals("ok:nodeA:p", outcome.value());
    }

    @Test
    void fallbackConfigUpdateTakesEffectAtRuntimeAndOnlyAffectsThatTarget() {
        assertTrue(registry.update("q1", failingFast(FallbackConfig.ofStaticValue("v1"))).isEmpty());
        assertTrue(registry.update("q2", failingFast(FallbackConfig.ofStaticValue("v2"))).isEmpty());
        downstream.setMode("q1", SimulatedDownstream.Mode.FAIL_FATAL);
        downstream.setMode("q2", SimulatedDownstream.Mode.FAIL_FATAL);

        assertEquals("v1", gateway.call("q1", "k1", "p").value());
        assertEquals("v2", gateway.call("q2", "k1", "p").value());

        // 运行中只更新 q1 的降级配置
        assertTrue(registry.update("q1", failingFast(FallbackConfig.ofStaticValue("v3"))).isEmpty());
        CallOutcome q1Again = gateway.call("q1", "k2", "p");
        assertEquals("v3", q1Again.value());
        assertEquals(CallOutcome.FallbackKind.STATIC_VALUE, q1Again.fallbackKind());
        // q2 不受影响
        assertEquals("v2", gateway.call("q2", "k2", "p").value());
    }
}
