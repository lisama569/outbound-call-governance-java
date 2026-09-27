package com.github.lisama569.outboundcallgovernancejava.tests.governance;

import com.github.lisama569.outboundcallgovernancejava.governance.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 降级链路：主目标失败/熔断时走静态兜底或降级目标；
 * 降级目标自身仍受配额/并发/熔断/预算约束；非法与成环的降级配置被拒绝。
 */
class FallbackTest {

    private GovernanceConfigRegistry registry;
    private SimulatedDownstream downstream;
    private OutboundGateway gateway;
    private int keySeq;

    @BeforeEach
    void setUp() {
        registry = new GovernanceConfigRegistry();
        downstream = new SimulatedDownstream();
        gateway = new OutboundGateway(registry, downstream);
    }

    private String nextKey() {
        return "k" + (++keySeq);
    }

    private TargetConfig configWithFallback(FallbackConfig fallback) {
        return new TargetConfig(100, 60_000, 5, 500, 1, 0, 5_000, 10, 5_000, fallback);
    }

    // ---------- 静态兜底 ----------

    @Test
    void staticFallbackOnDownstreamFailure() {
        assertTrue(registry.update("s1",
                configWithFallback(FallbackConfig.staticValue("cached-value"))).isEmpty());
        downstream.setMode("s1", SimulatedDownstream.Mode.FAIL_FATAL);

        CallOutcome outcome = gateway.call("s1", nextKey(), "p");

        assertEquals(CallOutcome.Status.SUCCESS, outcome.status());
        assertEquals(CallOutcome.FallbackKind.STATIC_VALUE, outcome.fallbackKind());
        assertEquals("cached-value", outcome.value());
        assertEquals(1, downstream.invocationCount("s1"));
    }

    @Test
    void staticFallbackWhenCircuitOpen() {
        TargetConfig config = new TargetConfig(100, 60_000, 5, 500, 1, 0, 5_000, 2, 60_000,
                FallbackConfig.staticValue("degraded"));
        assertTrue(registry.update("s2", config).isEmpty());
        downstream.setMode("s2", SimulatedDownstream.Mode.FAIL_RETRYABLE);
        gateway.call("s2", nextKey(), "p");
        gateway.call("s2", nextKey(), "p");
        assertEquals(CircuitState.OPEN, gateway.circuitStateOf("s2"));
        int before = downstream.invocationCount("s2");

        CallOutcome outcome = gateway.call("s2", nextKey(), "p");

        assertEquals(CallOutcome.Status.SUCCESS, outcome.status());
        assertEquals(CallOutcome.FallbackKind.STATIC_VALUE, outcome.fallbackKind());
        assertEquals("degraded", outcome.value());
        assertEquals(before, downstream.invocationCount("s2"), "熔断期间不应再发起下游调用");
    }

    @Test
    void realSuccessHasNoFallbackMarker() {
        assertTrue(registry.update("s3",
                configWithFallback(FallbackConfig.staticValue("unused"))).isEmpty());

        CallOutcome outcome = gateway.call("s3", nextKey(), "p");

        assertEquals(CallOutcome.Status.SUCCESS, outcome.status());
        assertEquals(CallOutcome.FallbackKind.NONE, outcome.fallbackKind());
    }

    // ---------- 降级目标 ----------

    @Test
    void fallbackTargetOnDownstreamFailure() {
        assertTrue(registry.update("fb-a", configWithFallback(FallbackConfig.none())).isEmpty());
        assertTrue(registry.update("fb-b",
                configWithFallback(FallbackConfig.target("fb-a"))).isEmpty());
        downstream.setMode("fb-b", SimulatedDownstream.Mode.FAIL_FATAL);

        CallOutcome outcome = gateway.call("fb-b", nextKey(), "p");

        assertEquals(CallOutcome.Status.SUCCESS, outcome.status());
        assertEquals(CallOutcome.FallbackKind.FALLBACK_TARGET, outcome.fallbackKind());
        assertEquals("ok:fb-a:p", outcome.value());
        assertEquals(1, downstream.invocationCount("fb-b"));
        assertEquals(1, downstream.invocationCount("fb-a"));
    }

    @Test
    void fallbackTargetWhenCircuitOpen() {
        assertTrue(registry.update("fc-a", configWithFallback(FallbackConfig.none())).isEmpty());
        TargetConfig primaryConfig = new TargetConfig(100, 60_000, 5, 500, 1, 0, 5_000, 2, 60_000,
                FallbackConfig.target("fc-a"));
        assertTrue(registry.update("fc-b", primaryConfig).isEmpty());
        downstream.setMode("fc-b", SimulatedDownstream.Mode.FAIL_RETRYABLE);
        gateway.call("fc-b", nextKey(), "p");
        gateway.call("fc-b", nextKey(), "p");
        assertEquals(CircuitState.OPEN, gateway.circuitStateOf("fc-b"));

        CallOutcome outcome = gateway.call("fc-b", nextKey(), "p");

        assertEquals(CallOutcome.Status.SUCCESS, outcome.status());
        assertEquals(CallOutcome.FallbackKind.FALLBACK_TARGET, outcome.fallbackKind());
        assertEquals("ok:fc-a:p", outcome.value());
    }

    @Test
    void chainedFallbackAcrossTwoLevels() {
        assertTrue(registry.update("ch-c", configWithFallback(FallbackConfig.none())).isEmpty());
        assertTrue(registry.update("ch-b",
                configWithFallback(FallbackConfig.target("ch-c"))).isEmpty());
        assertTrue(registry.update("ch-a",
                configWithFallback(FallbackConfig.target("ch-b"))).isEmpty());
        downstream.setMode("ch-a", SimulatedDownstream.Mode.FAIL_FATAL);
        downstream.setMode("ch-b", SimulatedDownstream.Mode.FAIL_FATAL);

        CallOutcome outcome = gateway.call("ch-a", nextKey(), "p");

        assertEquals(CallOutcome.Status.SUCCESS, outcome.status());
        assertEquals(CallOutcome.FallbackKind.FALLBACK_TARGET, outcome.fallbackKind());
        assertEquals("ok:ch-c:p", outcome.value());
        assertEquals(1, downstream.invocationCount("ch-c"));
    }

    // ---------- 降级目标自身受治理约束 ----------

    @Test
    void fallbackTargetSubjectToOwnQuota() {
        assertTrue(registry.update("fq-a", new TargetConfig(
                1, 60_000, 5, 500, 1, 0, 5_000, 10, 5_000)).isEmpty());
        assertTrue(registry.update("fq-b",
                configWithFallback(FallbackConfig.target("fq-a"))).isEmpty());
        downstream.setMode("fq-b", SimulatedDownstream.Mode.FAIL_FATAL);

        // 第一次降级成功，耗尽 fq-a 的配额
        CallOutcome first = gateway.call("fq-b", nextKey(), "p");
        assertEquals(CallOutcome.Status.SUCCESS, first.status());
        assertEquals(CallOutcome.FallbackKind.FALLBACK_TARGET, first.fallbackKind());

        // 第二次降级被 fq-a 自身配额挡下：降级也失败
        CallOutcome second = gateway.call("fq-b", nextKey(), "p");
        assertEquals(CallOutcome.Status.REJECTED, second.status());
        assertEquals(RejectionReason.QUOTA_EXHAUSTED, second.rejectionReason());
        assertEquals(CallOutcome.FallbackKind.FALLBACK_TARGET, second.fallbackKind());
        assertEquals(1, downstream.invocationCount("fq-a"));
    }

    @Test
    void fallbackTargetSubjectToOwnConcurrency() throws Exception {
        assertTrue(registry.update("fn-a", new TargetConfig(
                100, 60_000, 1, 1_000, 1, 0, 5_000, 10, 5_000)).isEmpty());
        assertTrue(registry.update("fn-b",
                configWithFallback(FallbackConfig.target("fn-a"))).isEmpty());
        downstream.setLatency("fn-a", 400);
        downstream.setMode("fn-b", SimulatedDownstream.Mode.FAIL_FATAL);

        // 占住 fn-a 唯一并发许可
        CompletableFuture<CallOutcome> holder =
                CompletableFuture.supplyAsync(() -> gateway.call("fn-a", nextKey(), "p"));
        Thread.sleep(100);

        CallOutcome outcome = gateway.call("fn-b", nextKey(), "p");
        assertEquals(CallOutcome.Status.REJECTED, outcome.status());
        assertEquals(RejectionReason.CONCURRENCY_SATURATED, outcome.rejectionReason());
        assertEquals(CallOutcome.FallbackKind.FALLBACK_TARGET, outcome.fallbackKind());
        assertEquals(CallOutcome.Status.SUCCESS, holder.get().status());
    }

    @Test
    void fallbackTargetSubjectToOwnCircuit() {
        TargetConfig fallbackConfig = new TargetConfig(
                100, 60_000, 5, 500, 1, 0, 5_000, 2, 60_000);
        assertTrue(registry.update("fr-a", fallbackConfig).isEmpty());
        assertTrue(registry.update("fr-b",
                configWithFallback(FallbackConfig.target("fr-a"))).isEmpty());
        // 先打爆 fr-a 的熔断器
        downstream.setMode("fr-a", SimulatedDownstream.Mode.FAIL_RETRYABLE);
        gateway.call("fr-a", nextKey(), "p");
        gateway.call("fr-a", nextKey(), "p");
        assertEquals(CircuitState.OPEN, gateway.circuitStateOf("fr-a"));
        downstream.setMode("fr-b", SimulatedDownstream.Mode.FAIL_FATAL);

        CallOutcome outcome = gateway.call("fr-b", nextKey(), "p");

        assertEquals(CallOutcome.Status.REJECTED, outcome.status());
        assertEquals(RejectionReason.CIRCUIT_OPEN, outcome.rejectionReason());
        assertEquals(CallOutcome.FallbackKind.FALLBACK_TARGET, outcome.fallbackKind());
    }

    @Test
    void fallbackTargetSubjectToOwnTimeout() {
        assertTrue(registry.update("ft-a", new TargetConfig(
                100, 60_000, 5, 200, 1, 0, 5_000, 10, 5_000)).isEmpty());
        assertTrue(registry.update("ft-b",
                configWithFallback(FallbackConfig.target("ft-a"))).isEmpty());
        downstream.setMode("ft-a", SimulatedDownstream.Mode.SLOW);
        downstream.setLatency("ft-a", 2_000);
        downstream.setMode("ft-b", SimulatedDownstream.Mode.FAIL_FATAL);

        long start = System.currentTimeMillis();
        CallOutcome outcome = gateway.call("ft-b", nextKey(), "p");
        long elapsed = System.currentTimeMillis() - start;

        assertEquals(CallOutcome.Status.FAILED, outcome.status());
        assertEquals(CallOutcome.FallbackKind.FALLBACK_TARGET, outcome.fallbackKind());
        assertTrue(elapsed < 2_000, "降级目标应受自身超时约束, elapsed=" + elapsed);
    }

    @Test
    void fallbackSkippedWhenRequestBudgetExhausted() {
        assertTrue(registry.update("fbud-a", configWithFallback(FallbackConfig.none())).isEmpty());
        // 主目标：超时 150ms，预算 200ms，下游挂起 2s —— 主调用把预算耗尽
        assertTrue(registry.update("fbud-b", new TargetConfig(
                100, 60_000, 5, 150, 5, 50, 200, 10, 5_000,
                FallbackConfig.target("fbud-a"))).isEmpty());
        downstream.setMode("fbud-b", SimulatedDownstream.Mode.SLOW);
        downstream.setLatency("fbud-b", 2_000);

        CallOutcome outcome = gateway.call("fbud-b", nextKey(), "p");

        assertEquals(CallOutcome.Status.FAILED, outcome.status());
        assertEquals(RejectionReason.BUDGET_EXHAUSTED, outcome.rejectionReason());
        assertEquals(CallOutcome.FallbackKind.FALLBACK_TARGET, outcome.fallbackKind());
        assertEquals(0, downstream.invocationCount("fbud-a"),
                "预算不足时不得再发起降级调用");
    }

    // ---------- 降级配置校验 ----------

    @Test
    void fallbackToUnknownTargetRejected() {
        Optional<ConfigRejection> rejection = registry.update("v1",
                configWithFallback(FallbackConfig.target("no-such-target")));

        assertTrue(rejection.isPresent());
        assertEquals(ConfigRejection.Reason.INVALID_FALLBACK, rejection.get().reason());
        // 被拒绝后该目标保持无降级配置
        assertEquals(FallbackConfig.Kind.NONE, registry.configOf("v1").fallback().kind());
    }

    @Test
    void fallbackToSelfRejected() {
        assertTrue(registry.update("v2", configWithFallback(FallbackConfig.none())).isEmpty());

        Optional<ConfigRejection> rejection = registry.update("v2",
                configWithFallback(FallbackConfig.target("v2")));

        assertTrue(rejection.isPresent());
        assertEquals(ConfigRejection.Reason.INVALID_FALLBACK, rejection.get().reason());
        assertEquals(FallbackConfig.Kind.NONE, registry.configOf("v2").fallback().kind());
    }

    @Test
    void fallbackCycleRejected() {
        assertTrue(registry.update("cy-a", configWithFallback(FallbackConfig.none())).isEmpty());
        assertTrue(registry.update("cy-b", configWithFallback(FallbackConfig.none())).isEmpty());
        assertTrue(registry.update("cy-c", configWithFallback(FallbackConfig.none())).isEmpty());
        // 合法链：a -> b -> c
        assertTrue(registry.update("cy-a",
                configWithFallback(FallbackConfig.target("cy-b"))).isEmpty());
        assertTrue(registry.update("cy-b",
                configWithFallback(FallbackConfig.target("cy-c"))).isEmpty());

        // c -> a 会成环，必须拒绝
        Optional<ConfigRejection> rejection = registry.update("cy-c",
                configWithFallback(FallbackConfig.target("cy-a")));
        assertTrue(rejection.isPresent());
        assertEquals(ConfigRejection.Reason.INVALID_FALLBACK, rejection.get().reason());

        // 两节点互指同样被拒绝
        Optional<ConfigRejection> directCycle = registry.update("cy-c",
                configWithFallback(FallbackConfig.target("cy-b")));
        assertTrue(directCycle.isPresent());
        assertEquals(ConfigRejection.Reason.INVALID_FALLBACK, directCycle.get().reason());

        // 被拒绝后原有链不受影响
        assertEquals(FallbackConfig.Kind.NONE, registry.configOf("cy-c").fallback().kind());
        assertEquals("cy-b", registry.configOf("cy-a").fallback().fallbackTarget());
        assertEquals("cy-c", registry.configOf("cy-b").fallback().fallbackTarget());
    }

    @Test
    void invalidFallbackReasonDistinctFromInvalidConfig() {
        Optional<ConfigRejection> badConfig = registry.update("d1", new TargetConfig(
                -1, 60_000, 5, 500, 1, 0, 5_000, 10, 5_000,
                FallbackConfig.staticValue("x")));
        assertTrue(badConfig.isPresent());
        assertEquals(ConfigRejection.Reason.INVALID_CONFIG, badConfig.get().reason());

        Optional<ConfigRejection> badFallback = registry.update("d1",
                configWithFallback(FallbackConfig.target("ghost")));
        assertTrue(badFallback.isPresent());
        assertEquals(ConfigRejection.Reason.INVALID_FALLBACK, badFallback.get().reason());
    }

    @Test
    void fallbackConfigUpdateAtRuntimeOnlyAffectsThatTarget() {
        assertTrue(registry.update("rt-a",
                configWithFallback(FallbackConfig.staticValue("a-v1"))).isEmpty());
        assertTrue(registry.update("rt-b",
                configWithFallback(FallbackConfig.staticValue("b-v1"))).isEmpty());
        downstream.setMode("rt-a", SimulatedDownstream.Mode.FAIL_FATAL);
        downstream.setMode("rt-b", SimulatedDownstream.Mode.FAIL_FATAL);

        assertEquals("a-v1", gateway.call("rt-a", nextKey(), "p").value());

        // 运行中只更新 rt-a 的兜底值
        assertTrue(registry.update("rt-a",
                configWithFallback(FallbackConfig.staticValue("a-v2"))).isEmpty());

        CallOutcome a = gateway.call("rt-a", nextKey(), "p");
        CallOutcome b = gateway.call("rt-b", nextKey(), "p");
        assertEquals("a-v2", a.value());
        assertEquals(CallOutcome.FallbackKind.STATIC_VALUE, a.fallbackKind());
        assertEquals("b-v1", b.value(), "其他目标的降级配置不应受影响");
    }
}
