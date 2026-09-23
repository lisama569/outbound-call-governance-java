package com.github.lisama569.outboundcallgovernancejava.governance;

import com.github.lisama569.outboundcallgovernancejava.model.CallOutcome;
import com.github.lisama569.outboundcallgovernancejava.model.CallStatus;
import com.github.lisama569.outboundcallgovernancejava.model.CircuitState;
import com.github.lisama569.outboundcallgovernancejava.model.RejectReason;
import com.github.lisama569.outboundcallgovernancejava.model.TargetConfig;
import com.github.lisama569.outboundcallgovernancejava.simulation.SimulatedDownstream;
import com.github.lisama569.outboundcallgovernancejava.support.TestKit;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CircuitRecoveryFallbackTest {

    /** threshold=2 快速开熔断；openWait=1000；其余宽松。 */
    private TargetConfig cfg(String fallbackTarget, String fallbackValue) {
        return new TargetConfig(100, 100_000, 20, 5_000, 5, 100_000, 5,
                2, 1_000, fallbackTarget, fallbackValue);
    }

    @Test
    void failureOpensCircuit_fastFailsThenSingleProbeRecovers() {
        TestKit kit = TestKit.virtual();
        kit.init("svc", cfg(null, null));
        kit.downstream.setBehavior("svc", SimulatedDownstream.Behavior.fail());

        // 前两次真实尝试失败 -> 打开熔断（maxAttempts=5，但第二次失败即触发熔断，停止重试）
        CallOutcome first = kit.client.call("svc", null, "p");
        assertSame(CallStatus.FAILED, first.status());
        assertSame(CircuitState.OPEN,
                kit.registry.get("svc").orElseThrow().breaker().currentState());

        // 熔断打开：快速失败，不再产生下游调用
        int before = kit.downstream.invocationCount("svc");
        CallOutcome fastFail = kit.client.call("svc", null, "p");
        assertSame(CallStatus.REJECTED, fastFail.status());
        assertSame(RejectReason.CIRCUIT_OPEN, fastFail.reason());
        assertEquals(before, kit.downstream.invocationCount("svc"), "OPEN 期间不得发起调用");

        // 等待期未到
        kit.clock.advance(999);
        assertSame(RejectReason.CIRCUIT_OPEN, kit.client.call("svc", null, "p").reason());
        assertEquals(before, kit.downstream.invocationCount("svc"));

        // 等待期到达：下一次调用成为唯一探测（HALF_OPEN）
        kit.clock.advance(1);
        // 此时下游已恢复
        kit.downstream.setBehavior("svc", SimulatedDownstream.Behavior.ok());
        CallOutcome probe = kit.client.call("svc", null, "p");
        assertSame(CallStatus.SUCCESS, probe.status());
        assertSame(CircuitState.CLOSED,
                kit.registry.get("svc").orElseThrow().breaker().currentState());
        assertEquals(before + 1, kit.downstream.invocationCount("svc"),
                "恢复过程只允许一次探测，不得放大调用量");
    }

    @Test
    void failedProbeReopens_andDoesNotFlap() {
        TestKit kit = TestKit.virtual();
        kit.init("svc", cfg(null, null));
        kit.downstream.setBehavior("svc", SimulatedDownstream.Behavior.fail());
        kit.client.call("svc", null, "p"); // 打开熔断
        int callsWhenOpen = kit.downstream.invocationCount("svc");

        kit.clock.advance(1_000);
        // 仍是故障下游：探测失败，回到 OPEN
        CallOutcome probe = kit.client.call("svc", null, "p");
        assertSame(CallStatus.FAILED, probe.status());
        assertSame(CircuitState.OPEN,
                kit.registry.get("svc").orElseThrow().breaker().currentState());
        assertEquals(callsWhenOpen + 1, kit.downstream.invocationCount("svc"), "只放了一个探测");

        // 临界点：立即再来一次必须仍被快速失败
        CallOutcome again = kit.client.call("svc", null, "p");
        assertSame(RejectReason.CIRCUIT_OPEN, again.reason());
        assertEquals(callsWhenOpen + 1, kit.downstream.invocationCount("svc"),
                "探测失败后不得在临界点反复切换而放大调用");
    }

    @Test
    void staticFallbackValueUsedWhenCircuitOpen() {
        TestKit kit = TestKit.virtual();
        kit.init("svc", cfg(null, "STATIC-DEFAULT"));
        kit.downstream.setBehavior("svc", SimulatedDownstream.Behavior.fail());

        CallOutcome failed = kit.client.call("svc", null, "p");
        // 两次失败过程中无回退目标的静态值仅在最终失败时应用（熔断器在第二次失败后打开）
        assertSame(CallStatus.FALLBACK, failed.status(), "最终失败应降级到静态回退值");
        assertEquals("STATIC-DEFAULT", failed.value());
        assertTrue(failed.fallback());

        // 熔断打开后快速失败同样降级
        CallOutcome duringOpen = kit.client.call("svc", null, "p");
        assertSame(CallStatus.FALLBACK, duringOpen.status());
        assertEquals("STATIC-DEFAULT", duringOpen.value());
    }

    @Test
    void fallbackTargetProvidesResultAndIsGoverned() {
        TestKit kit = TestKit.virtual();
        kit.init(java.util.Map.of(
                "primary", cfg("backup", null),
                "backup", cfg(null, null)));
        kit.downstream.setBehavior("primary", SimulatedDownstream.Behavior.fail());
        kit.downstream.setBehavior("backup", SimulatedDownstream.Behavior.ok());

        CallOutcome outcome = kit.client.call("primary", null, "p");
        assertSame(CallStatus.FALLBACK, outcome.status());
        assertEquals("ok(backup)", outcome.value());
        assertEquals("backup", outcome.target());
        assertTrue(outcome.fallback());
        assertTrue(kit.downstream.invocationCount("backup") >= 1);
    }
}
