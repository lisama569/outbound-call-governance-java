package com.github.lisama569.outboundcallgovernancejava.tests.governance;

import com.github.lisama569.outboundcallgovernancejava.governance.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 熔断状态机：连续失败打开 -> 快速失败 -> 半开探测（可观察中间态）
 * -> 探测成功闭合 / 探测失败重新打开；探测不放大调用量。
 */
class CircuitBreakerTest {

    private static final TargetConfig CONFIG = new TargetConfig(
            100, 60_000, 5, 500, 1, 0, 5_000, 2, 300);

    private GovernanceConfigRegistry registry;
    private SimulatedDownstream downstream;
    private OutboundGateway gateway;
    private int keySeq;

    public CircuitBreakerTest() {
    }

    @BeforeEach
    void setUp() {
        registry = new GovernanceConfigRegistry();
        downstream = new SimulatedDownstream();
        gateway = new OutboundGateway(registry, downstream);
    }

    private String nextKey() {
        return "k" + (++keySeq);
    }

    private void openCircuit(String target) {
        assertTrue(registry.update(target, CONFIG).isEmpty());
        downstream.setMode(target, SimulatedDownstream.Mode.FAIL_RETRYABLE);
        assertEquals(CallOutcome.Status.FAILED, gateway.call(target, nextKey(), "p").status());
        assertEquals(CallOutcome.Status.FAILED, gateway.call(target, nextKey(), "p").status());
        assertEquals(CircuitState.OPEN, gateway.circuitStateOf(target));
    }

    @Test
    void consecutiveFailuresOpenCircuitThenFastFail() {
        openCircuit("c1");
        int before = downstream.invocationCount("c1");

        // 熔断打开后快速失败，不再发起下游调用
        CallOutcome rejected = gateway.call("c1", nextKey(), "p");
        assertEquals(CallOutcome.Status.REJECTED, rejected.status());
        assertEquals(RejectionReason.CIRCUIT_OPEN, rejected.rejectionReason());
        assertEquals(CircuitState.OPEN, rejected.circuitStateAfter());
        assertEquals(before, downstream.invocationCount("c1"));
    }

    @Test
    void halfOpenProbeSuccessClosesCircuit() throws Exception {
        openCircuit("c2");

        Thread.sleep(400); // 冷却期满，允许进入半开探测
        downstream.setMode("c2", SimulatedDownstream.Mode.HEALTHY);

        CallOutcome probe = gateway.call("c2", nextKey(), "p");
        assertEquals(CallOutcome.Status.SUCCESS, probe.status());
        assertEquals(CircuitState.CLOSED, gateway.circuitStateOf("c2"));

        // 闭合后正常调用
        assertEquals(CallOutcome.Status.SUCCESS,
                gateway.call("c2", nextKey(), "p").status());
    }

    @Test
    void halfOpenProbeFailureReopensAndDoesNotAmplify() throws Exception {
        openCircuit("c3");
        int afterOpen = downstream.invocationCount("c3"); // 2

        Thread.sleep(400); // 冷却期满
        // 保持故障：探测失败 -> 重新打开
        CallOutcome probe = gateway.call("c3", nextKey(), "p");
        assertEquals(CallOutcome.Status.FAILED, probe.status());
        assertEquals(CircuitState.OPEN, gateway.circuitStateOf("c3"));
        assertEquals(afterOpen + 1, downstream.invocationCount("c3")); // 仅探测一次

        // 重新打开后立即快速失败，不放大调用量
        CallOutcome rejected = gateway.call("c3", nextKey(), "p");
        assertEquals(RejectionReason.CIRCUIT_OPEN, rejected.rejectionReason());
        assertEquals(afterOpen + 1, downstream.invocationCount("c3"));
    }

    @Test
    void halfOpenAdmitsOnlyOneProbeWhileInFlight() throws Exception {
        openCircuit("c4");
        int afterOpen = downstream.invocationCount("c4"); // 2

        Thread.sleep(400); // 冷却期满
        // 探测挂起 300ms（低于超时 500ms，最终成功）
        downstream.setMode("c4", SimulatedDownstream.Mode.SLOW);
        downstream.setLatency("c4", 300);

        CompletableFuture<CallOutcome> probe =
                CompletableFuture.supplyAsync(() -> gateway.call("c4", nextKey(), "p"));
        Thread.sleep(100); // 确保探测已在途，状态为 HALF_OPEN

        // 探测进行中，其余请求一律快速失败，不放大调用量
        List<CallOutcome> duringProbe = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            duringProbe.add(gateway.call("c4", nextKey(), "p"));
        }
        for (CallOutcome o : duringProbe) {
            assertEquals(CallOutcome.Status.REJECTED, o.status());
            assertEquals(RejectionReason.CIRCUIT_OPEN, o.rejectionReason());
        }
        assertEquals(afterOpen + 1, downstream.invocationCount("c4"));

        // 探测成功 -> 闭合，后续调用恢复
        assertEquals(CallOutcome.Status.SUCCESS, probe.get().status());
        assertEquals(CircuitState.CLOSED, gateway.circuitStateOf("c4"));
        downstream.setMode("c4", SimulatedDownstream.Mode.HEALTHY);
        downstream.setLatency("c4", 0);
        assertEquals(CallOutcome.Status.SUCCESS,
                gateway.call("c4", nextKey(), "p").status());
    }
}
