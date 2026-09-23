package com.github.lisama569.outboundcallgovernancejava.governance;

import com.github.lisama569.outboundcallgovernancejava.idempotency.IdempotencyStore;
import com.github.lisama569.outboundcallgovernancejava.model.CallOutcome;
import com.github.lisama569.outboundcallgovernancejava.model.CallStatus;
import com.github.lisama569.outboundcallgovernancejava.model.RejectReason;
import com.github.lisama569.outboundcallgovernancejava.model.TargetConfig;
import com.github.lisama569.outboundcallgovernancejava.simulation.SimulatedDownstream;
import com.github.lisama569.outboundcallgovernancejava.time.SystemTimeSource;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 超时在本地稳定复现：下游真实睡眠 300ms，治理层单次超时 80ms、总预算 200ms。
 * 不依赖任何外部服务或真实凭据。
 */
class TimeoutTest {

    @Test
    void slowDownstreamTimesOut_retriedThenBudgetStops() {
        SystemTimeSource time = new SystemTimeSource();
        SimulatedDownstream downstream = new SimulatedDownstream(time);
        GovernanceRegistry registry = new GovernanceRegistry(time);
        IdempotencyStore idem = new IdempotencyStore(time, 600_000);
        // 超时80ms；预算200ms；退避5ms；熔断阈值很高，排除熔断干扰
        TargetConfig cfg = new TargetConfig(100, 100_000, 10, 80, 10, 200, 5,
                100, 100_000, null, null);
        registry.initAll(java.util.Map.of("slow", cfg));
        try (GovernanceClient client = new GovernanceClient(registry, idem, downstream, time, time)) {
            downstream.setBehavior("slow", SimulatedDownstream.Behavior.slow(300));

            long t0 = System.currentTimeMillis();
            CallOutcome outcome = client.call("slow", null, "p");
            long realElapsed = System.currentTimeMillis() - t0;

            assertSame(CallStatus.FAILED, outcome.status());
            assertSame(RejectReason.RETRY_BUDGET_EXHAUSTED, outcome.reason(),
                    "超时重试受总预算约束，预算耗尽必须给出可区分原因");
            assertTrue(outcome.attempts() >= 2, "超时应至少重试一次，实际=" + outcome.attempts());
            assertEquals(outcome.attempts(), downstream.invocationCount("slow"));
            // 单次 80ms 超时生效：真实总时长远小于“300ms * 尝试次数”，且被 200ms 预算限制
            assertTrue(realElapsed < 500,
                    "超时与预算必须生效，实际真实耗时=" + realElapsed + "ms");
        }
    }
}
