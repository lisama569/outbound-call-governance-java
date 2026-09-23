package com.github.lisama569.outboundcallgovernancejava.governance;

import com.github.lisama569.outboundcallgovernancejava.model.CallOutcome;
import com.github.lisama569.outboundcallgovernancejava.model.CallStatus;
import com.github.lisama569.outboundcallgovernancejava.model.RejectReason;
import com.github.lisama569.outboundcallgovernancejava.model.TargetConfig;
import com.github.lisama569.outboundcallgovernancejava.simulation.SimulatedDownstream;
import com.github.lisama569.outboundcallgovernancejava.support.TestKit;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

class RetryBudgetTest {

    /** 配额/并发充足、永不熔断的配置，便于专注重试验证。 */
    private TargetConfig cfg(int maxAttempts, long budgetMs, long backoffMs, int threshold) {
        return new TargetConfig(100, 100_000, 20, 500, maxAttempts, budgetMs, backoffMs,
                threshold, 100_000, null, null);
    }

    @Test
    void retriesRetryableFailuresWithBackoffAndThenSucceeds() {
        TestKit kit = TestKit.virtualWithBackoffTime();
        kit.init("svc", cfg(5, 100_000, 10, 100));
        kit.downstream.setBehavior("svc", SimulatedDownstream.Behavior.failFirstN(2));

        CallOutcome outcome = kit.client.call("svc", null, "p");
        assertSame(CallStatus.SUCCESS, outcome.status());
        assertEquals(3, outcome.attempts(), "前两次失败、第三次成功");
        assertEquals(3, kit.downstream.invocationCount("svc"));
    }

    @Test
    void attemptsStopOnceBudgetExhausted_andReasonDistinguishable() {
        TestKit kit = TestKit.virtualWithBackoffTime();
        // maxAttempts=10，但预算只有 100ms；退避 50ms -> 50,100，第三次退避（200）放不下
        kit.init("svc", cfg(10, 100, 50, 100));
        kit.downstream.setBehavior("svc", SimulatedDownstream.Behavior.fail());

        CallOutcome outcome = kit.client.call("svc", null, "p");
        assertSame(CallStatus.FAILED, outcome.status());
        assertSame(RejectReason.RETRY_BUDGET_EXHAUSTED, outcome.reason(),
                "预算耗尽必须给出与普通下游失败可区分的原因");
        assertEquals(2, outcome.attempts(), "预算只允许两次真实尝试（50+50 退避后不再发起第三次）");
        assertEquals(2, kit.downstream.invocationCount("svc"), "预算耗尽后不得继续发起调用");
    }

    @Test
    void whenMaxAttemptsReachedFirst_reasonIsDownstreamFailure() {
        TestKit kit = TestKit.virtualWithBackoffTime();
        // 预算充足，但只允许 2 次尝试
        kit.init("svc", cfg(2, 1_000_000, 1, 100));
        kit.downstream.setBehavior("svc", SimulatedDownstream.Behavior.fail());

        CallOutcome outcome = kit.client.call("svc", null, "p");
        assertSame(CallStatus.FAILED, outcome.status());
        assertSame(RejectReason.DOWNSTREAM_FAILURE, outcome.reason());
        assertEquals(2, outcome.attempts());
        assertEquals(2, kit.downstream.invocationCount("svc"));
    }
}
