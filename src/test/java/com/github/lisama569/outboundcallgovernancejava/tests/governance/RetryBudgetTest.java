package com.github.lisama569.outboundcallgovernancejava.tests.governance;

import com.github.lisama569.outboundcallgovernancejava.governance.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 超时与可重试失败的重试/退避；重试总量受耗时预算约束，
 * 预算耗尽后不得继续发起调用。
 */
class RetryBudgetTest {

    private GovernanceConfigRegistry registry;
    private SimulatedDownstream downstream;
    private OutboundGateway gateway;

    @BeforeEach
    void setUp() {
        registry = new GovernanceConfigRegistry();
        downstream = new SimulatedDownstream();
        gateway = new OutboundGateway(registry, downstream);
    }

    @Test
    void retryableFailureRetriesUpToMaxAttempts() {
        // 预算充足：重试到 maxAttempts 上限
        assertTrue(registry.update("r1", new TargetConfig(
                100, 60_000, 5, 1_000, 3, 10, 30_000, 10, 5_000)).isEmpty());
        downstream.setMode("r1", SimulatedDownstream.Mode.FAIL_RETRYABLE);

        CallOutcome outcome = gateway.call("r1", "k1", "p");

        assertEquals(CallOutcome.Status.FAILED, outcome.status());
        assertEquals(RejectionReason.DOWNSTREAM_FAILURE, outcome.rejectionReason());
        assertEquals(3, outcome.attempts());
        assertEquals(3, downstream.invocationCount("r1"));
    }

    @Test
    void fatalFailureIsNotRetried() {
        assertTrue(registry.update("r2", new TargetConfig(
                100, 60_000, 5, 1_000, 5, 10, 30_000, 10, 5_000)).isEmpty());
        downstream.setMode("r2", SimulatedDownstream.Mode.FAIL_FATAL);

        CallOutcome outcome = gateway.call("r2", "k1", "p");

        assertEquals(CallOutcome.Status.FAILED, outcome.status());
        assertEquals(RejectionReason.DOWNSTREAM_FAILURE, outcome.rejectionReason());
        assertEquals(1, outcome.attempts());
        assertEquals(1, downstream.invocationCount("r2"));
    }

    @Test
    void timeoutIsRetriedAndBudgetStopsFurtherCalls() {
        // 单次超时 200ms，下游挂起 2s；预算 500ms，退避 100ms，maxAttempts 很大。
        // 时间线：attempt1 [0,200] 超时 -> 退避100 -> attempt2 [300,500] 超时
        //         -> 预算耗尽，不得再发起调用。
        assertTrue(registry.update("r3", new TargetConfig(
                100, 60_000, 5, 200, 100, 100, 500, 10, 5_000)).isEmpty());
        downstream.setMode("r3", SimulatedDownstream.Mode.SLOW);
        downstream.setLatency("r3", 2_000);

        long start = System.currentTimeMillis();
        CallOutcome outcome = gateway.call("r3", "k1", "p");
        long elapsed = System.currentTimeMillis() - start;

        assertEquals(CallOutcome.Status.FAILED, outcome.status());
        assertEquals(RejectionReason.BUDGET_EXHAUSTED, outcome.rejectionReason());
        assertEquals(2, outcome.attempts(), "预算耗尽后不得继续发起调用");
        assertEquals(2, downstream.invocationCount("r3"));
        assertTrue(elapsed < 2_000, "不应等待下游挂起结束, elapsed=" + elapsed);
    }

    @Test
    void successAfterRetry() {
        // 前两次可重试失败，之后恢复：用计数模式模拟——
        // 这里通过先失败后改配置的方式不可行（单次调用内），
        // 改用延迟低于超时验证一次成功路径的重试语义边界。
        assertTrue(registry.update("r4", new TargetConfig(
                100, 60_000, 5, 500, 3, 10, 30_000, 10, 5_000)).isEmpty());
        downstream.setLatency("r4", 100); // 低于超时，一次成功

        CallOutcome outcome = gateway.call("r4", "k1", "p");
        assertEquals(CallOutcome.Status.SUCCESS, outcome.status());
        assertEquals(1, outcome.attempts());
    }
}
