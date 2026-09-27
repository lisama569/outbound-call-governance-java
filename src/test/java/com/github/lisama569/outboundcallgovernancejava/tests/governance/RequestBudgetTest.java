package com.github.lisama569.outboundcallgovernancejava.tests.governance;

import com.github.lisama569.outboundcallgovernancejava.governance.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 整次请求耗时预算的收口语义：
 * 主调用与各级降级调用共用同一份预算；单次调用超时不得越过剩余预算；
 * 预算不足时不再发起降级调用；并发请求各自守住预算。
 */
class RequestBudgetTest {

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
    void primaryCallTimeoutExceedingBudgetIsCappedByBudget() {
        // 单次超时 2000ms 远大于整次预算 300ms：调用必须在预算附近被截断，
        // 不得按单次超时的时长才返回
        assertTrue(registry.update("pb1", new TargetConfig(
                100, 60_000, 5, 2_000, 1, 0, 300, 10, 5_000)).isEmpty());
        downstream.setMode("pb1", SimulatedDownstream.Mode.SLOW);
        downstream.setLatency("pb1", 5_000);

        long start = System.currentTimeMillis();
        CallOutcome outcome = gateway.call("pb1", "k1", "p");
        long elapsed = System.currentTimeMillis() - start;

        assertEquals(CallOutcome.Status.FAILED, outcome.status());
        assertEquals(RejectionReason.BUDGET_EXHAUSTED, outcome.rejectionReason());
        assertEquals(1, outcome.attempts());
        assertTrue(elapsed < 1_000,
                "整次耗时应被预算截断而非等待单次超时 2000ms, elapsed=" + elapsed);
    }

    @Test
    void fastSuccessWithinBudgetIsNotAffectedByLargerCallTimeout() {
        // 单次超时大于预算，但下游在预算内快速返回：仍应正常成功
        assertTrue(registry.update("pb1b", new TargetConfig(
                100, 60_000, 5, 2_000, 1, 0, 300, 10, 5_000)).isEmpty());
        downstream.setLatency("pb1b", 100);

        CallOutcome outcome = gateway.call("pb1b", "k1", "p");

        assertEquals(CallOutcome.Status.SUCCESS, outcome.status());
        assertEquals("ok:pb1b:p", outcome.value());
    }

    @Test
    void fallbackAttemptTimeoutIsCappedBySharedRemainingBudget() {
        // 降级目标的单次超时（1200ms）大于发起重试时的剩余预算（约200ms）：
        // 该次尝试必须按剩余预算截断，整次请求不得越过 1500ms 预算
        assertTrue(registry.update("pb2-fb", new TargetConfig(
                100, 60_000, 5, 1_200, 5, 100, 30_000, 10, 5_000)).isEmpty());
        assertTrue(registry.update("pb2", new TargetConfig(
                100, 60_000, 5, 500, 1, 0, 1_500, 10, 5_000,
                FallbackConfig.target("pb2-fb"))).isEmpty());
        downstream.setMode("pb2", SimulatedDownstream.Mode.FAIL_FATAL);
        downstream.setMode("pb2-fb", SimulatedDownstream.Mode.SLOW);
        downstream.setLatency("pb2-fb", 10_000);

        long start = System.currentTimeMillis();
        CallOutcome outcome = gateway.call("pb2", "k1", "p");
        long elapsed = System.currentTimeMillis() - start;

        assertEquals(CallOutcome.Status.FAILED, outcome.status());
        assertEquals(RejectionReason.BUDGET_EXHAUSTED, outcome.rejectionReason());
        assertEquals(CallOutcome.FallbackKind.FALLBACK_TARGET, outcome.fallbackKind());
        // 时间线：attempt1 [0,1200] -> 退避100 -> attempt2 被预算截断为 200ms -> 预算耗尽
        assertEquals(2, downstream.invocationCount("pb2-fb"));
        assertTrue(elapsed < 2_000,
                "降级重试不得按单次超时跑满而越过共享预算, elapsed=" + elapsed);
    }

    @Test
    void fallbackSkippedWhenRemainingBudgetBelowFallbackCallTimeout() {
        // 主调用失败后剩余预算约 200ms，不足以完整跑一次降级调用（单次超时 500ms）：
        // 不再发起降级，就地收口为降级也失败
        assertTrue(registry.update("pb3-fb", new TargetConfig(
                100, 60_000, 5, 500, 1, 0, 30_000, 10, 5_000)).isEmpty());
        assertTrue(registry.update("pb3", new TargetConfig(
                100, 60_000, 5, 400, 1, 0, 600, 10, 5_000,
                FallbackConfig.target("pb3-fb"))).isEmpty());
        downstream.setMode("pb3", SimulatedDownstream.Mode.SLOW);
        downstream.setLatency("pb3", 5_000);

        long start = System.currentTimeMillis();
        CallOutcome outcome = gateway.call("pb3", "k1", "p");
        long elapsed = System.currentTimeMillis() - start;

        assertEquals(CallOutcome.Status.FAILED, outcome.status());
        assertEquals(CallOutcome.FallbackKind.FALLBACK_TARGET, outcome.fallbackKind());
        assertEquals(0, downstream.invocationCount("pb3-fb"),
                "剩余预算不足以完整跑一次降级调用时不得发起");
        assertTrue(elapsed < 1_000,
                "不发起降级调用，整次耗时不应越过预算, elapsed=" + elapsed);
    }

    @Test
    void fallbackSkippedWhenBudgetFullyConsumedByPrimary() {
        // 主调用把 400ms 预算全部耗尽：降级不再发起
        assertTrue(registry.update("pb4-fb", new TargetConfig(
                100, 60_000, 5, 100, 1, 0, 30_000, 10, 5_000)).isEmpty());
        assertTrue(registry.update("pb4", new TargetConfig(
                100, 60_000, 5, 300, 3, 100, 400, 10, 5_000,
                FallbackConfig.target("pb4-fb"))).isEmpty());
        downstream.setMode("pb4", SimulatedDownstream.Mode.SLOW);
        downstream.setLatency("pb4", 5_000);

        long start = System.currentTimeMillis();
        CallOutcome outcome = gateway.call("pb4", "k1", "p");
        long elapsed = System.currentTimeMillis() - start;

        assertEquals(CallOutcome.Status.FAILED, outcome.status());
        assertEquals(RejectionReason.BUDGET_EXHAUSTED, outcome.rejectionReason());
        assertEquals(CallOutcome.FallbackKind.FALLBACK_TARGET, outcome.fallbackKind());
        assertEquals(0, downstream.invocationCount("pb4-fb"),
                "预算耗尽后不得再发起降级调用");
        assertTrue(elapsed < 1_000, "整次耗时不应越过预算, elapsed=" + elapsed);
    }

    @Test
    void multiLevelFallbackSharesBudgetAndStopsWhenInsufficient() {
        // 三级链 a -> b -> c 共用 a 的 1500ms 预算并逐级递减：
        // a 花 600ms，b 花 500ms，剩余约 400ms 不足以跑 c（单次超时 500ms），c 不发起
        assertTrue(registry.update("pb5-c", new TargetConfig(
                100, 60_000, 5, 500, 1, 0, 30_000, 10, 5_000)).isEmpty());
        assertTrue(registry.update("pb5-b", new TargetConfig(
                100, 60_000, 5, 500, 1, 0, 30_000, 10, 5_000,
                FallbackConfig.target("pb5-c"))).isEmpty());
        assertTrue(registry.update("pb5-a", new TargetConfig(
                100, 60_000, 5, 600, 1, 0, 1_500, 10, 5_000,
                FallbackConfig.target("pb5-b"))).isEmpty());
        for (String t : List.of("pb5-a", "pb5-b", "pb5-c")) {
            downstream.setMode(t, SimulatedDownstream.Mode.SLOW);
            downstream.setLatency(t, 10_000);
        }

        long start = System.currentTimeMillis();
        CallOutcome outcome = gateway.call("pb5-a", "k1", "p");
        long elapsed = System.currentTimeMillis() - start;

        assertEquals(CallOutcome.Status.FAILED, outcome.status());
        assertEquals(CallOutcome.FallbackKind.FALLBACK_TARGET, outcome.fallbackKind());
        assertEquals(1, downstream.invocationCount("pb5-a"));
        assertEquals(1, downstream.invocationCount("pb5-b"));
        assertEquals(0, downstream.invocationCount("pb5-c"),
                "剩余预算不足时不得发起下一级降级");
        assertTrue(elapsed < 1_400,
                "多级降级共用预算，整次耗时不应越过 1500ms 预算, elapsed=" + elapsed);
    }

    @Test
    void concurrentRequestsEachStayWithinBudget() throws Exception {
        // 并发 8 个请求：各自预算 400ms、单次超时 2000ms，
        // 不允许任何一个个别请求超过预算
        assertTrue(registry.update("pb6", new TargetConfig(
                100, 60_000, 20, 2_000, 1, 0, 400, 10, 5_000)).isEmpty());
        downstream.setMode("pb6", SimulatedDownstream.Mode.SLOW);
        downstream.setLatency("pb6", 5_000);

        int parallelism = 8;
        ExecutorService pool = Executors.newFixedThreadPool(parallelism);
        CountDownLatch gate = new CountDownLatch(1);
        List<Future<long[]>> futures = new ArrayList<>();
        for (int i = 0; i < parallelism; i++) {
            String key = "ck-" + i;
            futures.add(pool.submit(() -> {
                gate.await();
                long start = System.currentTimeMillis();
                CallOutcome outcome = gateway.call("pb6", key, "p");
                long elapsed = System.currentTimeMillis() - start;
                return new long[]{elapsed, outcome.status() == CallOutcome.Status.FAILED
                        && outcome.rejectionReason() == RejectionReason.BUDGET_EXHAUSTED ? 1 : 0};
            }));
        }
        gate.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(30, TimeUnit.SECONDS));

        for (Future<long[]> f : futures) {
            long[] result = f.get();
            assertEquals(1, result[1], "并发下每个请求都应按预算耗尽收口");
            assertTrue(result[0] < 1_200,
                    "并发下个别请求不得越过预算, elapsed=" + result[0]);
        }
    }
}
