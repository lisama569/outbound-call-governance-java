package com.github.lisama569.outboundcallgovernancejava.tests.governance;

import com.github.lisama569.outboundcallgovernancejava.governance.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 整次请求的耗时预算收口：主调用与各级降级调用共用同一份预算，
 * 单次等待被剩余预算截断；预算不足以再完整发起一次调用时不再发起。
 * 用例断言既看最终结果，也看实际墙钟耗时不得越过预算。
 */
class BudgetEnforcementTest {

    private static final long ELAPSED_TOLERANCE_MS = 400;

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

    private record TimedOutcome(CallOutcome outcome, long elapsedMillis) {
    }

    @Test
    void primaryCallTimeoutLargerThanBudgetIsCutByBudget() {
        // 单次超时 1200ms，但整次预算仅 300ms，下游挂起 3s。
        // 修复前：主调用按 1200ms 单次超时才返回，整次耗时是预算的好几倍。
        long budget = 300;
        assertTrue(registry.update("bp1", new TargetConfig(
                100, 60_000, 5, 1_200, 1, 0, budget, 10, 5_000)).isEmpty());
        downstream.setMode("bp1", SimulatedDownstream.Mode.SLOW);
        downstream.setLatency("bp1", 3_000);

        long start = System.currentTimeMillis();
        CallOutcome outcome = gateway.call("bp1", nextKey(), "p");
        long elapsed = System.currentTimeMillis() - start;

        assertEquals(CallOutcome.Status.FAILED, outcome.status());
        assertEquals(RejectionReason.BUDGET_EXHAUSTED, outcome.rejectionReason());
        assertEquals(1, outcome.attempts());
        assertEquals(1, downstream.invocationCount("bp1"));
        assertTrue(elapsed < budget + ELAPSED_TOLERANCE_MS,
                "整次请求不得越过预算, elapsed=" + elapsed + " budget=" + budget);
        assertTrue(elapsed < 1_200, "不应按单次超时 1200ms 才返回, elapsed=" + elapsed);
    }

    @Test
    void fallbackCallTimeoutLargerThanRemainingBudgetIsNotDispatched() {
        // 主目标：单次超时 200ms、预算 500ms，主调用以可重试失败快速结束（几乎不耗时），
        // 剩余预算约 500ms。降级目标单次超时 1000ms > 剩余预算：
        // 修复前会把降级调用发出去并按 1000ms 等待，整次耗时越过预算。
        assertTrue(registry.update("bf2-a", new TargetConfig(
                100, 60_000, 5, 1_000, 1, 0, 5_000, 10, 5_000)).isEmpty());
        long budget = 500;
        assertTrue(registry.update("bf2-b", new TargetConfig(
                100, 60_000, 5, 200, 1, 0, budget, 10, 5_000,
                FallbackConfig.target("bf2-a"))).isEmpty());
        downstream.setMode("bf2-b", SimulatedDownstream.Mode.FAIL_FATAL);
        downstream.setMode("bf2-a", SimulatedDownstream.Mode.SLOW);
        downstream.setLatency("bf2-a", 3_000);

        long start = System.currentTimeMillis();
        CallOutcome outcome = gateway.call("bf2-b", nextKey(), "p");
        long elapsed = System.currentTimeMillis() - start;

        assertNotEquals(CallOutcome.Status.SUCCESS, outcome.status());
        assertEquals(CallOutcome.FallbackKind.FALLBACK_TARGET, outcome.fallbackKind(),
                "结果应标记为走了降级路径但降级也失败");
        assertEquals(0, downstream.invocationCount("bf2-a"),
                "剩余预算不足以完整跑一次降级调用时不得发起降级");
        assertTrue(elapsed < budget + ELAPSED_TOLERANCE_MS,
                "整次请求不得越过预算, elapsed=" + elapsed + " budget=" + budget);
        assertTrue(elapsed < 1_000, "不应按降级目标单次超时 1000ms 等待, elapsed=" + elapsed);
    }

    @Test
    void fallbackSkippedWhenBudgetAlreadyDrainedByPrimary() {
        // 主目标：单次超时 200ms、预算 450ms，下游挂起 3s；
        // 第一次尝试在 200ms 超时（此时剩余 ~250ms，容得下一次完整 200ms 尝试，故重试），
        // 第二次尝试被剩余预算截断（~250ms）后超时收口，剩余预算已不足降级目标的
        // 单次超时（500ms），降级不得发起。
        assertTrue(registry.update("bf3-a", new TargetConfig(
                100, 60_000, 5, 500, 1, 0, 5_000, 10, 5_000)).isEmpty());
        long budget = 450;
        assertTrue(registry.update("bf3-b", new TargetConfig(
                100, 60_000, 5, 200, 10, 0, budget, 10, 5_000,
                FallbackConfig.target("bf3-a"))).isEmpty());
        downstream.setMode("bf3-b", SimulatedDownstream.Mode.SLOW);
        downstream.setLatency("bf3-b", 3_000);

        long start = System.currentTimeMillis();
        CallOutcome outcome = gateway.call("bf3-b", nextKey(), "p");
        long elapsed = System.currentTimeMillis() - start;

        assertEquals(CallOutcome.Status.FAILED, outcome.status());
        assertEquals(RejectionReason.BUDGET_EXHAUSTED, outcome.rejectionReason());
        assertEquals(CallOutcome.FallbackKind.FALLBACK_TARGET, outcome.fallbackKind());
        assertEquals(0, downstream.invocationCount("bf3-a"),
                "预算已被主调用花光，不得再把整趟降级调用跑完");
        assertTrue(elapsed < budget + ELAPSED_TOLERANCE_MS,
                "整次请求不得越过预算, elapsed=" + elapsed + " budget=" + budget);
    }

    @Test
    void chainedFallbacksShareOneBudgetAndAllFinishWithinIt() {
        // 链 bl4-a -> bl4-b -> bl4-c，三级单次超时均为 150ms，
        // 前两级快速失败，第三级健康。整份预算 500ms 被三级共享、逐级递减。
        long callTimeout = 150;
        long budget = 500;
        assertTrue(registry.update("bl4-c", new TargetConfig(
                100, 60_000, 5, callTimeout, 1, 0, budget, 10, 5_000,
                FallbackConfig.none())).isEmpty());
        assertTrue(registry.update("bl4-b", new TargetConfig(
                100, 60_000, 5, callTimeout, 1, 0, budget, 10, 5_000,
                FallbackConfig.target("bl4-c"))).isEmpty());
        assertTrue(registry.update("bl4-a", new TargetConfig(
                100, 60_000, 5, callTimeout, 1, 0, budget, 10, 5_000,
                FallbackConfig.target("bl4-b"))).isEmpty());
        downstream.setMode("bl4-a", SimulatedDownstream.Mode.FAIL_FATAL);
        downstream.setMode("bl4-b", SimulatedDownstream.Mode.FAIL_FATAL);

        long start = System.currentTimeMillis();
        CallOutcome outcome = gateway.call("bl4-a", nextKey(), "p");
        long elapsed = System.currentTimeMillis() - start;

        assertEquals(CallOutcome.Status.SUCCESS, outcome.status());
        assertEquals(CallOutcome.FallbackKind.FALLBACK_TARGET, outcome.fallbackKind());
        assertEquals("ok:bl4-c:p", outcome.value());
        assertEquals(1, downstream.invocationCount("bl4-a"));
        assertEquals(1, downstream.invocationCount("bl4-b"));
        assertEquals(1, downstream.invocationCount("bl4-c"));
        assertTrue(elapsed < budget + ELAPSED_TOLERANCE_MS,
                "多级降级共用一份预算, elapsed=" + elapsed + " budget=" + budget);
    }

    @Test
    void chainedFallbacksStopWhenRemainingBudgetCannotFitNextLevel() {
        // 链 bl5-a -> bl5-b -> bl5-c：
        // bl5-a 单次超时 300ms（预算 450ms），其挂起 300ms 后超时；
        // 剩余 ~150ms，bl5-b 单次超时仅 100ms 容得下 -> 发起并快速失败；
        // 此后剩余已不足 bl5-c 的单次超时 200ms -> 第二级降级不得发起，
        // 就地返回“降级也失败”。
        long budget = 450;
        assertTrue(registry.update("bl5-c", new TargetConfig(
                100, 60_000, 5, 200, 1, 0, budget, 10, 5_000)).isEmpty());
        assertTrue(registry.update("bl5-b", new TargetConfig(
                100, 60_000, 5, 100, 1, 0, budget, 10, 5_000,
                FallbackConfig.target("bl5-c"))).isEmpty());
        assertTrue(registry.update("bl5-a", new TargetConfig(
                100, 60_000, 5, 300, 1, 0, budget, 10, 5_000,
                FallbackConfig.target("bl5-b"))).isEmpty());
        downstream.setMode("bl5-a", SimulatedDownstream.Mode.SLOW);
        downstream.setLatency("bl5-a", 3_000);
        downstream.setMode("bl5-b", SimulatedDownstream.Mode.FAIL_FATAL);

        long start = System.currentTimeMillis();
        CallOutcome outcome = gateway.call("bl5-a", nextKey(), "p");
        long elapsed = System.currentTimeMillis() - start;

        assertNotEquals(CallOutcome.Status.SUCCESS, outcome.status());
        assertEquals(CallOutcome.FallbackKind.FALLBACK_TARGET, outcome.fallbackKind());
        assertEquals(1, downstream.invocationCount("bl5-a"));
        assertEquals(1, downstream.invocationCount("bl5-b"),
                "第一级降级剩余预算足够，应当发起");
        assertEquals(0, downstream.invocationCount("bl5-c"),
                "剩余预算不足以完整跑第二级降级，不得发起");
        assertTrue(elapsed < budget + ELAPSED_TOLERANCE_MS,
                "多级降级逐级递减，整次请求不得越过预算, elapsed=" + elapsed + " budget=" + budget);
    }

    @Test
    void concurrentRequestsEachRespectTheBudget() {
        // 12 个相互独立（不同幂等键）的并发请求；单次超时 1000ms 远大于预算 300ms，
        // 下游统一挂起 3s。并发调度下任何一个请求都不得越过自己的预算。
        long budget = 300;
        int threads = 12;
        assertTrue(registry.update("bc6", new TargetConfig(
                100, 60_000, threads, 1_000, 1, 0, budget, 10, 5_000)).isEmpty());
        downstream.setMode("bc6", SimulatedDownstream.Mode.SLOW);
        downstream.setLatency("bc6", 3_000);

        ExecutorService pool = Executors.newFixedThreadPool(threads);
        List<CompletableFuture<TimedOutcome>> slowResults = new ArrayList<>();
        for (int i = 0; i < threads; i++) {
            final String key = "ck-" + i;
            slowResults.add(CompletableFuture.supplyAsync(() -> {
                long s = System.currentTimeMillis();
                CallOutcome o = gateway.call("bc6", key, "p");
                return new TimedOutcome(o, System.currentTimeMillis() - s);
            }, pool));
        }
        int idx = 0;
        for (CompletableFuture<TimedOutcome> f : slowResults) {
            TimedOutcome r = f.join();
            assertEquals(CallOutcome.Status.FAILED, r.outcome().status());
            assertEquals(RejectionReason.BUDGET_EXHAUSTED, r.outcome().rejectionReason());
            assertTrue(r.elapsedMillis() < budget + ELAPSED_TOLERANCE_MS,
                    "并发下第 " + idx + " 个请求越过预算, elapsed="
                            + r.elapsedMillis() + " budget=" + budget);
            idx++;
        }
        pool.shutdown();

        assertEquals(threads, downstream.invocationCount("bc6"),
                "每个独立请求都应有且仅有一次实质调用");
        // 用另一组带墙钟计时的并发请求确认没有个别请求被拖长
        ExecutorService pool2 = Executors.newFixedThreadPool(threads);
        downstream.setMode("bc6", SimulatedDownstream.Mode.HEALTHY);
        List<CompletableFuture<Long>> healthy = new ArrayList<>();
        for (int i = 0; i < threads; i++) {
            final String key = "hk-" + i;
            healthy.add(CompletableFuture.supplyAsync(() -> {
                long s = System.currentTimeMillis();
                gateway.call("bc6", key, "p");
                return System.currentTimeMillis() - s;
            }, pool2));
        }
        for (CompletableFuture<Long> f : healthy) {
            long elapsed = f.join();
            assertTrue(elapsed < 1_000,
                    "并发下健康请求不应受其他请求影响而等待单次超时, elapsed=" + elapsed);
        }
        pool2.shutdown();
    }
}
