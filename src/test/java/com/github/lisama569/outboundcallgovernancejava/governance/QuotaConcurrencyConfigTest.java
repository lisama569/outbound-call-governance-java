package com.github.lisama569.outboundcallgovernancejava.governance;

import com.github.lisama569.outboundcallgovernancejava.idempotency.IdempotencyStore;
import com.github.lisama569.outboundcallgovernancejava.model.CallOutcome;
import com.github.lisama569.outboundcallgovernancejava.model.CallStatus;
import com.github.lisama569.outboundcallgovernancejava.model.ConfigUpdateResult;
import com.github.lisama569.outboundcallgovernancejava.model.RejectReason;
import com.github.lisama569.outboundcallgovernancejava.model.TargetConfig;
import com.github.lisama569.outboundcallgovernancejava.simulation.SimulatedDownstream;
import com.github.lisama569.outboundcallgovernancejava.support.TestKit;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class QuotaConcurrencyConfigTest {

    private TargetConfig cfg(int quota, int concurrency) {
        return new TargetConfig(quota, 1_000_000, concurrency, 5_000, 1, 60_000, 0,
                100, 1_000_000, null, null);
    }

    @Test
    void concurrentCallsRespectQuotaBoundary_realInvocationsNeverExceedQuota() throws Exception {
        TestKit kit = TestKit.virtual();
        kit.init("svc", cfg(5, 20));
        kit.downstream.setBehavior("svc", SimulatedDownstream.Behavior.ok());

        int threads = 20;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<CallOutcome>> futures = new ArrayList<>();
        try {
            for (int i = 0; i < threads; i++) {
                futures.add(pool.submit(() -> {
                    start.await();
                    return kit.client.call("svc", null, "p");
                }));
            }
            start.countDown();
            int success = 0;
            int quotaRejected = 0;
            int concurrencyRejected = 0;
            for (Future<CallOutcome> f : futures) {
                CallOutcome o = f.get(5, TimeUnit.SECONDS);
                if (o.status() == CallStatus.SUCCESS) {
                    success++;
                } else if (o.reason() == RejectReason.QUOTA_EXCEEDED) {
                    quotaRejected++;
                } else if (o.reason() == RejectReason.CONCURRENCY_LIMIT) {
                    concurrencyRejected++;
                }
            }
            assertEquals(5, success, "恰好配额数量成功");
            assertEquals(5, kit.downstream.invocationCount("svc"), "真实调用绝不能超过配额");
            assertEquals(15, quotaRejected + concurrencyRejected, "其余请求必须被拒绝");
            assertTrue(quotaRejected + concurrencyRejected > 0);
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void runtimeQuotaIncreaseAdmitsPreviouslyRejectedTarget_onlyThatTarget() {
        TestKit kit = TestKit.virtual();
        kit.init(java.util.Map.of(
                "a", cfg(1, 10),
                "b", cfg(1, 10)));
        kit.downstream.setBehavior("a", SimulatedDownstream.Behavior.ok());
        kit.downstream.setBehavior("b", SimulatedDownstream.Behavior.ok());

        // 各用掉 1 次配额
        assertSame(CallStatus.SUCCESS, kit.client.call("a", null, "p").status());
        assertSame(CallStatus.SUCCESS, kit.client.call("b", null, "p").status());

        assertSame(RejectReason.QUOTA_EXCEEDED,
                kit.client.call("a", null, "p").reason());
        assertSame(RejectReason.QUOTA_EXCEEDED,
                kit.client.call("b", null, "p").reason());

        // 运行中只提高 a 的配额到 3
        ConfigUpdateResult result = kit.registry.updateTarget("a", cfg(3, 10));
        assertTrue(result.accepted());

        assertSame(CallStatus.SUCCESS, kit.client.call("a", null, "p").status(),
                "配置变更后 a 应立即被放行");
        assertSame(RejectReason.QUOTA_EXCEEDED,
                kit.client.call("b", null, "p").reason(), "未涉及目标 b 必须不受影响");
    }

    @Test
    void runtimeBadConfigChangeIsRejectedAndKeepsOldLimit() {
        TestKit kit = TestKit.virtual();
        kit.init("a", cfg(2, 10));
        TargetConfig bad = new TargetConfig(0, 1_000_000, 10, 5_000, 1, 60_000, 0,
                100, 1_000_000, null, null);
        ConfigUpdateResult result = kit.registry.updateTarget("a", bad);
        assertSame(RejectReason.INVALID_CONFIG, result.reason());

        kit.downstream.setBehavior("a", SimulatedDownstream.Behavior.ok());
        assertSame(CallStatus.SUCCESS, kit.client.call("a", null, "p").status());
        assertSame(CallStatus.SUCCESS, kit.client.call("a", null, "p").status());
        assertSame(RejectReason.QUOTA_EXCEEDED, kit.client.call("a", null, "p").reason(),
                "非法变更被拒绝后旧配额仍然生效");
    }
}
