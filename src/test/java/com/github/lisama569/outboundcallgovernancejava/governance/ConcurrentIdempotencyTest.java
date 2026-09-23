package com.github.lisama569.outboundcallgovernancejava.governance;

import com.github.lisama569.outboundcallgovernancejava.idempotency.IdempotencyStore;
import com.github.lisama569.outboundcallgovernancejava.model.CallOutcome;
import com.github.lisama569.outboundcallgovernancejava.model.CallStatus;
import com.github.lisama569.outboundcallgovernancejava.model.TargetConfig;
import com.github.lisama569.outboundcallgovernancejava.time.SystemTimeSource;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ConcurrentIdempotencyTest {

    /** 可阻塞的下游：统计真实执行次数；第一次调用先在 gate 上等待，便于观察“执行中”。 */
    static final class BlockingInvoker implements DownstreamInvoker {
        final AtomicInteger executions = new AtomicInteger();
        final CountDownLatch entered = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);

        @Override
        public String invoke(String target, String requestId, String payload) throws Exception {
            int n = executions.incrementAndGet();
            entered.countDown();
            assertTrue(release.await(5, TimeUnit.SECONDS), "测试闸门必须被释放");
            return "result-" + target;
        }
    }

    @Test
    void concurrentDuplicateCallsProduceSingleExecutionAndConsistentResults() throws Exception {
        SystemTimeSource time = new SystemTimeSource();
        BlockingInvoker invoker = new BlockingInvoker();
        GovernanceRegistry registry = new GovernanceRegistry(time);
        IdempotencyStore idem = new IdempotencyStore(time, 600_000);
        TargetConfig cfg = new TargetConfig(100, 100_000, 20, 5_000, 1, 60_000, 5,
                100, 100_000, null, null);
        registry.initAll(java.util.Map.of("svc", cfg));

        int threads = 8;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        try (GovernanceClient client = new GovernanceClient(registry, idem, invoker, time, time)) {
            CountDownLatch ready = new CountDownLatch(threads);
            CountDownLatch start = new CountDownLatch(1);
            List<Future<CallOutcome>> futures = new ArrayList<>();

            for (int i = 0; i < threads; i++) {
                final String reqId = "req-dup-" + i;
                futures.add(pool.submit(() -> {
                    ready.countDown();
                    assertTrue(start.await(5, TimeUnit.SECONDS));
                    return client.call("svc", new CallContext(reqId, "IDEMP-KEY-1", "payload"));
                }));
            }

            assertTrue(ready.await(5, TimeUnit.SECONDS));
            start.countDown();

            // 等待唯一执行进入下游，此时幂等键应处于“执行中”
            assertTrue(invoker.entered.await(5, TimeUnit.SECONDS));
            IdempotencyStore.Attempt peek = idem.peek("svc", "IDEMP-KEY-1");
            assertSame(IdempotencyStore.AttemptStatus.IN_FLIGHT, peek.status(),
                    "并发期间必须能观察到执行中状态");
            assertTrue(peek.holder().isPresent(), "执行中必须暴露持有者线程以区分已完成");

            invoker.release.countDown();

            Set<String> values = new HashSet<>();
            Set<CallStatus> statuses = new HashSet<>();
            Set<String> requestIds = ConcurrentHashMap.newKeySet();
            for (int i = 0; i < threads; i++) {
                CallOutcome outcome = futures.get(i).get(10, TimeUnit.SECONDS);
                values.add(outcome.value());
                statuses.add(outcome.status());
                requestIds.add(outcome.requestId());
            }

            assertEquals(1, invoker.executions.get(), "同一幂等键只能产生一次实质执行");
            assertEquals(Set.of("result-svc"), values, "所有重复请求必须返回一致结果");
            assertEquals(Set.of(CallStatus.SUCCESS), statuses);
            assertEquals(threads, requestIds.size(), "每个请求仍保留自己的请求标识");
        } finally {
            pool.shutdownNow();
        }
    }
}
