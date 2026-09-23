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

import static org.junit.jupiter.api.Assertions.*;

/**
 * 同一幂等键的并发/重复请求只产生一次实质执行，
 * 重复请求返回一致结果，且能区分“执行中挂接”与“已完成回放”。
 */
class IdempotencyConcurrencyTest {

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
    void concurrentDuplicatesProduceSingleExecutionAndConsistentResult() throws Exception {
        downstream.setLatency("idem", 300); // 保证并发请求到达时首次执行仍在进行
        int threads = 8;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch ready = new CountDownLatch(threads);
        CountDownLatch go = new CountDownLatch(1);
        List<Future<CallOutcome>> futures = new ArrayList<>();
        for (int i = 0; i < threads; i++) {
            futures.add(pool.submit(() -> {
                ready.countDown();
                go.await();
                return gateway.call("idem", "same-key", "p");
            }));
        }
        ready.await();
        go.countDown();

        List<CallOutcome> outcomes = new ArrayList<>();
        for (Future<CallOutcome> f : futures) {
            outcomes.add(f.get());
        }
        pool.shutdown();

        // 只产生一次实质执行
        assertEquals(1, downstream.invocationCount("idem"));

        // 全部成功且结果一致（同一 requestId、同一返回值）
        String requestId = outcomes.get(0).requestId();
        String value = outcomes.get(0).value();
        for (CallOutcome o : outcomes) {
            assertEquals(CallOutcome.Status.SUCCESS, o.status());
            assertEquals(requestId, o.requestId());
            assertEquals(value, o.value());
        }

        // 恰好一个实质执行，其余为执行中挂接
        assertEquals(1, outcomes.stream()
                .filter(o -> o.idempotencyStatus() == IdempotencyStatus.FRESH_EXECUTION).count());
        assertEquals(threads - 1, outcomes.stream()
                .filter(o -> o.idempotencyStatus() == IdempotencyStatus.JOINED_IN_PROGRESS).count());

        // 完成后再来重复请求 -> 已完成回放，结果仍一致，且不触发新执行
        CallOutcome replay = gateway.call("idem", "same-key", "p");
        assertEquals(IdempotencyStatus.REPLAYED_AFTER_COMPLETION, replay.idempotencyStatus());
        assertEquals(requestId, replay.requestId());
        assertEquals(value, replay.value());
        assertEquals(1, downstream.invocationCount("idem"));
    }

    @Test
    void differentKeysExecuteIndependently() {
        CallOutcome o1 = gateway.call("idem2", "key-a", "p");
        CallOutcome o2 = gateway.call("idem2", "key-b", "p");
        assertEquals(IdempotencyStatus.FRESH_EXECUTION, o1.idempotencyStatus());
        assertEquals(IdempotencyStatus.FRESH_EXECUTION, o2.idempotencyStatus());
        assertEquals(2, downstream.invocationCount("idem2"));
    }
}
