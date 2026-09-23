package com.github.lisama569.outboundcallgovernancejava.idempotency;

import com.github.lisama569.outboundcallgovernancejava.model.CallOutcome;
import com.github.lisama569.outboundcallgovernancejava.model.CallStatus;
import com.github.lisama569.outboundcallgovernancejava.support.VirtualClock;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;

class IdempotencyStoreTest {

    private CallOutcome ok(String value) {
        return CallOutcome.success(value, "leader-req", "svc", 1, 5);
    }

    @Test
    void concurrentAcquires_singleLeader_othersSeeInFlightWithHolder() throws Exception {
        VirtualClock clock = new VirtualClock();
        IdempotencyStore store = new IdempotencyStore(clock, 10_000);

        IdempotencyStore.Attempt leader = store.acquire("svc", "k1", 0);
        assertSame(IdempotencyStore.AttemptStatus.LEADER, leader.status());

        IdempotencyStore.Attempt other = store.acquire("svc", "k1", 0);
        assertSame(IdempotencyStore.AttemptStatus.IN_FLIGHT, other.status(), "必须能区分执行中");
        assertEquals(Thread.currentThread().getName(), other.holder().orElse(null), "执行中应暴露持有者");

        IdempotencyStore.Attempt peek = store.peek("svc", "k1");
        assertSame(IdempotencyStore.AttemptStatus.IN_FLIGHT, peek.status());
    }

    @Test
    void followerWaitsAndGetsConsistentCompletedResult() throws Exception {
        VirtualClock clock = new VirtualClock();
        IdempotencyStore store = new IdempotencyStore(clock, 10_000);

        assertSame(IdempotencyStore.AttemptStatus.LEADER, store.acquire("svc", "k", 0).status());
        Object lock = new Object();
        AtomicInteger seen = new AtomicInteger();

        Thread follower = new Thread(() -> {
            try {
                IdempotencyStore.Attempt a = store.acquire("svc", "k", 5_000);
                if (a.status() == IdempotencyStore.AttemptStatus.COMPLETED
                        && "consistent".equals(a.completed().value())) {
                    seen.incrementAndGet();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }, "follower");
        follower.start();

        Thread.sleep(100); // 确保 follower 已在等待
        store.complete("svc", "k", ok("consistent"));
        follower.join(2_000);
        assertEquals(1, seen.get(), "跟随者必须等到一致的已完成结果");

        IdempotencyStore.Attempt later = store.acquire("svc", "k", 0);
        assertSame(IdempotencyStore.AttemptStatus.COMPLETED, later.status());
        assertNotNull(later.completed());
        assertEquals("consistent", later.completed().value());
        assertSame(CallStatus.SUCCESS, later.completed().status());
    }

    @Test
    void leaderFailureReleasesSlot_allowingLaterExecution() throws Exception {
        VirtualClock clock = new VirtualClock();
        IdempotencyStore store = new IdempotencyStore(clock, 10_000);

        assertSame(IdempotencyStore.AttemptStatus.LEADER, store.acquire("svc", "k", 0).status());
        store.release("svc", "k");

        IdempotencyStore.Attempt retry = store.acquire("svc", "k", 0);
        assertSame(IdempotencyStore.AttemptStatus.LEADER, retry.status(), "失败释放后应允许重新执行");
    }

    @Test
    void completedResultExpiresAfterTtl() throws Exception {
        VirtualClock clock = new VirtualClock();
        IdempotencyStore store = new IdempotencyStore(clock, 1_000);

        assertSame(IdempotencyStore.AttemptStatus.LEADER, store.acquire("svc", "k", 0).status());
        store.complete("svc", "k", ok("v"));
        assertSame(IdempotencyStore.AttemptStatus.COMPLETED, store.acquire("svc", "k", 0).status());

        clock.advance(1_000);
        IdempotencyStore.Attempt again = store.acquire("svc", "k", 0);
        assertSame(IdempotencyStore.AttemptStatus.LEADER, again.status(), "TTL 过后应可重新执行");
    }
}
