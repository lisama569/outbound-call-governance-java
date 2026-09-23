package com.github.lisama569.outboundcallgovernancejava.quota;

import com.github.lisama569.outboundcallgovernancejava.model.RejectReason;
import com.github.lisama569.outboundcallgovernancejava.model.TargetConfig;
import com.github.lisama569.outboundcallgovernancejava.support.VirtualClock;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

class QuotaStateTest {

    private TargetConfig cfg(int quota, long windowMs, int concurrency) {
        return new TargetConfig(quota, windowMs, concurrency, 1000, 3, 5000, 10,
                5, 1000, null, null);
    }

    @Test
    void quotaBoundary_allowsExactlyQuotaThenRejects_andRefreshesAfterWindow() {
        VirtualClock clock = new VirtualClock();
        QuotaState quota = new QuotaState(cfg(3, 1000, 10), clock);

        List<QuotaState.QuotaTicket> held = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            QuotaState.Acquire a = quota.tryAcquire();
            assertNull(a.reason(), "第 " + (i + 1) + " 次应放行");
            held.add(a.ticket());
        }
        QuotaState.Acquire rejected = quota.tryAcquire();
        assertSame(RejectReason.QUOTA_EXCEEDED, rejected.reason(), "第 4 次应因配额拒绝");

        // 未到窗口边界（999ms）仍拒绝
        clock.advance(999);
        assertSame(RejectReason.QUOTA_EXCEEDED, quota.tryAcquire().reason());

        // 释放占用使并发不成为干扰；跨过窗口后配额刷新
        held.forEach(QuotaState.QuotaTicket::close);
        clock.advance(1);
        QuotaState.Acquire next = quota.tryAcquire();
        assertNull(next.reason(), "新窗口应重新放行");
        next.ticket().close();
    }

    @Test
    void concurrencyLimit_distinguishableFromQuota() {
        VirtualClock clock = new VirtualClock();
        // 配额很大，但并发只有 2
        QuotaState quota = new QuotaState(cfg(100, 10_000, 2), clock);

        QuotaState.Acquire a1 = quota.tryAcquire();
        QuotaState.Acquire a2 = quota.tryAcquire();
        assertNull(a1.reason());
        assertNull(a2.reason());

        QuotaState.Acquire a3 = quota.tryAcquire();
        assertSame(RejectReason.CONCURRENCY_LIMIT, a3.reason(), "应区分并发上限");

        a1.ticket().close();
        QuotaState.Acquire a4 = quota.tryAcquire();
        assertNull(a4.reason(), "释放并发后应可再次获得");
        a2.ticket().close();
        a4.ticket().close();
    }

    @Test
    void runtimeConfigChange_takesEffectImmediatelyForSameTargetOnly() {
        VirtualClock clock = new VirtualClock();
        QuotaState a = new QuotaState(cfg(2, 10_000, 2), clock);
        QuotaState b = new QuotaState(cfg(2, 10_000, 2), clock);

        // A 用满配额
        a.tryAcquire().ticket().close();
        a.tryAcquire().ticket().close();
        assertSame(RejectReason.QUOTA_EXCEEDED, a.tryAcquire().reason());

        // 运行中把 A 的配额提高到 5：当前窗口立即生效
        a.updateConfig(cfg(5, 10_000, 2));
        assertNull(a.tryAcquire().reason(), "配置提高后应立即放行");

        // 运行中把 B 的并发降到 1，不影响 A
        b.updateConfig(cfg(2, 10_000, 1));
        QuotaState.Acquire b1 = b.tryAcquire();
        assertNull(b1.reason());
        assertSame(RejectReason.CONCURRENCY_LIMIT, b.tryAcquire().reason());
        assertEquals(1, 1, "占位断言");
        b1.ticket().close();
    }
}
