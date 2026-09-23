package com.github.lisama569.outboundcallgovernancejava.circuit;

import com.github.lisama569.outboundcallgovernancejava.model.CircuitState;
import com.github.lisama569.outboundcallgovernancejava.model.RejectReason;
import com.github.lisama569.outboundcallgovernancejava.model.TargetConfig;
import com.github.lisama569.outboundcallgovernancejava.support.VirtualClock;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CircuitBreakerTest {

    private TargetConfig cfg(int threshold, long openWaitMs) {
        return new TargetConfig(100, 10_000, 10, 1000, 3, 10_000, 10,
                threshold, openWaitMs, null, null);
    }

    @Test
    void opensAfterConsecutiveFailuresAndFastFails() {
        VirtualClock clock = new VirtualClock();
        CircuitBreaker cb = new CircuitBreaker("svc", cfg(3, 1000), clock);

        assertTrue(cb.beforeCall().isEmpty());
        cb.recordFailure();
        assertTrue(cb.beforeCall().isEmpty(), "未达阈值应保持 CLOSED");
        cb.recordFailure();
        cb.recordFailure();
        assertSame(CircuitState.OPEN, cb.currentState());

        assertTrue(cb.beforeCall().isPresent(), "OPEN 必须快速失败");
        assertSame(RejectReason.CIRCUIT_OPEN, cb.beforeCall().get());
    }

    @Test
    void openToHalfOpenObservable_singleProbeOnly_probeSuccessCloses() {
        VirtualClock clock = new VirtualClock();
        List<String> transitions = new ArrayList<>();
        CircuitBreaker cb = new CircuitBreaker("svc", cfg(1, 500), clock);
        cb.setTransitionListener((t, from, to, f) -> transitions.add(from + "->" + to));

        cb.beforeCall();
        cb.recordFailure();
        assertSame(CircuitState.OPEN, cb.currentState());

        // 等待期未满：仍快速失败
        clock.advance(499);
        assertSame(RejectReason.CIRCUIT_OPEN, cb.beforeCall().get());
        assertSame(CircuitState.OPEN, cb.currentState(), "等待期未满不得进入 HALF_OPEN");

        // 等待期到达：第一个调用原子进入 HALF_OPEN 并被放行（中间状态可观察）
        clock.advance(1);
        assertTrue(cb.beforeCall().isEmpty(), "第一个调用应作为唯一探测放行");
        assertSame(CircuitState.HALF_OPEN, cb.currentState());

        // 第二个并发调用不得被放行：探测不放大调用量
        assertSame(RejectReason.CIRCUIT_OPEN, cb.beforeCall().get());

        cb.recordSuccess();
        assertSame(CircuitState.CLOSED, cb.currentState());
        assertEquals(0, cb.consecutiveFailures());
        assertTrue(cb.beforeCall().isEmpty());
        assertEquals(List.of("CLOSED->OPEN", "OPEN->HALF_OPEN", "HALF_OPEN->CLOSED"), transitions);
    }

    @Test
    void halfOpenProbeFailure_reopensAndRetimes_withoutFlapping() {
        VirtualClock clock = new VirtualClock();
        CircuitBreaker cb = new CircuitBreaker("svc", cfg(1, 500), clock);

        cb.beforeCall();
        cb.recordFailure(); // CLOSED -> OPEN
        clock.advance(500);
        assertTrue(cb.beforeCall().isEmpty()); // -> HALF_OPEN（探测）
        assertSame(CircuitState.HALF_OPEN, cb.currentState());
        cb.recordFailure(); // 探测失败 -> OPEN 重新计时
        assertSame(CircuitState.OPEN, cb.currentState());

        // 刚重新打开：必须继续快速失败，不能立刻又给探测（防临界点反复切换）
        assertSame(RejectReason.CIRCUIT_OPEN, cb.beforeCall().get());
        assertSame(CircuitState.OPEN, cb.currentState());

        clock.advance(500);
        assertTrue(cb.beforeCall().isEmpty(), "重新等待后才允许新探测");
        assertSame(CircuitState.HALF_OPEN, cb.currentState());
        cb.recordSuccess();
        assertSame(CircuitState.CLOSED, cb.currentState());
    }

    @Test
    void successResetsConsecutiveFailures() {
        VirtualClock clock = new VirtualClock();
        CircuitBreaker cb = new CircuitBreaker("svc", cfg(3, 1000), clock);
        cb.beforeCall();
        cb.recordFailure();
        cb.beforeCall();
        cb.recordSuccess();
        assertEquals(0, cb.consecutiveFailures());
        cb.beforeCall();
        cb.recordFailure();
        cb.beforeCall();
        cb.recordFailure();
        assertSame(CircuitState.CLOSED, cb.currentState(), "成功已清零，再来两次失败不应开熔断");
        assertFalse(cb.beforeCall().isPresent());
    }
}
