package com.github.lisama569.outboundcallgovernancejava.governance;

import org.springframework.stereotype.Component;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 可编程的模拟下游依赖：本地稳定复现超时、可重试失败等故障，
 * 不依赖外部服务或真实凭据。故障模式与延迟按目标独立设置。
 */
@Component
public class SimulatedDownstream implements DownstreamClient {

    public enum Mode {
        HEALTHY,
        /** 抛出可重试异常 */
        FAIL_RETRYABLE,
        /** 抛出不可重试异常 */
        FAIL_FATAL,
        /** 调用挂起（配合延迟设置模拟超时） */
        SLOW
    }

    private static final class Behavior {
        volatile Mode mode = Mode.HEALTHY;
        volatile long latencyMillis = 0;
        final AtomicInteger invocations = new AtomicInteger();
    }

    private final ConcurrentHashMap<String, Behavior> behaviors = new ConcurrentHashMap<>();

    @Override
    public String invoke(String target, String payload) throws DownstreamException {
        Behavior b = behaviors.computeIfAbsent(target, t -> new Behavior());
        b.invocations.incrementAndGet();
        long latency = b.latencyMillis;
        if (b.mode == Mode.SLOW && latency <= 0) {
            latency = 5_000; // 默认挂起 5s，足以触发常规超时
        }
        if (latency > 0) {
            try {
                Thread.sleep(latency);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new DownstreamException("interrupted", false);
            }
        }
        return switch (b.mode) {
            case HEALTHY -> "ok:" + target + ":" + payload;
            case FAIL_RETRYABLE ->
                    throw new DownstreamException("simulated retryable failure on " + target, true);
            case FAIL_FATAL ->
                    throw new DownstreamException("simulated fatal failure on " + target, false);
            case SLOW -> "slow-ok:" + target + ":" + payload;
        };
    }

    /** 设置某目标的故障模式。 */
    public void setMode(String target, Mode mode) {
        behaviors.computeIfAbsent(target, t -> new Behavior()).mode = mode;
    }

    /** 设置某目标的模拟延迟（毫秒）。 */
    public void setLatency(String target, long latencyMillis) {
        behaviors.computeIfAbsent(target, t -> new Behavior()).latencyMillis = latencyMillis;
    }

    /** 某目标实际被执行的调用次数（用于断言调用量）。 */
    public int invocationCount(String target) {
        Behavior b = behaviors.get(target);
        return b == null ? 0 : b.invocations.get();
    }
}
