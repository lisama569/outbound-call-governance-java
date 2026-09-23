package com.github.lisama569.outboundcallgovernancejava.governance;

/**
 * 熔断器状态机：CLOSED -> OPEN -> HALF_OPEN -> CLOSED/OPEN。
 *
 * 设计要点：
 * - OPEN 期间快速失败；冷却期满后仅允许迁移到 HALF_OPEN（可观察的中间状态）。
 * - HALF_OPEN 只发放一个探测许可，探测期间其余请求一律快速失败，不放大调用量。
 * - 探测成功 -> CLOSED 并清零失败计数；探测失败 -> OPEN 并重新计时，
 *   状态迁移只由已记录的结果驱动，避免临界点反复切换。
 */
class CircuitBreaker {

    private int failureThreshold;
    private long resetMillis;

    private CircuitState state = CircuitState.CLOSED;
    private int consecutiveFailures;
    private long openedAt;
    private boolean probeInFlight;

    CircuitBreaker(TargetConfig config) {
        this.failureThreshold = config.circuitFailureThreshold();
        this.resetMillis = config.circuitResetMillis();
    }

    synchronized void reconfigure(TargetConfig config) {
        this.failureThreshold = config.circuitFailureThreshold();
        this.resetMillis = config.circuitResetMillis();
    }

    /**
     * 尝试获取调用许可。
     *
     * @return true 表示允许调用（CLOSED 正常许可或 HALF_OPEN 探测许可）
     */
    synchronized boolean tryPermit() {
        switch (state) {
            case CLOSED -> {
                return true;
            }
            case OPEN -> {
                if (System.currentTimeMillis() - openedAt >= resetMillis) {
                    // 冷却期满，迁移到可观察的半开状态，发放唯一探测许可
                    state = CircuitState.HALF_OPEN;
                    probeInFlight = true;
                    return true;
                }
                return false;
            }
            case HALF_OPEN -> {
                if (probeInFlight) {
                    return false; // 探测进行中，其余请求快速失败
                }
                probeInFlight = true;
                return true;
            }
        }
        return false;
    }

    /** 记录一次成功：半开探测成功则闭合，正常状态清零失败计数。 */
    synchronized void onSuccess() {
        if (state == CircuitState.HALF_OPEN) {
            probeInFlight = false;
            state = CircuitState.CLOSED;
        }
        consecutiveFailures = 0;
    }

    /** 记录一次失败：达到阈值打开熔断；半开探测失败立即重新打开并重新计时。 */
    synchronized void onFailure() {
        if (state == CircuitState.HALF_OPEN) {
            probeInFlight = false;
            open();
            return;
        }
        consecutiveFailures++;
        if (consecutiveFailures >= failureThreshold) {
            open();
        }
    }

    synchronized CircuitState state() {
        // 只读观察：不触发迁移，迁移由 tryPermit 驱动，保证状态变化可解释
        return state;
    }

    synchronized int consecutiveFailures() {
        return consecutiveFailures;
    }

    private void open() {
        state = CircuitState.OPEN;
        openedAt = System.currentTimeMillis();
    }
}
