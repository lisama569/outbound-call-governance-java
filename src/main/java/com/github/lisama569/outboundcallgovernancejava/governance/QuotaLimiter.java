package com.github.lisama569.outboundcallgovernancejava.governance;

import java.util.concurrent.Semaphore;

/**
 * 单目标配额（固定窗口计数）与并发（信号量）限制器。
 * 配置变更运行中生效：窗口计数与在途并发在变更后继续保持，
 * 以便注册表能识别“回退到低于当前用量”的冲突。
 */
class QuotaLimiter {

    private final Semaphore concurrencyPermits;

    private long windowMillis;
    private int quotaPerWindow;
    private int maxPermits;
    private long windowStart;
    private int windowCount;

    QuotaLimiter(TargetConfig config) {
        this.windowMillis = config.windowMillis();
        this.quotaPerWindow = config.quotaPerWindow();
        this.maxPermits = config.maxConcurrency();
        this.concurrencyPermits = new Semaphore(config.maxConcurrency());
        this.windowStart = System.currentTimeMillis();
    }

    /**
     * 运行中应用新配置。窗口计数与在途并发保持不变；
     * 并发许可按差额增减信号量。
     */
    synchronized void reconfigure(TargetConfig config) {
        rollWindowIfNeeded();
        int delta = config.maxConcurrency() - maxPermits;
        if (delta > 0) {
            concurrencyPermits.release(delta);
        } else if (delta < 0) {
            concurrencyPermits.acquireUninterruptibly(-delta);
        }
        maxPermits = config.maxConcurrency();
        this.windowMillis = config.windowMillis();
        this.quotaPerWindow = config.quotaPerWindow();
    }

    /** 尝试占用一个配额，成功返回 true。 */
    synchronized boolean tryAcquireQuota() {
        rollWindowIfNeeded();
        if (windowCount >= quotaPerWindow) {
            return false;
        }
        windowCount++;
        return true;
    }

    /** 退回一个配额（后续节点拒绝时调用，避免误计）。 */
    synchronized void refundQuota() {
        if (windowCount > 0) {
            windowCount--;
        }
    }

    /** 尝试占用一个并发许可，成功返回 true。 */
    boolean tryAcquireConcurrency() {
        return concurrencyPermits.tryAcquire();
    }

    void releaseConcurrency() {
        concurrencyPermits.release();
    }

    synchronized int currentWindowUsage() {
        rollWindowIfNeeded();
        return windowCount;
    }

    synchronized int currentInFlight() {
        return maxPermits - concurrencyPermits.availablePermits();
    }

    /** 当前配额上限（供判定依据日志使用）。 */
    synchronized int quotaPerWindow() {
        return quotaPerWindow;
    }

    private void rollWindowIfNeeded() {
        long now = System.currentTimeMillis();
        if (now - windowStart >= windowMillis) {
            windowStart = now;
            windowCount = 0;
        }
    }
}
