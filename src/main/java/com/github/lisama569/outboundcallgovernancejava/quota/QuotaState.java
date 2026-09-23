package com.github.lisama569.outboundcallgovernancejava.quota;

import com.github.lisama569.outboundcallgovernancejava.model.RejectReason;
import com.github.lisama569.outboundcallgovernancejava.model.TargetConfig;
import com.github.lisama569.outboundcallgovernancejava.time.TimeSource;

/**
 * 单目标的固定窗配额 + 并发上限状态。
 *
 * <p>配额模型：每个长度为 {@code windowMillis} 的固定窗口最多放行 {@code quotaPerWindow}
 * 次真实调用；窗口在第一次申请时惰性开启，跨窗后计数清零。并发上限通过公平信号量控制，
 * 许可数可在运行中调整，进行中的调用不被抢占，新上限对后续申请立即生效。</p>
 *
 * <p>判定顺序：先配额后并发；配额已满返回 {@link RejectReason#QUOTA_EXCEEDED}，
 * 拿不到并发许可返回 {@link RejectReason#CONCURRENCY_LIMIT}，两者可区分。
 * 被并发拒绝不消耗配额（没有发生真实调用）。</p>
 */
public class QuotaState {

    /** 申请结果：被拒绝时 reason 非空；获得许可时 ticket 非空，结束时必须 close。 */
    public record Acquire(RejectReason reason, QuotaTicket ticket) {
        static Acquire granted(QuotaTicket ticket) {
            return new Acquire(null, ticket);
        }
        static Acquire rejected(RejectReason reason) {
            return new Acquire(reason, null);
        }
        public boolean granted() {
            return ticket != null;
        }
    }

    /** 占用一个并发许可的凭证，close 时归还，必须由真实调用方在 finally 中关闭。 */
    public static final class QuotaTicket implements AutoCloseable {
        private final AdjustableSemaphore semaphore;
        private volatile boolean closed;

        QuotaTicket(AdjustableSemaphore semaphore) {
            this.semaphore = semaphore;
        }

        @Override
        public void close() {
            if (!closed) {
                closed = true;
                semaphore.release();
            }
        }
    }

    /** 公平且可在运行中调整许可数的信号量。 */
    static final class AdjustableSemaphore extends java.util.concurrent.Semaphore {
        AdjustableSemaphore(int permits) {
            super(permits, true);
        }

        /**
         * 将总许可数调整为 newPermits：排空当前空闲许可后重新发放。
         * 已被持有（进行中调用）的许可不受影响，调用结束后自然归还。
         */
        void setPermits(int newPermits) {
            drainPermits();
            if (newPermits > 0) {
                release(newPermits);
            }
        }
    }

    private final TimeSource timeSource;
    private volatile TargetConfig config;
    private final AdjustableSemaphore semaphore;

    // 固定窗计数，仅在 this 监视器内访问
    private long windowStartMillis = Long.MIN_VALUE;
    private int usedInWindow;

    public QuotaState(TargetConfig config, TimeSource timeSource) {
        this.config = config;
        this.timeSource = timeSource;
        this.semaphore = new AdjustableSemaphore(config.maxConcurrency());
    }

    /** 运行中更新配置：并发上限立即调整，配额上限对当前窗口立即生效，窗口计时不重置。 */
    public synchronized void updateConfig(TargetConfig config) {
        this.config = config;
        this.semaphore.setPermits(config.maxConcurrency());
    }

    /** 申请一次真实调用：先判时间窗配额，再判并发上限。 */
    public synchronized Acquire tryAcquire() {
        long now = timeSource.nowMillis();
        if (windowStartMillis == Long.MIN_VALUE || now - windowStartMillis >= config.windowMillis()) {
            // 进入新窗口
            windowStartMillis = now;
            usedInWindow = 0;
        }
        if (usedInWindow >= config.quotaPerWindow()) {
            return Acquire.rejected(RejectReason.QUOTA_EXCEEDED);
        }
        if (!semaphore.tryAcquire()) {
            // 未获得并发许可，不消耗配额
            return Acquire.rejected(RejectReason.CONCURRENCY_LIMIT);
        }
        usedInWindow++;
        return Acquire.granted(new QuotaTicket(semaphore));
    }
}
