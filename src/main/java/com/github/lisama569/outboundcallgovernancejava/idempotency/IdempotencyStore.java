package com.github.lisama569.outboundcallgovernancejava.idempotency;

import com.github.lisama569.outboundcallgovernancejava.model.CallOutcome;
import com.github.lisama569.outboundcallgovernancejava.time.TimeSource;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 幂等键去重存储（按 target + idempotencyKey 维度）。
 *
 * <p>同一键的并发/重试请求只会有一次实质执行：第一个到达的线程成为 LEADER 真正执行，
 * 其余线程拿到 IN_FLIGHT（可观察持有者线程，以区分“执行中”）并在 Entry 监视器上
 * 等待；LEADER 成功后写入不可变结果并唤醒所有等待者，后来者拿到 COMPLETED 与一致
 * 结果。LEADER 最终失败（无回退）时调用 {@link #release} 删除占位，重复请求不被
 * 失败结果毒化，可由后续重试重新执行。完成结果保留 {@code ttlMillis} 后过期。</p>
 */
public class IdempotencyStore {

    /** 一次占位尝试的状态。 */
    public enum AttemptStatus {
        /** 当前线程获得唯一执行权 */
        LEADER,
        /** 另一线程正在执行（执行中），等待后可取得结果 */
        IN_FLIGHT,
        /** 该键已经完成，直接返回缓存的一致结果 */
        COMPLETED
    }

    /** 申请结果。 */
    public record Attempt(AttemptStatus status, CallOutcome completed, String holderThread) {
        static Attempt leader() {
            return new Attempt(AttemptStatus.LEADER, null, Thread.currentThread().getName());
        }
        static Attempt inFlight(String holderThread) {
            return new Attempt(AttemptStatus.IN_FLIGHT, null, holderThread);
        }
        static Attempt completed(CallOutcome outcome) {
            return new Attempt(AttemptStatus.COMPLETED, outcome, null);
        }
        /** 是否能区分“执行中”：可观察到持有者线程 */
        public Optional<String> holder() {
            return Optional.ofNullable(holderThread);
        }
    }

    private static final class Entry {
        final String holderThread;
        final long createdAtMillis;
        /** 完成后的一致结果；为 null 表示仍在执行 */
        volatile CallOutcome outcome;

        Entry(String holderThread, long createdAtMillis) {
            this.holderThread = holderThread;
            this.createdAtMillis = createdAtMillis;
        }
    }

    private final TimeSource timeSource;
    private final long ttlMillis;
    private final ConcurrentHashMap<String, Entry> entries = new ConcurrentHashMap<>();

    public IdempotencyStore(TimeSource timeSource, long ttlMillis) {
        this.timeSource = timeSource;
        this.ttlMillis = ttlMillis;
    }

    private static String key(String target, String idempotencyKey) {
        return target + "#" + idempotencyKey;
    }

    /**
     * 以幂等键申请一次调用。
     * @param waitMillis IN_FLIGHT 时最多等待多久（毫秒），&lt;=0 表示不等待
     * @return LEADER / IN_FLIGHT / COMPLETED
     */
    public Attempt acquire(String target, String key, long waitMillis) throws InterruptedException {
        String k = key(target, key);
        long now = timeSource.nowMillis();
        // putIfAbsent 原子决定唯一 LEADER
        Entry fresh = new Entry(Thread.currentThread().getName(), now);
        Entry existing = entries.putIfAbsent(k, fresh);
        if (existing == null) {
            return Attempt.leader();
        }
        synchronized (existing) {
            // 已完成且未过期
            if (existing.outcome != null && now - existing.createdAtMillis < ttlMillis) {
                return Attempt.completed(existing.outcome);
            }
            // 已完成但已过期：替换为新的执行占位
            if (existing.outcome != null) {
                entries.put(k, fresh);
                return Attempt.leader();
            }
            if (waitMillis <= 0) {
                return Attempt.inFlight(existing.holderThread);
            }
            long deadline = timeSource.nowMillis() + waitMillis;
            while (existing.outcome == null) {
                long remaining = deadline - timeSource.nowMillis();
                if (remaining <= 0) {
                    return Attempt.inFlight(existing.holderThread);
                }
                existing.wait(remaining);
            }
            if (timeSource.nowMillis() - existing.createdAtMillis >= ttlMillis) {
                entries.put(k, fresh);
                return Attempt.leader();
            }
            return Attempt.completed(existing.outcome);
        }
    }

    /** LEADER 完成后记录一致结果并唤醒所有等待者。 */
    public void complete(String target, String key, CallOutcome outcome) {
        Entry entry = entries.get(key(target, key));
        if (entry == null) {
            return;
        }
        synchronized (entry) {
            entry.outcome = outcome;
            entry.notifyAll();
        }
    }

    /** LEADER 最终失败时删除占位并唤醒等待者，允许后续同键请求重新执行。 */
    public void release(String target, String key) {
        Entry entry = entries.remove(key(target, key));
        if (entry != null) {
            synchronized (entry) {
                entry.notifyAll();
            }
        }
    }

    /** 不等待地查看键状态（测试与可观察性用），过期结果视为不存在。 */
    public Attempt peek(String target, String key) {
        Entry entry = entries.get(key(target, key));
        if (entry == null) {
            return Attempt.inFlight(null);
        }
        synchronized (entry) {
            if (entry.outcome != null
                    && timeSource.nowMillis() - entry.createdAtMillis < ttlMillis) {
                return Attempt.completed(entry.outcome);
            }
            return Attempt.inFlight(entry.holderThread);
        }
    }
}
