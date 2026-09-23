package com.github.lisama569.outboundcallgovernancejava.circuit;

import com.github.lisama569.outboundcallgovernancejava.model.CircuitState;
import com.github.lisama569.outboundcallgovernancejava.model.RejectReason;
import com.github.lisama569.outboundcallgovernancejava.model.TargetConfig;
import com.github.lisama569.outboundcallgovernancejava.time.TimeSource;

import java.util.Optional;

/**
 * 单目标熔断器。
 *
 * <p>状态机：</p>
 * <ul>
 *   <li>CLOSED：正常放行；连续失败达到 {@code failureThreshold} 后转为 OPEN。</li>
 *   <li>OPEN：快速失败（{@link RejectReason#CIRCUIT_OPEN}）；经过 {@code openWaitMillis}
 *       后，下一次调用把状态推进到 HALF_OPEN，HALF_OPEN 是可观察的中间状态。</li>
 *   <li>HALF_OPEN：同一时刻只放行一个探测请求（其余请求仍快速失败），因此探测失败不会
 *       放大调用量；探测成功 -> CLOSED（计数清零），探测失败 -> OPEN 并重新计时。</li>
 * </ul>
 *
 * <p>状态迁移只基于单调推进的时间与受监视器保护的状态判定，OPEN→HALF_OPEN 的临界
 * 切换由第一个到达的调用原子完成，不会在临界点反复横跳。</p>
 */
public class CircuitBreaker {

    /** 不可变状态快照。 */
    record Snapshot(CircuitState state, int consecutiveFailures, long openedAtMillis) {
        static Snapshot closed() {
            return new Snapshot(CircuitState.CLOSED, 0, 0L);
        }
    }

    /** 状态迁移监听器，用于日志与测试可观察性。 */
    public interface TransitionListener {
        void onTransition(String target, CircuitState from, CircuitState to, int consecutiveFailures);
    }

    private final String target;
    private final TimeSource timeSource;
    private volatile TargetConfig config;
    private volatile TransitionListener listener;

    private Snapshot snapshot = Snapshot.closed();

    public CircuitBreaker(String target, TargetConfig config, TimeSource timeSource) {
        this.target = target;
        this.config = config;
        this.timeSource = timeSource;
    }

    /** 运行中更新阈值/等待时长，立即对后续判定生效；不强制改变当前状态。 */
    public synchronized void updateConfig(TargetConfig config) {
        this.config = config;
    }

    public void setTransitionListener(TransitionListener listener) {
        this.listener = listener;
    }

    /**
     * 调用前检查（可能发生状态迁移）。
     * @return 非空表示应快速失败；空表示放行（HALF_OPEN 下仅一个探测线程可拿到放行）
     */
    public synchronized Optional<RejectReason> beforeCall() {
        Snapshot s = snapshot;
        long now = timeSource.nowMillis();
        switch (s.state()) {
            case CLOSED -> {
                return Optional.empty();
            }
            case OPEN -> {
                if (now - s.openedAtMillis() >= config.openWaitMillis()) {
                    // 第一个穿过等待期的调用原子地进入 HALF_OPEN，并成为唯一探测
                    transition(s, new Snapshot(CircuitState.HALF_OPEN, s.consecutiveFailures(), s.openedAtMillis()));
                    return Optional.empty();
                }
                return Optional.of(RejectReason.CIRCUIT_OPEN);
            }
            case HALF_OPEN -> {
                // 探测尚未结束：其余调用继续快速失败，不放大调用量
                return Optional.of(RejectReason.CIRCUIT_OPEN);
            }
            default -> {
                return Optional.of(RejectReason.CIRCUIT_OPEN);
            }
        }
    }

    /**
     * 只观察当前是否允许继续（用于重试退避后），不把 OPEN 推进到 HALF_OPEN——
     * OPEN→HALF_OPEN 的唯一探测只能由一次全新调用经 {@link #beforeCall()} 发起。
     */
    public synchronized Optional<RejectReason> passiveCheck() {
        return switch (snapshot.state()) {
            case CLOSED, HALF_OPEN -> Optional.empty();
            case OPEN -> Optional.of(RejectReason.CIRCUIT_OPEN);
        };
    }

    /** 探测成功或正常调用成功：HALF_OPEN -> CLOSED；CLOSED 下清零连续失败计数。 */
    public synchronized void recordSuccess() {
        Snapshot s = snapshot;
        switch (s.state()) {
            case CLOSED -> {
                if (s.consecutiveFailures() != 0) {
                    snapshot = new Snapshot(CircuitState.CLOSED, 0, 0L);
                }
            }
            case HALF_OPEN -> transition(s, Snapshot.closed());
            default -> {
                // OPEN 下理论上没有成功路径，忽略
            }
        }
    }

    /** 真实调用失败：CLOSED 累计计数并可能开熔断；HALF_OPEN 探测失败 -> OPEN 重新计时。 */
    public synchronized void recordFailure() {
        Snapshot s = snapshot;
        switch (s.state()) {
            case CLOSED -> {
                int failures = s.consecutiveFailures() + 1;
                if (failures >= config.failureThreshold()) {
                    transition(s, new Snapshot(CircuitState.OPEN, failures, timeSource.nowMillis()));
                } else {
                    snapshot = new Snapshot(CircuitState.CLOSED, failures, 0L);
                }
            }
            case HALF_OPEN ->
                // 探测失败：重新进入 OPEN 并重新计时，避免临界点反复切换
                    transition(s, new Snapshot(CircuitState.OPEN, s.consecutiveFailures() + 1, timeSource.nowMillis()));
            default -> {
                // OPEN 下不会有真实调用，忽略
            }
        }
    }

    public synchronized CircuitState currentState() {
        return snapshot.state();
    }

    public synchronized int consecutiveFailures() {
        return snapshot.consecutiveFailures();
    }

    private void transition(Snapshot from, Snapshot to) {
        snapshot = to;
        TransitionListener l = listener;
        if (l != null && from.state() != to.state()) {
            l.onTransition(target, from.state(), to.state(), to.consecutiveFailures());
        }
    }
}
