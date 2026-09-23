package com.github.lisama569.outboundcallgovernancejava.simulation;

import com.github.lisama569.outboundcallgovernancejava.governance.DownstreamFailureException;
import com.github.lisama569.outboundcallgovernancejava.governance.DownstreamInvoker;
import com.github.lisama569.outboundcallgovernancejava.governance.DownstreamTimeoutException;
import com.github.lisama569.outboundcallgovernancejava.time.Sleeper;
import com.github.lisama569.outboundcallgovernancejava.time.SystemTimeSource;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 本地下游模拟器：可稳定复现超时、连续失败、部分失败、慢调用，
 * 不依赖任何外部服务或真实凭据。按 target 配置行为并统计真实调用次数，
 * 便于断言重试/熔断探测没有放大调用量。
 */
public class SimulatedDownstream implements DownstreamInvoker {

    /** 单次调用的剧本。 */
    public enum Mode {
        /** 永远成功 */
        OK,
        /** 睡眠 latencyMillis 后抛出超时异常 */
        TIMEOUT,
        /** 永远抛出可重试的服务故障 */
        FAIL,
        /** 前 failFirstN 次失败，之后成功 */
        FAIL_FIRST_N,
        /** 睡眠 latencyMillis 后成功（慢调用，配合短超时可复现超时） */
        SLOW
    }

    /** 单目标的行为配置。 */
    public static final class Behavior {
        public final Mode mode;
        public final long latencyMillis;
        public final int failFirstN;

        public Behavior(Mode mode, long latencyMillis, int failFirstN) {
            this.mode = mode;
            this.latencyMillis = latencyMillis;
            this.failFirstN = failFirstN;
        }

        public static Behavior ok() {
            return new Behavior(Mode.OK, 0L, 0);
        }

        public static Behavior timeout(long latencyMillis) {
            return new Behavior(Mode.TIMEOUT, latencyMillis, 0);
        }

        public static Behavior fail() {
            return new Behavior(Mode.FAIL, 0L, 0);
        }

        public static Behavior failFirstN(int n) {
            return new Behavior(Mode.FAIL_FIRST_N, 0L, n);
        }

        public static Behavior slow(long latencyMillis) {
            return new Behavior(Mode.SLOW, latencyMillis, 0);
        }
    }

    private final Map<String, Behavior> behaviors = new ConcurrentHashMap<>();
    private final Map<String, AtomicInteger> counters = new ConcurrentHashMap<>();
    private final Sleeper sleeper;

    public SimulatedDownstream() {
        this(new SystemTimeSource());
    }

    public SimulatedDownstream(Sleeper sleeper) {
        this.sleeper = sleeper;
    }

    public void setBehavior(String target, Behavior behavior) {
        behaviors.put(target, behavior);
        counters.computeIfAbsent(target, t -> new AtomicInteger());
    }

    /** 返回该目标接收到的真实调用次数（用于断言重试/探测没有放大调用量）。 */
    public int invocationCount(String target) {
        AtomicInteger c = counters.get(target);
        return c == null ? 0 : c.get();
    }

    public void resetCount(String target) {
        AtomicInteger c = counters.get(target);
        if (c != null) {
            c.set(0);
        }
    }

    @Override
    public String invoke(String target, String requestId, String payload) throws Exception {
        counters.computeIfAbsent(target, t -> new AtomicInteger()).incrementAndGet();
        Behavior behavior = behaviors.getOrDefault(target, Behavior.ok());
        switch (behavior.mode) {
            case TIMEOUT -> {
                if (behavior.latencyMillis > 0) {
                    sleeper.sleep(behavior.latencyMillis);
                }
                throw new DownstreamTimeoutException(
                        "模拟下游超时 target=" + target + " requestId=" + requestId);
            }
            case FAIL -> throw new DownstreamFailureException(
                    "模拟下游故障 target=" + target + " requestId=" + requestId);
            case FAIL_FIRST_N -> {
                int n = counters.get(target).get();
                if (n <= behavior.failFirstN) {
                    throw new DownstreamFailureException(
                            "模拟下游前 " + behavior.failFirstN + " 次失败，当前第 " + n
                                    + " 次 target=" + target + " requestId=" + requestId);
                }
                return "ok(" + target + "," + n + ")";
            }
            case SLOW -> {
                sleeper.sleep(behavior.latencyMillis);
                return "slow-ok(" + target + ")";
            }
            case OK -> {
                return "ok(" + target + ")";
            }
            default -> {
                return "ok(" + target + ")";
            }
        }
    }
}
