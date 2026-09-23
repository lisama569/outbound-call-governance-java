package com.github.lisama569.outboundcallgovernancejava.support;

import com.github.lisama569.outboundcallgovernancejava.time.Sleeper;
import com.github.lisama569.outboundcallgovernancejava.time.TimeSource;

import java.util.concurrent.atomic.AtomicLong;

/**
 * 测试用虚拟时钟：时间只在显式 advance 时前进；sleep 直接推进虚拟时间并可被中断，
 * 从而确定性地验证窗口滚动、退避、预算耗尽与熔断等待，无需真实等待。
 */
public class VirtualClock implements TimeSource, Sleeper {

    private final AtomicLong now = new AtomicLong(0);

    @Override
    public long nowMillis() {
        return now.get();
    }

    public void advance(long deltaMillis) {
        now.addAndGet(deltaMillis);
    }

    public void setNow(long millis) {
        now.set(millis);
    }

    @Override
    public void sleep(long millis) throws InterruptedException {
        if (Thread.currentThread().isInterrupted()) {
            throw new InterruptedException();
        }
        advance(millis);
    }
}
