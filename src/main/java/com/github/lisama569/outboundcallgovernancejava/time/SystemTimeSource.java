package com.github.lisama569.outboundcallgovernancejava.time;

/**
 * 基于系统时钟与 Thread.sleep 的默认实现。
 */
public class SystemTimeSource implements TimeSource, Sleeper {

    @Override
    public long nowMillis() {
        return System.currentTimeMillis();
    }

    @Override
    public void sleep(long millis) throws InterruptedException {
        Thread.sleep(millis);
    }
}
