package com.github.lisama569.outboundcallgovernancejava.time;

/**
 * 退避等待的睡眠抽象，可注入测试替身。
 */
public interface Sleeper {
    void sleep(long millis) throws InterruptedException;
}
