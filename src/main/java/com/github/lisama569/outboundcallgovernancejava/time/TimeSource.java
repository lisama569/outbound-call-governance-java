package com.github.lisama569.outboundcallgovernancejava.time;

/**
 * 时间源，可注入虚拟时钟以实现确定性测试。
 */
public interface TimeSource {
    long nowMillis();
}
