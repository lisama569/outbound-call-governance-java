package com.github.lisama569.outboundcallgovernancejava.governance;

/**
 * 下游依赖调用接口。
 */
public interface DownstreamClient {
    /**
     * 执行一次下游调用。
     *
     * @param target  调用目标
     * @param payload 请求内容
     * @return 下游返回值
     * @throws DownstreamException 调用失败（可重试或不可重试）
     */
    String invoke(String target, String payload) throws DownstreamException;
}
