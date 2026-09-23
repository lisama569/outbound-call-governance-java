package com.github.lisama569.outboundcallgovernancejava.governance;

/**
 * 下游调用的抽象。真实系统中由 HTTP/RPC 客户端实现；
 * 本地测试用 SimulatedDownstream 模拟超时/失败，无需外部服务或凭据。
 */
@FunctionalInterface
public interface DownstreamInvoker {

    /**
     * 执行一次下游调用。
     * @param target     目标名
     * @param requestId  请求标识（日志/追踪用）
     * @param payload    请求负载
     * @return 下游返回的字符串结果
     * @throws Exception 下游故障（超时、连接失败、业务异常等）
     */
    String invoke(String target, String requestId, String payload) throws Exception;
}
