package com.github.lisama569.outboundcallgovernancejava.governance;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.github.lisama569.outboundcallgovernancejava.model.TargetConfig;
import com.github.lisama569.outboundcallgovernancejava.simulation.SimulatedDownstream;
import com.github.lisama569.outboundcallgovernancejava.support.TestKit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 断言治理日志包含请求标识、判定依据与熔断状态，且不同拒绝原因在日志中可区分。
 */
class LoggingObservabilityTest {

    private ListAppender<ILoggingEvent> appender;
    private Logger logger;

    @BeforeEach
    void attach() {
        logger = (Logger) LoggerFactory.getLogger(GovernanceClient.class);
        appender = new ListAppender<>();
        appender.start();
        logger.addAppender(appender);
    }

    @AfterEach
    void detach() {
        logger.detachAppender(appender);
    }

    private List<String> messages() {
        return appender.list.stream().map(ILoggingEvent::getFormattedMessage).toList();
    }

    @Test
    void logsContainRequestIdDecisionBasisAndCircuitState() {
        TestKit kit = TestKit.virtual();
        TargetConfig cfg = new TargetConfig(100, 100_000, 20, 5_000, 2, 100_000, 1,
                1, 1_000, null, null);
        kit.init("svc", cfg);
        kit.downstream.setBehavior("svc", SimulatedDownstream.Behavior.fail());

        String requestId = "req-LOG-1";
        kit.client.call("svc", new CallContext(requestId, null, "p"));
        kit.client.call("svc", new CallContext(requestId, null, "p")); // 熔断打开后快速失败

        List<String> all = messages();

        // 每条关键日志都带请求标识
        assertTrue(all.stream().anyMatch(m -> m.contains(requestId)), "必须打印请求标识");
        // 判定依据
        assertTrue(all.stream().anyMatch(m -> m.contains("判定依据")), "必须打印判定依据");
        assertTrue(all.stream().anyMatch(m -> m.contains("DownstreamFailureException")),
                "必须打印可区分的失败判定依据");
        assertTrue(all.stream().anyMatch(m -> m.contains("CIRCUIT_OPEN")),
                "快速失败必须打印 CIRCUIT_OPEN 判定依据");
        // 熔断状态
        assertTrue(all.stream().anyMatch(m -> m.contains("熔断状态=OPEN")), "必须打印熔断状态 OPEN");
        assertTrue(all.stream().anyMatch(m -> m.contains("熔断状态迁移")), "熔断迁移必须可观察");
        // 至少一条 WARN 级别的拒绝/失败日志
        assertTrue(appender.list.stream().anyMatch(e -> e.getLevel() == Level.WARN));
    }

    @Test
    void quotaAndUnknownTargetReasonsAreDistinguishableInLogs() {
        TestKit kit = TestKit.virtual();
        kit.init("svc", new TargetConfig(1, 1_000_000, 10, 5_000, 1, 60_000, 0,
                100, 1_000_000, null, null));
        kit.downstream.setBehavior("svc", SimulatedDownstream.Behavior.ok());

        kit.client.call("svc", new CallContext("req-Q1", null, "p"));
        kit.client.call("svc", new CallContext("req-Q2", null, "p")); // QUOTA_EXCEEDED
        kit.client.call("ghost", new CallContext("req-U1", null, "p")); // UNKNOWN_TARGET

        List<String> all = messages();
        assertTrue(all.stream().anyMatch(m -> m.contains("req-Q2") && m.contains("QUOTA_EXCEEDED")),
                "配额拒绝必须带请求标识与 QUOTA_EXCEEDED");
        assertTrue(all.stream().anyMatch(m -> m.contains("req-U1") && m.contains("UNKNOWN_TARGET")),
                "未知目标必须带请求标识与 UNKNOWN_TARGET");
    }
}
