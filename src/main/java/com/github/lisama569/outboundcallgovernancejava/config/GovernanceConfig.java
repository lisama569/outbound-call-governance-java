package com.github.lisama569.outboundcallgovernancejava.config;

import com.github.lisama569.outboundcallgovernancejava.governance.GovernanceClient;
import com.github.lisama569.outboundcallgovernancejava.governance.GovernanceRegistry;
import com.github.lisama569.outboundcallgovernancejava.idempotency.IdempotencyStore;
import com.github.lisama569.outboundcallgovernancejava.simulation.SimulatedDownstream;
import com.github.lisama569.outboundcallgovernancejava.time.SystemTimeSource;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Spring 装配：把治理组件注册为单例 Bean。
 */
@Configuration
public class GovernanceConfig {

    @Bean
    public SystemTimeSource systemTimeSource() {
        return new SystemTimeSource();
    }

    @Bean
    public GovernanceRegistry governanceRegistry(SystemTimeSource timeSource) {
        return new GovernanceRegistry(timeSource);
    }

    @Bean
    public IdempotencyStore idempotencyStore(SystemTimeSource timeSource) {
        return new IdempotencyStore(timeSource, 60_000L);
    }

    @Bean
    public SimulatedDownstream simulatedDownstream() {
        return new SimulatedDownstream();
    }

    @Bean
    public GovernanceClient governanceClient(GovernanceRegistry registry,
                                             IdempotencyStore idempotencyStore,
                                             SimulatedDownstream downstream,
                                             SystemTimeSource timeSource) {
        return new GovernanceClient(registry, idempotencyStore, downstream, timeSource, timeSource);
    }
}
