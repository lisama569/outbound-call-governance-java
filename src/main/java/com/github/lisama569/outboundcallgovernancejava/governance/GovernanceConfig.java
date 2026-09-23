package com.github.lisama569.outboundcallgovernancejava.governance;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * 治理组件装配。
 */
@Configuration
public class GovernanceConfig {

    @Bean
    public GovernanceConfigRegistry governanceConfigRegistry() {
        return new GovernanceConfigRegistry();
    }
}
