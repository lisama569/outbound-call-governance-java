package com.github.lisama569.outboundcallgovernancejava.tests.governance;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.lisama569.outboundcallgovernancejava.governance.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class JacksonSmokeTest {
    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    void deserializeWithoutFallbackField() throws Exception {
        String json = "{\"quotaPerWindow\":100,\"windowMillis\":60000,\"maxConcurrency\":10,"
                + "\"callTimeoutMillis\":2000,\"maxAttempts\":3,\"backoffMillis\":100,"
                + "\"retryBudgetMillis\":10000,\"circuitFailureThreshold\":3,\"circuitResetMillis\":5000}";
        TargetConfig c = mapper.readValue(json, TargetConfig.class);
        assertNotNull(c.fallback());
        assertTrue(c.fallback().isNone());
    }

    @Test
    void deserializeWithFallbackField() throws Exception {
        String json = "{\"quotaPerWindow\":100,\"windowMillis\":60000,\"maxConcurrency\":10,"
                + "\"callTimeoutMillis\":2000,\"maxAttempts\":3,\"backoffMillis\":100,"
                + "\"retryBudgetMillis\":10000,\"circuitFailureThreshold\":3,\"circuitResetMillis\":5000,"
                + "\"fallback\":{\"staticValue\":null,\"fallbackTarget\":\"pay-backup\"}}";
        TargetConfig c = mapper.readValue(json, TargetConfig.class);
        assertEquals("pay-backup", c.fallback().fallbackTarget());
    }
}
