package com.github.lisama569.outboundcallgovernancejava.web;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.lisama569.outboundcallgovernancejava.governance.GovernanceClient;
import com.github.lisama569.outboundcallgovernancejava.governance.GovernanceRegistry;
import com.github.lisama569.outboundcallgovernancejava.model.TargetConfig;
import com.github.lisama569.outboundcallgovernancejava.simulation.SimulatedDownstream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * 本地手工验证端点的 MVC 冒烟测试（standalone，无需启动真实端口）。
 */
class GovernanceControllerIT {

    private final ObjectMapper om = new ObjectMapper();
    private MockMvc mvc;

    @BeforeEach
    void setup() {
        com.github.lisama569.outboundcallgovernancejava.time.SystemTimeSource time =
                new com.github.lisama569.outboundcallgovernancejava.time.SystemTimeSource();
        SimulatedDownstream downstream = new SimulatedDownstream(time);
        GovernanceRegistry registry = new GovernanceRegistry(time);
        com.github.lisama569.outboundcallgovernancejava.idempotency.IdempotencyStore idem =
                new com.github.lisama569.outboundcallgovernancejava.idempotency.IdempotencyStore(time, 600_000);
        GovernanceClient client = new GovernanceClient(registry, idem, downstream, time, time);
        mvc = MockMvcBuilders.standaloneSetup(new GovernanceController(client, registry, downstream)).build();
    }

    @Test
    void configureTarget_defineBehavior_callAndInspectCircuit() throws Exception {
        TargetConfig cfg = new TargetConfig(3, 60_000, 5, 5_000, 2, 60_000, 1,
                2, 1_000, null, null);

        // 1) 合法配置被接受
        mvc.perform(put("/governance/targets/svc")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(om.writeValueAsString(cfg)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.accepted").value(true));

        // 2) 非法配置被拒绝且原因可区分
        TargetConfig bad = new TargetConfig(0, 60_000, 5, 5_000, 2, 60_000, 1,
                2, 1_000, null, null);
        mvc.perform(put("/governance/targets/svc")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(om.writeValueAsString(bad)))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.reason").value("INVALID_CONFIG"));

        // 3) 模拟下游永远成功
        mvc.perform(post("/governance/targets/svc/behavior")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"mode":"OK"}
                                """))
                .andExpect(status().isOk());

        // 4) 发起调用：成功
        mvc.perform(post("/governance/call/svc")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"requestId":"req-http-1","payload":"hi"}
                                """))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("SUCCESS"))
                .andExpect(jsonPath("$.value").value("ok(svc)"))
                .andExpect(jsonPath("$.requestId").value("req-http-1"));

        // 5) 未知目标
        mvc.perform(post("/governance/call/nope")
                        .contentType(MediaType.APPLICATION_JSON).content("{}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("REJECTED"))
                .andExpect(jsonPath("$.reason").value("UNKNOWN_TARGET"));

        // 6) 熔断状态查询：CLOSED
        mvc.perform(get("/governance/circuit/svc"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.state").value("CLOSED"));
    }

    @Test
    void fallbackConfigRejectedOverHttpWithDistinctReason() throws Exception {
        com.github.lisama569.outboundcallgovernancejava.time.SystemTimeSource time =
                new com.github.lisama569.outboundcallgovernancejava.time.SystemTimeSource();
        SimulatedDownstream downstream = new SimulatedDownstream(time);
        GovernanceRegistry registry = new GovernanceRegistry(time);
        var idem = new com.github.lisama569.outboundcallgovernancejava.idempotency.IdempotencyStore(time, 60_000);
        GovernanceClient client = new GovernanceClient(registry, idem, downstream, time, time);
        mvc = MockMvcBuilders.standaloneSetup(new GovernanceController(client, registry, downstream)).build();

        TargetConfig cfg = new TargetConfig(3, 60_000, 5, 5_000, 2, 60_000, 1,
                2, 1_000, "missing-target", null);
        mvc.perform(put("/governance/targets/svc")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(om.writeValueAsString(cfg)))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.accepted").value(false))
                .andExpect(jsonPath("$.reason").value("FALLBACK_CONFIG_REJECTED"));
    }
}
