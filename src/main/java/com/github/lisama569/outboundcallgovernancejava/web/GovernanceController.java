package com.github.lisama569.outboundcallgovernancejava.web;

import com.github.lisama569.outboundcallgovernancejava.governance.CallContext;
import com.github.lisama569.outboundcallgovernancejava.governance.GovernanceClient;
import com.github.lisama569.outboundcallgovernancejava.governance.GovernanceRegistry;
import com.github.lisama569.outboundcallgovernancejava.model.CallOutcome;
import com.github.lisama569.outboundcallgovernancejava.model.CircuitState;
import com.github.lisama569.outboundcallgovernancejava.model.ConfigUpdateResult;
import com.github.lisama569.outboundcallgovernancejava.model.TargetConfig;
import com.github.lisama569.outboundcallgovernancejava.simulation.SimulatedDownstream;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

/**
 * 本地手工验证端点（无需外部服务或真实凭据）：
 * <ul>
 *   <li>PUT  /governance/targets/{name}            运行中新增/更新单个目标配置</li>
 *   <li>POST /governance/targets/{name}/behavior   配置该目标的模拟下游行为</li>
 *   <li>POST /governance/call/{target}             发起一次被治理的出站调用</li>
 *   <li>GET  /governance/circuit/{target}          查看熔断状态</li>
 *   <li>GET  /governance/invocations/{target}      查看真实下游调用次数</li>
 * </ul>
 */
@RestController
@RequestMapping("/governance")
public class GovernanceController {

    private final GovernanceClient client;
    private final GovernanceRegistry registry;
    private final SimulatedDownstream downstream;

    public GovernanceController(GovernanceClient client,
                                GovernanceRegistry registry,
                                SimulatedDownstream downstream) {
        this.client = client;
        this.registry = registry;
        this.downstream = downstream;
    }

    @PutMapping("/targets/{name}")
    public ResponseEntity<ConfigUpdateResult> updateTarget(@PathVariable String name,
                                                           @RequestBody TargetConfig config) {
        ConfigUpdateResult result = registry.updateTarget(name, config);
        return result.accepted() ? ResponseEntity.ok(result) : ResponseEntity.badRequest().body(result);
    }

    @PostMapping("/targets/{name}/behavior")
    public Map<String, String> setBehavior(@PathVariable String name,
                                           @RequestBody BehaviorRequest request) {
        SimulatedDownstream.Behavior behavior = toBehavior(request);
        downstream.setBehavior(name, behavior);
        return Map.of("target", name, "behavior", request.mode() == null ? "OK" : request.mode());
    }

    @PostMapping("/call/{target}")
    public CallOutcome call(@PathVariable String target, @RequestBody(required = false) CallRequest request) {
        CallRequest req = request == null
                ? new CallRequest(null, null, null)
                : request;
        String requestId = req.requestId() == null || req.requestId().isBlank()
                ? "req-" + UUID.randomUUID()
                : req.requestId();
        String key = req.idempotencyKey() == null || req.idempotencyKey().isBlank()
                ? null
                : req.idempotencyKey();
        return client.call(target, new CallContext(requestId, key, req.payload()));
    }

    @GetMapping("/circuit/{target}")
    public ResponseEntity<Map<String, String>> circuit(@PathVariable String target) {
        return registry.get(target)
                .map(s -> {
                    Map<String, String> body = new LinkedHashMap<>();
                    CircuitState st = s.breaker().currentState();
                    body.put("target", target);
                    body.put("state", st.name());
                    body.put("consecutiveFailures", String.valueOf(s.breaker().consecutiveFailures()));
                    return ResponseEntity.ok(body);
                })
                .orElseGet(() -> ResponseEntity.status(404).body(Map.of("target", target, "state", "UNKNOWN_TARGET")));
    }

    @GetMapping("/invocations/{target}")
    public Map<String, Object> invocations(@PathVariable String target) {
        return Map.of("target", target, "invocations", downstream.invocationCount(target));
    }

    private static SimulatedDownstream.Behavior toBehavior(BehaviorRequest request) {
        String mode = request.mode() == null ? "OK" : request.mode().trim().toUpperCase();
        long latency = request.latencyMillis() == null ? 0L : request.latencyMillis();
        int n = request.failFirstN() == null ? 0 : request.failFirstN();
        return switch (mode) {
            case "TIMEOUT" -> SimulatedDownstream.Behavior.timeout(latency);
            case "FAIL" -> SimulatedDownstream.Behavior.fail();
            case "FAIL_FIRST_N" -> SimulatedDownstream.Behavior.failFirstN(n);
            case "SLOW" -> SimulatedDownstream.Behavior.slow(latency);
            default -> SimulatedDownstream.Behavior.ok();
        };
    }
}
