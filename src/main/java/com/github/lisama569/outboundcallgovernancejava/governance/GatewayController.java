package com.github.lisama569.outboundcallgovernancejava.governance;

import org.springframework.web.bind.annotation.*;

import java.util.Map;
import java.util.Optional;

/**
 * 治理能力的 REST 入口，便于本地验证。
 */
@RestController
@RequestMapping("/gateway")
public class GatewayController {

    private final OutboundGateway gateway;

    public GatewayController(OutboundGateway gateway) {
        this.gateway = gateway;
    }

    @PostMapping("/call")
    public CallOutcome call(@RequestParam String target,
                            @RequestParam String key,
                            @RequestParam(defaultValue = "") String payload) {
        return gateway.call(target, key, payload);
    }

    @PostMapping("/config/{target}")
    public Map<String, Object> updateConfig(@PathVariable String target,
                                            @RequestBody TargetConfig config) {
        Optional<ConfigRejection> rejection = gateway.updateConfig(target, config);
        return rejection.<Map<String, Object>>map(r -> Map.of(
                        "accepted", false,
                        "reason", r.reason().name(),
                        "message", r.message()))
                .orElse(Map.of("accepted", true));
    }

    @GetMapping("/state/{target}")
    public Map<String, Object> state(@PathVariable String target) {
        return Map.of("target", target,
                "circuitState", gateway.circuitStateOf(target).name());
    }
}
