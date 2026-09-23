# Outbound Call Governance

为服务对下游依赖的出站调用提供统一治理：按目标的配额与并发上限、超时重试（受耗时预算约束）、
幂等去重、熔断与降级。所有依赖故障均通过内置的 `SimulatedDownstream` 在本地稳定复现，
不依赖外部服务或真实凭据。

## 架构与组件

```
OutboundGateway（统一入口）
  ├─ IdempotencyRegistry   幂等去重：同一键只产生一次实质执行
  └─ GovernanceConfigRegistry（按目标的运行态：配置 + QuotaLimiter + CircuitBreaker）
       ├─ QuotaLimiter      固定窗口配额 + 并发信号量
       └─ CircuitBreaker    CLOSED / OPEN / HALF_OPEN 状态机
DownstreamClient ← SimulatedDownstream（可编程故障：HEALTHY / FAIL_RETRYABLE / FAIL_FATAL / SLOW）
```

单次调用的编排顺序：**幂等去重 → 配额 → 并发 → 熔断许可 → 超时/重试（预算约束）**。
每个判定节点都会打印日志，包含 `requestId`、`target`、`key`、判定依据（如配额用量、
在途并发、连续失败次数）与熔断状态。

## 配额模型

- 配置按调用目标（`target`）隔离，见 `TargetConfig`：`quotaPerWindow` / `windowMillis`
  构成固定窗口配额，`maxConcurrency` 为并发信号量上限。
- 超限拒绝原因可区分（`CallOutcome.rejectionReason`）：
  - `QUOTA_EXHAUSTED`：窗口内配额耗尽；
  - `CONCURRENCY_SATURATED`：并发许可占满；
  - 后续节点拒绝时退回已占配额/许可，避免误计。
- **运行中配置变更**：`OutboundGateway.updateConfig` / `POST /gateway/config/{target}`
  立即生效且只影响对应目标，未涉及的目标不受影响。拒绝原因可区分：
  - `INVALID_CONFIG`：配置值非法（非正数等）；
  - `ROLLBACK_CONFLICT`：值合法但回退到低于当前在途用量（并发低于 in-flight、
    配额低于当前窗口已用量），旧配置保持不变。

## 重试与幂等语义

- **超时**：单次调用受 `callTimeoutMillis` 约束，超时按可重试失败处理。
- **重试**：仅可重试失败（超时、`FAIL_RETRYABLE`）触发重试，最多 `maxAttempts` 次，
  退避为 `backoffMillis * attempt`。
- **耗时预算**：整次请求（含全部重试与退避）受 `retryBudgetMillis` 约束；
  每次发起新尝试前检查剩余预算，预算耗尽即停止，不再发起任何新调用，
  结果记为 `BUDGET_EXHAUSTED`（与 `DOWNSTREAM_FAILURE` 可区分）。
- **幂等**：以 `target + idempotencyKey` 为键，并发或重试的相同键请求只产生一次实质执行：
  - `FRESH_EXECUTION`：本次请求触发了实质执行；
  - `JOINED_IN_PROGRESS`：相同键正在执行中，本次挂接等待同一结果；
  - `REPLAYED_AFTER_COMPLETION`：相同键已完成，直接回放结果。
  重复请求返回与首次执行一致的结果（同一 `requestId` 与返回值），且不再消耗下游调用。

## 熔断状态机

```
            连续失败 ≥ circuitFailureThreshold
 CLOSED  ────────────────────────────────►  OPEN
   ▲                                          │ 冷却 circuitResetMillis 期满
   │            探测成功                        ▼
   └────────────────────────────────────  HALF_OPEN（仅发放 1 个探测许可）
                 探测失败 ──► 回到 OPEN 并重新计时
```

- `OPEN` 期间请求快速失败（`CIRCUIT_OPEN`），不发起下游调用。
- `HALF_OPEN` 是可观察的中间状态（`GET /gateway/state/{target}`），只发放一个探测许可；
  探测在途时其余请求同样快速失败，探测失败重新打开且不放大调用量。
- 状态迁移只由已记录的调用结果驱动，避免临界点反复切换。

## 本地验证

```bash
# 运行全部测试（并发幂等、配额临界与配置变更、超时重试预算、熔断恢复）
mvn test

# 启动服务后通过 REST 验证
mvn spring-boot:run
curl -X POST 'http://localhost:8080/gateway/call?target=pay&key=order-1&payload=x'
curl -X POST 'http://localhost:8080/gateway/config/pay' \
     -H 'Content-Type: application/json' \
     -d '{"quotaPerWindow":100,"windowMillis":60000,"maxConcurrency":10,
          "callTimeoutMillis":2000,"maxAttempts":3,"backoffMillis":100,
          "retryBudgetMillis":10000,"circuitFailureThreshold":3,"circuitResetMillis":5000}'
curl 'http://localhost:8080/gateway/state/pay'
```

故障复现无需外部依赖：测试中通过 `SimulatedDownstream.setMode/setLatency`
按目标注入可重试失败、不可重试失败与挂起超时，并用 `invocationCount` 断言实际调用量。

## 测试覆盖

| 测试类 | 场景 |
| --- | --- |
| `QuotaAndConfigTest` | 配额临界、并发上限、运行中配置变更只影响对应目标、非法配置与回退冲突的可区分拒绝 |
| `IdempotencyConcurrencyTest` | 8 线程并发同键调用仅一次实质执行、结果一致、执行中挂接与已完成回放可区分 |
| `RetryBudgetTest` | 可重试失败重试到上限、不可重试失败不重试、超时重试受预算约束且预算耗尽后不再发起调用 |
| `CircuitBreakerTest` | 连续失败打开熔断、快速失败、半开单探测、探测成功闭合、探测失败重开且不放大调用量 |
