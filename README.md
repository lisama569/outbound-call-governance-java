# 出站调用治理（Outbound Call Governance）

为服务对下游依赖的出站调用提供统一治理能力：**配额与并发上限、超时与预算受限重试、
幂等去重、熔断与降级**。目标是在并发与依赖故障下给出稳定、可解释的结果，并使依赖故障
能在本地稳定复现——**不依赖任何外部服务或真实凭据**。

- Java 21 / Spring Boot 3.4，核心治理逻辑为纯 Java 组件，可脱离容器单测。
- 时间与睡眠均可注入（`TimeSource` / `Sleeper`），测试用 `VirtualClock` 确定性推进时间。
- 下游依赖由 `SimulatedDownstream` 在本地模拟（OK / 超时 / 连续失败 / 前 N 次失败 / 慢调用）。

---

## 1. 总体链路

`GovernanceClient.call(target, ctx)` 对每次调用依次执行：

```
幂等去重 ──> 熔断入口前检 ──> 每尝试：配额/并发申请 ──> 超时调用
                    ▲                                      │
                    └──── 退避（受预算约束）<── 可重试失败 ──┘
失败/被拒绝 ──> 静态回退值 / 回退目标链（受配置校验与深度上限保护）
```

每次关键判定都会打印日志，统一包含**请求标识（requestId）**、**判定依据**与**熔断状态**。

---

## 2. 配置模型（按调用目标）

`TargetConfig`（每个调用目标一份，字段均强校验）：

| 字段 | 含义 | 约束 |
| --- | --- | --- |
| `quotaPerWindow` | 每个固定时间窗允许的**真实调用数** | > 0 |
| `windowMillis` | 配额固定窗口长度（毫秒） | > 0 |
| `maxConcurrency` | 同目标最大并发真实调用数 | > 0 |
| `timeoutMillis` | 单次下游调用超时（毫秒） | > 0 |
| `maxAttempts` | 最大尝试次数（含首次） | > 0 |
| `budgetMillis` | 重试总耗时预算（毫秒） | > 0 |
| `backoffMillis` | 首次退避基数（指数退避） | >= 0 |
| `failureThreshold` | 连续失败多少次后打开熔断 | > 0 |
| `openWaitMillis` | OPEN 后多久允许一次恢复探测 | > 0 |
| `fallbackTarget` | 回退目标名（可空） | 必须存在且不成环 |
| `fallbackValue` | 静态回退值（非空时优先于回退目标） | 任意 |

### 运行中变更

- `PUT /governance/targets/{name}` 或 `GovernanceRegistry.updateTarget` 在运行中新增/更新单个目标。
- 变更**只作用于对应目标**（`ConcurrentHashMap.compute` 原子替换该目标的配置引用并转发给
  配额与熔断组件），未涉及目标完全不受影响。
- 校验失败时**原子拒绝、保留旧配置**，并返回 `ConfigUpdateResult{accepted=false, reason, detail}`。

### 可区分的拒绝原因（`RejectReason`）

| 原因 | 触发场景 |
| --- | --- |
| `UNKNOWN_TARGET` | 调用了未配置治理规则的目标 |
| `INVALID_CONFIG` | 配置字段缺失/非法（如配额为 0） |
| `FALLBACK_CONFIG_REJECTED` | 回退目标不存在、回退到自身、回退链成环 |
| `QUOTA_EXCEEDED` | 时间窗配额用尽 |
| `CONCURRENCY_LIMIT` | 并发上限用尽（与配额拒绝可区分） |
| `CIRCUIT_OPEN` | 熔断打开（或 HALF_OPEN 期间非探测请求）快速失败 |
| `RETRY_BUDGET_EXHAUSTED` | 重试耗时预算耗尽，不再发起调用 |
| `DOWNSTREAM_FAILURE` | 下游失败（不可重试，或次数用尽，或熔断后停止） |
| `FALLBACK_UNAVAILABLE` | 回退链运行期不可用（目标缺失/链失败/超深度） |

---

## 3. 配额与并发模型

- **固定窗配额**：窗口在第一次申请时惰性开启，跨窗计数清零；窗口内最多放行 `quotaPerWindow` 次。
- **判定顺序：先配额、后并发**。
  - 配额已满 → `QUOTA_EXCEEDED`；
  - 拿不到并发许可 → `CONCURRENCY_LIMIT`（**不消耗配额**，因为未发生真实调用）。
- 并发由**公平信号量**（`AdjustableSemaphore`）控制，许可数可在运行中调整：
  `drainPermits()` 后按新上限重新发放，进行中调用不被抢占，新上限对后续申请立即生效。
- 每次真实尝试（含每次重试）都独立申请/释放配额与并发许可；凭证用 try-with-resources 保证归还。
- 熔断打开期间的快速失败不申请配额，因此不会消耗真实调用额度。

---

## 4. 重试与超时语义

- 调用在独立线程执行，用 `Future.get(timeoutMillis)` 强制超时；超时后取消任务并抛出
  `DownstreamTimeoutException`，**不会无限等待下游**。
- **仅对可重试故障重试**：`DownstreamTimeoutException`（超时）与
  `DownstreamFailureException`（如 5xx / 连接重置）。其他异常视为不可重试，立即失败。
- **指数退避**：第 n 次重试退避 `backoffMillis * 2^(n-1)`。
- **重试总量受耗时预算 `budgetMillis` 硬约束**：
  - 每次尝试前检查剩余预算；当剩余预算连下一次退避都放不下时立即停止，返回
    `RETRY_BUDGET_EXHAUSTED`，**预算耗尽后不再发起任何调用**；
  - 若预算充足但尝试次数达到 `maxAttempts`，返回 `DOWNSTREAM_FAILURE`。两种终止原因可区分。
  - 单次等待时间取 `min(timeoutMillis, 剩余预算)`，保证最后一次尝试也不会越过总预算。
- 任意一次失败把熔断器打开后，重试循环立即停止；退避后仅**被动观察**熔断状态，
  OPEN→HALF_OPEN 的恢复探测只能由一次**全新调用**发起。

---

## 5. 幂等语义

维度为 `target + idempotencyKey`（`IdempotencyStore`）：

- 同一键的并发/重试请求，第一个到达的线程成为 **LEADER**，唯一真正执行下游调用；
  其余线程为 **IN_FLIGHT**，在条目监视器上等待，**绝不二次执行**。
- LEADER 成功（含回退成功）后写入不可变结果并唤醒所有等待者，后来者得到 **COMPLETED**
  与**一致结果**；跟随者保留各自的 requestId，便于追踪。
- 可明确区分**执行中 vs 已完成**：`IN_FLIGHT` 暴露持有者线程名（日志打印 holder），
  `COMPLETED` 携带缓存结果；`peek` 提供非阻塞可观察查询。
- LEADER 最终失败（无回退）时调用 `release` 删除占位，重复请求不被失败结果毒化，
  后续重试可重新执行。
- 完成结果保留 TTL（默认 60s，可配置），过期后允许重新执行。

---

## 6. 熔断状态机与降级

```
                 连续失败 >= failureThreshold
        CLOSED ───────────────────────────────► OPEN
          ▲                                        │
          │ 探测成功                                │ 经过 openWaitMillis，
          │                                        ▼ 首个调用原子进入
          └─────────────────────────────────── HALF_OPEN
              探测失败则 HALF_OPEN ──► OPEN（重新计时）
```

- **CLOSED**：正常放行；连续失败达阈值 → OPEN。任何成功清零连续失败计数。
- **OPEN**：所有调用**快速失败**（`CIRCUIT_OPEN`），不发起下游调用、不消耗配额。
- **HALF_OPEN（可观察中间状态）**：等待期到达后，**第一个**调用原子进入 HALF_OPEN 并成为
  **唯一探测**；同一时刻其他请求仍快速失败，因此**探测不会放大调用量**。
  - 探测成功 → CLOSED（计数清零）；
  - 探测失败 → OPEN 并**重新计时**。刚重开时立即再来的请求仍被拒绝，
    状态迁移只由单调时间与受锁保护的判定驱动，**不会在临界点反复切换**。
- 状态迁移通过 `TransitionListener` 与日志暴露（`CLOSED -> OPEN -> HALF_OPEN -> CLOSED`）。
- **降级**：失败或被拒绝后，按配置返回静态 `fallbackValue`，或转发到 `fallbackTarget`
  （回退目标本身也受完整治理）。回退结果标记为 `CallStatus.FALLBACK` 且 `fallback=true`，
  与真实成功可区分；回退链有最大深度（`MAX_FALLBACK_DEPTH=4`）兜底，成环在配置层即被拒绝。

---

## 7. 本地验证方法

### 7.1 运行测试（确定性，离线，无外部服务/凭据）

```bash
mvn -o test
```

测试分布：

| 测试类 | 覆盖场景 |
| --- | --- |
| `QuotaStateTest` | 配额临界、配额与并发原因区分、运行中配置变更立即生效 |
| `CircuitBreakerTest` | 打开快速失败、可观察 HALF_OPEN、唯一探测、探测成败、防抖动 |
| `IdempotencyStoreTest` | 单 LEADER、执行中/已完成区分、一致结果、失败释放、TTL |
| `GovernanceRegistryTest` | INVALID_CONFIG / FALLBACK_CONFIG_REJECTED、回退环、单目标原子变更 |
| `RetryBudgetTest` | 可重试失败退避后成功、预算耗尽停止、次数用尽原因区分 |
| `TimeoutTest` | 真实慢下游（睡 300ms）+ 80ms 超时 + 200ms 预算，稳定复现超时 |
| `ConcurrentIdempotencyTest` | 8 线程同一幂等键：仅一次真实执行、结果一致、执行中可观察 |
| `QuotaConcurrencyConfigTest` | 20 线程配额临界、运行中变更只影响对应目标、非法变更保留旧限 |
| `CircuitRecoveryFallbackTest` | OPEN→HALF_OPEN→CLOSED 恢复、探测失败重开、静态值/回退目标降级 |
| `LoggingObservabilityTest` | 断言日志含 requestId、判定依据、熔断状态，原因可区分 |
| `GovernanceControllerIT` | REST 端点冒烟：配置/非法配置/行为/调用/熔断状态查询 |

### 7.2 手工 HTTP 验证

```bash
mvn -o spring-boot:run
```

```bash
# 1) 新增/更新目标配置（配额3，并发5，超时5s，最多2次尝试，熔断阈值2，等待1s）
curl -s -X PUT localhost:8080/governance/targets/svc \
  -H 'Content-Type: application/json' \
  -d '{"quotaPerWindow":3,"windowMillis":60000,"maxConcurrency":5,"timeoutMillis":5000,
       "maxAttempts":2,"budgetMillis":60000,"backoffMillis":1,"failureThreshold":2,
       "openWaitMillis":1000,"fallbackTarget":null,"fallbackValue":null}'

# 2) 让该目标的本地模拟下游永远失败（也可用 OK / TIMEOUT / FAIL_FIRST_N / SLOW）
curl -s -X POST localhost:8080/governance/targets/svc/behavior \
  -H 'Content-Type: application/json' -d '{"mode":"FAIL"}'

# 3) 发起调用（连续两次失败后熔断打开，随后快速失败）
curl -s -X POST localhost:8080/governance/targets/svc \
  -H 'Content-Type: application/json' -d '{"requestId":"req-demo-1","payload":"hi"}'

# 4) 查看熔断状态（CLOSED / OPEN / HALF_OPEN）与真实下游调用次数
curl -s localhost:8080/governance/circuit/svc
curl -s localhost:8080/governance/invocations/svc
```

非法配置示例（返回 400 与 `INVALID_CONFIG`）：

```bash
curl -s -X PUT localhost:8080/governance/targets/bad \
  -H 'Content-Type: application/json' \
  -d '{"quotaPerWindow":0,"windowMillis":60000,"maxConcurrency":5,"timeoutMillis":5000,
       "maxAttempts":2,"budgetMillis":60000,"backoffMillis":1,"failureThreshold":2,
       "openWaitMillis":1000}'
```

回退到不存在目标（返回 `FALLBACK_CONFIG_REJECTED`，区别于 `INVALID_CONFIG`）：

```bash
curl -s -X PUT localhost:8080/governance/targets/svc \
  -H 'Content-Type: application/json' \
  -d '{"quotaPerWindow":3,"windowMillis":60000,"maxConcurrency":5,"timeoutMillis":5000,
       "maxAttempts":2,"budgetMillis":60000,"backoffMillis":1,"failureThreshold":2,
       "openWaitMillis":1000,"fallbackTarget":"ghost"}'
```

---

## 8. 主要代码位置

| 关注点 | 类 |
| --- | --- |
| 治理编排门面 | `governance/GovernanceClient` |
| 目标注册表 / 配置校验 / 运行中变更 | `governance/GovernanceRegistry`、`governance/TargetState` |
| 配额 + 并发 | `quota/QuotaState`（含 `AdjustableSemaphore`） |
| 熔断状态机 | `circuit/CircuitBreaker` |
| 幂等去重 | `idempotency/IdempotencyStore` |
| 结果/原因/配置模型 | `model/*`（`CallOutcome`、`RejectReason`、`TargetConfig` 等） |
| 时间抽象 | `time/TimeSource`、`time/Sleeper`、`time/SystemTimeSource` |
| 本地故障复现 | `simulation/SimulatedDownstream` |
| 手工验证端点 | `web/GovernanceController` |
