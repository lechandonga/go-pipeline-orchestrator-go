# go-pipeline-orchestrator

纯本地、零第三方依赖的 Go 任务编排与运行观测组件。作业就是进程内的 Go 函数，
依赖关系组成 DAG，由一个单线程协调器（coordinator）驱动调度；不依赖任何
外部服务、队列或编排框架（仅使用标准库）。

## 快速开始

```go
eng := orchestrator.NewEngine(orchestrator.Options{
    Concurrency: 2,                      // 全局并发上限
    RunTimeout:  5 * time.Second,        // 运行级超时（可选）
})
reg := orchestrator.NewRegistry()
reg.MustRegister(orchestrator.JobSpec{Name: "a", Run: fnA})
reg.MustRegister(orchestrator.JobSpec{Name: "b", Deps: []string{"a"}, Run: fnB})

plan, err := eng.Build(reg)              // 执行前校验，不合法直接拒绝
if err != nil { /* *ValidationError */ }

run := plan.NewRun(context.Background())
snap := run.RunAndWait()                 // 或 Start() + 并发 Snapshot() 观测
```

可运行示例：`go run ./cmd/demo`。

## 注册与校验（开始执行前拒绝）

`Engine.Build` 在启动任何作业之前完成全部校验，错误为 `*ValidationError`，
通过 `Reason` 字段区分原因：

| Reason | 触发条件 |
| --- | --- |
| `duplicate_job` | 同名作业重复注册（在 `Register` 阶段返回） |
| `unknown_dependency` | `Deps` 引用了未注册的作业，或同一依赖重复声明 |
| `self_dependency` | 作业依赖自身 |
| `cycle_detected` | DFS 三色标记检测到依赖环，错误信息包含环路径 |
| `job_already_built` | `Build` 之后再向注册表追加作业 |

校验失败时不会创建运行，更不会执行任何作业。

## 作业状态机

```
pending ──所有依赖成功──▶ ready ──获得并发槽──▶ running ──成功──▶ succeeded
   │                        ▲      │  ▲   │
   │                        │      │  │   ├──不可重试错误 / 重试耗尽──▶ failed
   │(上游失败/取消)          │      │  │   ├──可重试错误──▶ (退避) ──┘
   │                        └──────┘  └──运行取消/超时且作业在执行──▶ canceled
   └──────────────────────────────────────────────▶ skipped（从未启动）
```

- `running` 包含“正在执行”与“退避等待重试”两段；退避结束后作业重新进入
  `ready` 队列，重新竞争并发槽。
- 一个作业失败/取消后，从该作业出发 BFS：所有**尚未启动**的下游标记为
  `skipped` 并记录直接原因（`skipped: upstream "x" did not succeed`）；
  与失败点无依赖关系的分支不受影响，继续正常执行。
- `skipped` 作业的执行体保证一次都不会被调用。

运行状态：`pending → running → succeeded | failed | canceled | timed_out`。

终态判定优先级：`timed_out`（超时标志或 ctx DeadlineExceeded）> `canceled`
（ctx 被取消）> `failed`（存在 failed/canceled 作业）> `succeeded`。

## 重试与退避语义

- 作业返回普通 `error`：**不可重试**，立即 `failed`，即使配置了 `MaxAttempts`。
- 返回 `orchestrator.Retryable(err)`：可重试，按 `MaxAttempts`（含首次）重试。
- `context.Canceled` / `context.DeadlineExceeded` 即使被 `Retryable` 包装也
  **绝不重试**——运行正在终止，重试没有意义。
- 退避：`RetryBackoff * 2^(n-1)`（指数），可叠加 `BackoffJitter`（0~0.9 的
  对称抖动），避免并发重试齐步唤醒；退避期间运行被取消会立即收尾。
- **副作用只生效一次**：重试意味着执行体会被多次调用，有副作用的步骤要用
  `orchestrator.NewEffectGuard()` 包裹，首次执行后后续重试复用同一结果：

```go
guard := orchestrator.NewEffectGuard() // 通常在闭包外创建
run := func(ctx context.Context, attempt int) error {
    if err := guard.Do(func() error { charge(); return nil }); err != nil { return err }
    return doIdempotentRemoteCall(ctx) // 其余步骤设计成可安全重放
}
```

## 取消与超时语义

- `Run.Cancel()` 或父 context 取消、或达到 `RunTimeout`，运行 context 被取消。
- 作业执行体通过入参 `ctx` 感知终止，必须在 `select`/IO 中尊重 `ctx.Done()`；
  引擎无法强杀不响应 ctx 的 goroutine（Go 的通用约束）。
- 终止发生后：已在执行（含退避中）的作业收敛为 `canceled`，从未启动的作业
  为 `skipped`；协调器 drain 完所有在途作业后关闭 `Done()`，运行必然进入
  `canceled` 或 `timed_out`，不会停留在 `running`。

## 并发调度

- `Options.Concurrency` 限制同时执行的作业数；ready 队列按**注册顺序**取槽，
  先满足依赖的作业先入队，因此启动顺序可解释、可复现。
- 所有作业状态转移集中在协调器单个 goroutine 完成，作业 goroutine 只通过
  事件通道回报，状态机天然无竞争。

## 观测口径（并发查询自洽）

`Run.Snapshot()` 返回某一时刻的值拷贝快照，所有字段在同一把 RWMutex 读锁下
取得，多个 goroutine 并发查询不会看到“计数已变但作业状态未变”的中间态：

- 作业口径：`State`、`Attempts`（实际发起次数）、`Retries = Attempts-1`、
  `Duration`（首次启动→终态；未结束时为→快照时刻）、起止时间、最后错误。
- 运行口径：`Succeeded/Failed/Canceled/Skipped/Running/Pending` 分桶之和恒等于
  `Total`；各作业 `Attempts` 之和 = `TotalAttempts`；`Retries` 之和 =
  `TotalRetries`。测试会在 6 个查询 goroutine 并发下持续断言这些不变量。
- `FailureReport` 给出因果解释：根因失败作业、被取消的作业、以及被波及跳过
  的下游各自的直接原因。

## 验证方法

```bash
go test ./...                 # 常规运行
CGO_ENABLED=1 go test -race -count=1 ./...   # 竞态检测
go test -v ./orchestrator     # 查看每个业务用例及引擎业务日志
go run ./cmd/demo             # 端到端示例（含重试与失败分支隔离）
```

测试覆盖的业务：

| 业务 | 用例 |
| --- | --- |
| 注册校验 | `TestValidationRejectedBeforeExecution`（重复/未知依赖/自依赖/两种环）、`TestRegisterAfterBuildRejected` |
| 依赖失败与分支隔离 | `TestDependencyFailureIsolatesBranches`（失败原因 + 跳过级联 + 独立分支照常成功 + 报告） |
| 重试/退避/幂等 | `TestRetryBackoffAndIdempotentEffect`、`TestRetryExhaustedMarksFailed`、`TestContextErrorNeverRetried` |
| 取消与超时 | `TestCancelStopsAllRunningJobs`、`TestRunTimeout`、`TestTimeoutDuringBackoff` |
| 并发调度 | `TestConcurrencyLimitAndOrder`、`TestConcurrencyPeakNeverExceedsLimit` |
| 并发观测自洽 | `TestConcurrentSnapshotsAreConsistent` |

## 代码结构

- `orchestrator/registry.go`：注册与 DAG 校验（未知依赖 / 自依赖 / 环）。
- `orchestrator/engine.go`：引擎、选项、运行生命周期入口。
- `orchestrator/coordinator.go`：单线程协调器（调度、重试、级联、终态收敛）。
- `orchestrator/snapshot.go`：自洽快照、失败因果报告、退避计算。
- `orchestrator/errors.go`：可区分的校验错误与可重试错误包装。
- `orchestrator/effect.go`：一次性副作用守卫。
- `cmd/demo`：端到端示例。
