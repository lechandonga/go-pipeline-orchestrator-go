# 本地任务编排与运行观测

纯本地、零外部依赖（仅标准库）的作业编排器：注册作业、声明依赖、受限并发执行、
失败重试、取消/超时，以及并发安全的运行观测。不依赖任何第三方编排框架或外部服务。

## 快速开始

```go
registry := orchestrator.NewRegistry()

registry.MustRegister(orchestrator.Job{
    Name: "extract",
    Handler: func(ctx context.Context, exec *orchestrator.Execution) error {
        _, err := exec.Effects().Once("extract", func() (any, error) {
            return data, nil // 仅在成功时缓存，重试不会重复生效
        })
        return err
    },
})

registry.MustRegister(orchestrator.Job{
    Name: "load", DependsOn: []string{"extract"},
    MaxAttempts: 3,
    Backoff: orchestrator.ExponentialBackoff(10*time.Millisecond, time.Second),
    Handler: loadHandler, // handler 必须响应 ctx.Done()
})

run, err := registry.Run(ctx, orchestrator.RunOptions{
    ID: "daily-load", Concurrency: 4, Timeout: 30 * time.Second,
})
// err 非 nil 表示“执行前”的 DAG 校验失败；此时没有任何作业被启动。
snapshot := run.Snapshot()
```

## 校验（执行前拒绝，原因可区分）

`Register` / `Run` 在启动任何 goroutine 前完成全部校验，错误可用 `errors.Is` 区分：

| 场景 | 哨兵错误 |
| --- | --- |
| 同名作业重复注册 | `ErrDuplicateJob` |
| 依赖指向不存在的作业（含自依赖） | `ErrUnknownDependency` |
| 依赖成环 | `ErrDependencyCycle`（Kahn 算法，`ValidationError.JobIDs` 给出环上/被环阻塞的作业） |
| 空注册表 | `ErrNoJobs` |

多个问题同时存在时 `Validate()` 返回全部错误；`Run` 返回第一个。

## 状态机

作业状态（`JobStatus`）：

```
pending ──> queued ──> running ──────────────> succeeded
                           │  ^                 │
                           v  │                 │ 失败：下游全部
                        retrying                v      skipped
                           │                    failed
                           └──(达到 MaxAttempts / 不可重试)

任意非终态 ──(取消/超时)──> cancelled
```

- `pending`：尚未满足依赖或未获得并发槽。
- `queued`：已获得并发槽、即将执行（同批就绪作业按名称字典序派发，顺序可解释）。
- `running`：某次尝试正在执行；`Attempts` 为当前尝试序号。
- `retrying`：尝试失败、正在等待退避；`Retries` 加一，退避在 `RetryWaits` 中留痕。
- 终态：`succeeded` / `failed` / `skipped` / `cancelled`。
- `skipped` 是传递性的：上游 `failed` 后，其全部直接/间接下游标记为 skipped，
  `SkippedReason` 说明因哪个上游失败；与失败无关的分支继续正常执行。

运行状态（`RunStatus`）：`running` → `succeeded`（全部成功）/
`failed`（至少一个 failed 或 skipped）/ `cancelled`（因 `Cancel()`、父 ctx 取消
或运行级 `Timeout` 而中止）。终态不可逆，再次 `Cancel()` 返回 `ErrAlreadyFinished`。

## 重试与副作用语义

- 默认错误可重试；`orchestrator.NonRetryable(err)` 包装的错误立即失败，不再退避。
  handler panic 被 recover 并视为不可重试错误（进程不崩溃）。
- 尝试次数上限为 `Job.MaxAttempts`（含首次）；`Job.Backoff` 支持
  `ConstantBackoff`、`ExponentialBackoff(base, max)` 或自定义 `Backoff`。
  退避针对的是**第 N 次失败之后、第 N+1 次尝试之前**的等待。
- 退避期间继续占用该作业的并发槽（避免“失败作业”无限挤占其他作业的语义歧义）；
  在退避中取消会立即终止，不会发起新尝试。
- 副作用去重：`Execution.Effects().Once(key, fn)` 仅在 `fn` **成功返回**时缓存结果；
  失败不缓存，允许下一次尝试重做；成功之后即使作业继续重试也直接复用，
  保证“已经生效的执行不重复产生副作用”。
- 成功作业可用 `exec.SetOutput(v)` 产出结果，下游通过 `exec.Result(name)` 读取。

## 取消与超时语义

- `RunOptions.Timeout` 是整次运行的截止时间；`Job.Timeout` 是单次尝试的截止时间
  （尝试超时返回 `context.DeadlineExceeded`，默认可重试，是否重试由 `MaxAttempts` 决定）。
- 父 `context.Context` 取消、`run.Cancel()`、运行超时三者等价，都会取消运行 ctx。
- 取消生效后：所有在执行的 handler 通过 ctx 收到 `Done()`（handler 必须及时返回）；
  已在排队的作业不再启动，标记 `cancelled`；退避中的 sleep 立即唤醒并终止；
  取消之后到达的成功结果不会解锁新作业。
- dispatcher 会排空被取消 worker 的终态消息，保证不会有 goroutine 泄漏，
  运行一定会进入终态，不会永久停留在 `running`。

## 观测口径

`run.Snapshot()` 在**同一把 RWMutex 读锁**下复制运行状态、每个作业的明细
（状态、尝试次数、重试次数、累计耗时、退避序列、最后错误、跳过原因、产出）与
副作用表，因此同一次查询拿到的一定是同一个时间点的自洽视图：

- `Stats` 恒等式：`Pending+Queued+Running+Retrying+Succeeded+Failed+Skipped+Cancelled == Total`。
- `Retries` 是所有作业重试次数之和；`Duration` 为各次尝试耗时之和（运行级耗时另见
  `Snapshot.Duration`）。
- 计数与作业明细在同一临界区读取，并发执行与并发查询下不会观察到中间态。
- `run.Events()` 返回按时间排序的状态转移日志（`queued`、`attempt_started`、
  `retry_scheduled`、`succeeded`、`failed`、`skipped`、`cancelled`），
  也可通过 `RunOptions.ErrEventHandler` 实时订阅。

## 验证方法

```bash
go test -race -count=1 ./...        # 竞态检测
go test -race -count=10 ./...       # 稳定性重复运行
go test -v ./...                    # 查看业务覆盖与用例日志
```

测试按业务分组，日志中会打印：

- `===== 业务覆盖: xxx =====`：覆盖的业务域（DAG 校验、依赖失败与分支隔离、
  重试退避与副作用去重、取消/超时、并发度与顺序、并发观测一致性等）。
- `[PASS] 用例: ... => 结果`：每个用例验证的场景与观测到的终态。

主要测试：

| 测试 | 覆盖业务 |
| --- | --- |
| `TestValidateRejectsBadDAGsBeforeExecution` | 重复注册/未知依赖/自依赖/成环/空注册表 |
| `TestDependencyFailureSkipsOnlyDownstream` | 失败传递跳过、无关分支继续、运行级 failed 可解释 |
| `TestDependencyOutputFlowsToDependents` | 上游产出下游消费 |
| `TestRetryWithBackoffAndEffectOnce` | 指数退避、尝试/重试计数、副作用仅一次 |
| `TestFailedSideEffectRetriedButSuccessNotRepeated` | 失败副作用可重试、成功不重复 |
| `TestNonRetryableFailsImmediately` / `TestPanicIsNonRetryableFailure` | 不可重试区分、panic 安全 |
| `TestCancelStopsRunningAndQueuedJobs` / `TestCancelDuringBackoffStopsRetry` | 取消停止执行中/排队中/退避中作业 |
| `TestRunTimeoutDoesNotHang` / `TestJobTimeoutIsRetryable` | 运行级超时终态、作业级超时重试 |
| `TestConcurrencyLimitAndOrderIsExplainable` | 最大并发不超限、字典序启动、依赖顺序 |
| `TestConcurrentSnapshotsAreSelfConsistent` | 6 个查询者并发轮询，校验计数恒等式与明细一致性 |
