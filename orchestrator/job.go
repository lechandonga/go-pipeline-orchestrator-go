package orchestrator

import (
	"context"
	"time"
)

// RunFunc 是作业执行体。
//
// ctx: 运行级上下文，取消/超时会关闭 Done；作业必须尊重 ctx 才能及时停下来。
// attempt: 从 1 开始的执行序号（重试时递增）。
//
// 返回 nil 表示成功；返回 Retryable(err) 表示可重试失败；返回普通 error 表示
// 不可重试失败。返回 context 取消/超时相关错误时不会重试。
type RunFunc func(ctx context.Context, attempt int) error

// JobSpec 是作业的注册描述。
type JobSpec struct {
	Name         string        // 作业名，运行内唯一
	Deps         []string      // 前置作业名，所有前置成功后本作业才可启动
	Run          RunFunc       // 执行体
	MaxAttempts  int           // 最大尝试次数（含首次）；<=0 表示不重试，即 1 次
	RetryBackoff time.Duration // 重试初始退避，后续按 2 倍指数增长，受 jitter 扰动
}

// JobState 是单个作业在运行中的生命周期状态。
type JobState string

const (
	JobPending   JobState = "pending" // 等待依赖
	JobReady     JobState = "ready"   // 依赖已满足，等待并发槽
	JobRunning   JobState = "running" // 正在执行（含退避等待）
	JobSucceeded JobState = "succeeded"
	JobFailed    JobState = "failed"   // 尝试次数耗尽或返回不可重试错误
	JobSkipped   JobState = "skipped"  // 上游失败/取消，本作业从未启动
	JobCanceled  JobState = "canceled" // 已拿到并发槽并开始执行，期间被取消
)

// RunState 是整次运行的状态。
type RunState string

const (
	RunPending   RunState = "pending" // 已创建，尚未 Start
	RunRunning   RunState = "running"
	RunSucceeded RunState = "succeeded" // 所有作业成功
	RunFailed    RunState = "failed"    // 存在失败（含被跳过/取消的下游）
	RunCanceled  RunState = "canceled"  // 被外部 Cancel 中断
	RunTimedOut  RunState = "timed_out" // 超过运行级超时时间
)

// JobResult 是单个作业在某一时刻的自洽快照（值拷贝，调用方可自由持有）。
type JobResult struct {
	Name      string
	State     JobState
	Attempts  int           // 实际发起的执行次数
	Retries   int           // Attempts - 1（仅在终态时有意义）
	Duration  time.Duration // 从首次启动到终态的墙钟耗时（pending 时为 0）
	StartedAt time.Time
	EndedAt   time.Time
	LastError string
	DependsOn []string
}

// RunSnapshot 是整次运行在某一时刻的自洽快照。所有字段在同一把锁下取得，
// 并发查询时不会看到“计数已变但作业状态未变”之类的中间态。
type RunSnapshot struct {
	RunID         int64
	State         RunState
	Jobs          []JobResult // 按注册顺序
	Total         int
	Succeeded     int
	Failed        int // state == failed
	Skipped       int
	Canceled      int
	Running       int
	Pending       int // pending + ready
	TotalRetries  int
	TotalAttempts int
	StartedAt     time.Time
	EndedAt       time.Time
	Duration      time.Duration // StartedAt 到 EndedAt（未结束时为到快照时刻）
	FailureReport string        // 人类可读的失败因果说明（哪些根因、哪些被波及）
}
