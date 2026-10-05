package orchestrator

import (
	"context"
	"log"
	"math/rand"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// Logger 是引擎使用的最小日志接口，*log.Logger 默认满足。
type Logger interface {
	Printf(format string, v ...any)
}

// Options 控制引擎行为。
type Options struct {
	Concurrency   int           // 全局最大并发执行作业数；<=0 表示不限
	RunTimeout    time.Duration // 每次运行的整体超时；<=0 表示不超时
	BackoffJitter float64       // 退避抖动比例，范围 [0,1)，0 关闭抖动
	Logger        Logger        // 为空时使用 stderr 默认 logger
	Now           func() time.Time
}

// Engine 是本地编排引擎：所有调度均在进程内完成，无外部依赖。
type Engine struct {
	opts   Options
	logger Logger
	now    func() time.Time
	nextID atomic.Int64
	rngMu  sync.Mutex
	rng    *rand.Rand
}

// NewEngine 创建引擎。
func NewEngine(opts Options) *Engine {
	lg := opts.Logger
	if lg == nil {
		lg = log.New(os.Stderr, "[orchestrator] ", log.LstdFlags|log.Lmicroseconds)
	}
	now := opts.Now
	if now == nil {
		now = time.Now
	}
	return &Engine{
		opts:   opts,
		logger: lg,
		now:    now,
		rng:    rand.New(rand.NewSource(now().UnixNano())),
	}
}

// Build 校验注册表并生成可运行的 Plan。校验失败直接返回 *ValidationError，
// 此时不会开始任何执行。
func (e *Engine) Build(r *Registry) (*Plan, error) {
	r.mu.Lock()
	if err := r.validate(); err != nil {
		r.mu.Unlock()
		return nil, err
	}
	r.built = true
	specs := make([]*JobSpec, len(r.specs))
	copy(specs, r.specs)
	r.mu.Unlock()

	deps := make(map[string][]string, len(specs))
	rev := make(map[string][]string, len(specs))
	for _, s := range specs {
		d := append([]string{}, s.Deps...)
		sort.Strings(d)
		deps[s.Name] = d
		for _, dep := range s.Deps {
			rev[dep] = append(rev[dep], s.Name)
		}
	}
	for name := range rev {
		sort.Strings(rev[name])
	}
	e.logger.Printf("plan built: %d jobs validated, no unknown deps/cycles", len(specs))
	return &Plan{engine: e, specs: specs, deps: deps, rev: rev}, nil
}

// Plan 是通过校验的不可变执行计划。
type Plan struct {
	engine *Engine
	specs  []*JobSpec
	deps   map[string][]string
	rev    map[string][]string
}

type jobRuntime struct {
	spec      *JobSpec
	state     JobState
	attempts  int
	startedAt time.Time
	endedAt   time.Time
	lastErr   string
	waitDeps  map[string]struct{} // 尚未成功的依赖
}

// Run 表示一次运行，是观测入口。Start 之后可被多个 goroutine 并发查询。
type Run struct {
	plan     *Plan
	id       int64
	ctx      context.Context
	cancel   context.CancelFunc
	timedOut atomic.Bool

	mu        sync.RWMutex
	state     RunState
	jobs      map[string]*jobRuntime
	order     []string
	startTime time.Time
	endTime   time.Time
	retries   int
	attempts  int
	done      chan struct{}
}

// NewRun 创建一次运行（状态为 pending），不启动任何作业。
func (p *Plan) NewRun(parent context.Context) *Run {
	id := p.engine.nextID.Add(1)
	ctx, cancel := context.WithCancel(parent)
	jobs := make(map[string]*jobRuntime, len(p.specs))
	order := make([]string, 0, len(p.specs))
	for _, s := range p.specs {
		wait := make(map[string]struct{}, len(s.Deps))
		for _, d := range s.Deps {
			wait[d] = struct{}{}
		}
		jobs[s.Name] = &jobRuntime{spec: s, state: JobPending, waitDeps: wait}
		order = append(order, s.Name)
	}
	return &Run{plan: p, id: id, ctx: ctx, cancel: cancel, state: RunPending, jobs: jobs, order: order, done: make(chan struct{})}
}

// Start 启动调度并立即返回；用 Wait 等待结束。重复调用无效。
func (run *Run) Start() {
	run.mu.Lock()
	if run.state != RunPending {
		run.mu.Unlock()
		return
	}
	run.state = RunRunning
	run.startTime = run.plan.engine.now()
	run.mu.Unlock()

	run.plan.engine.logger.Printf("run %d started: jobs=%d concurrency=%d timeout=%s",
		run.id, len(run.order), run.plan.engine.opts.Concurrency, run.plan.engine.opts.RunTimeout)

	if run.plan.engine.opts.RunTimeout > 0 {
		// 独立 timer 触发标记，ctx.Err() 会暴露为 DeadlineExceeded。
		_ = time.AfterFunc(run.plan.engine.opts.RunTimeout, func() {
			run.timedOut.Store(true)
			run.plan.engine.logger.Printf("run %d timeout reached (%s), cancelling all in-flight jobs", run.id, run.plan.engine.opts.RunTimeout)
			run.cancel()
		})
	}
	go run.coordinate()
}

// Cancel 从外部取消整次运行。已通过校验但未启动的作业标记为 skipped。
func (run *Run) Cancel() {
	run.plan.engine.logger.Printf("run %d cancel requested", run.id)
	run.cancel()
}

// Wait 阻塞直到整次运行进入终态。
func (run *Run) Wait() { <-run.done }

// Done 返回终态通道。
func (run *Run) Done() <-chan struct{} { return run.done }

// RunAndWait 启动并等待结束，返回终态快照。
func (run *Run) RunAndWait() RunSnapshot {
	run.Start()
	run.Wait()
	return run.Snapshot()
}
