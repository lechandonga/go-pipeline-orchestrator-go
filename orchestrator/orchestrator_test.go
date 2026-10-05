package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// newTestEngine 把引擎日志接到 t.Log；go test -v 时可以直接看到每个用例
// 覆盖的业务（校验、级联、重试、取消、超时、并发观测）及其执行情况。
func newTestEngine(t *testing.T, opts Options) *Engine {
	t.Helper()
	if opts.Logger == nil {
		opts.Logger = log.New(testWriter{t}, "  ", log.LstdFlags|log.Lmicroseconds)
	}
	return NewEngine(opts)
}

type testWriter struct{ t *testing.T }

func (w testWriter) Write(p []byte) (int, error) {
	w.t.Logf("%s", strings.TrimRight(string(p), "\r\n"))
	return len(p), nil
}

func okJob() RunFunc { return func(context.Context, int) error { return nil } }

func findJob(snap RunSnapshot, name string) JobResult {
	for _, j := range snap.Jobs {
		if j.Name == name {
			return j
		}
	}
	return JobResult{Name: name, State: "<missing>"}
}

func mustBuildRun(t *testing.T, eng *Engine, reg *Registry) RunSnapshot {
	t.Helper()
	plan, err := eng.Build(reg)
	if err != nil {
		t.Fatal(err)
	}
	return plan.NewRun(context.Background()).RunAndWait()
}

// ---- 业务一：注册与依赖编排校验：执行前拒绝，原因可区分 ----

func TestValidationRejectedBeforeExecution(t *testing.T) {
	t.Run("duplicate registration", func(t *testing.T) {
		r := NewRegistry()
		if err := r.Register(JobSpec{Name: "a", Run: okJob()}); err != nil {
			t.Fatal(err)
		}
		err := r.Register(JobSpec{Name: "a", Run: okJob()})
		var ve *ValidationError
		if !errors.As(err, &ve) || ve.Reason != ReasonDuplicateJob {
			t.Fatalf("want %s, got %v", ReasonDuplicateJob, err)
		}
	})

	cases := []struct {
		name   string
		build  func(*Registry)
		reason ValidationReason
	}{
		{
			name: "unknown dependency",
			build: func(r *Registry) {
				r.MustRegister(JobSpec{Name: "a", Deps: []string{"ghost"}, Run: okJob()})
			},
			reason: ReasonUnknownDep,
		},
		{
			name: "self dependency",
			build: func(r *Registry) {
				r.MustRegister(JobSpec{Name: "a", Deps: []string{"a"}, Run: okJob()})
			},
			reason: ReasonSelfDependency,
		},
		{
			name:   "direct cycle a-b-a",
			reason: ReasonCycleDetected,
			build: func(r *Registry) {
				r.MustRegister(JobSpec{Name: "a", Deps: []string{"b"}, Run: okJob()})
				r.MustRegister(JobSpec{Name: "b", Deps: []string{"a"}, Run: okJob()})
			},
		},
		{
			name:   "long cycle c-d-e-c",
			reason: ReasonCycleDetected,
			build: func(r *Registry) {
				r.MustRegister(JobSpec{Name: "c", Deps: []string{"e"}, Run: okJob()})
				r.MustRegister(JobSpec{Name: "d", Deps: []string{"c"}, Run: okJob()})
				r.MustRegister(JobSpec{Name: "e", Deps: []string{"d"}, Run: okJob()})
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			eng := newTestEngine(t, Options{})
			reg := NewRegistry()
			tc.build(reg)
			_, err := eng.Build(reg)
			var ve *ValidationError
			if !errors.As(err, &ve) {
				t.Fatalf("expected *ValidationError, got %v", err)
			}
			if ve.Reason != tc.reason {
				t.Fatalf("expected reason %s, got %s (%v)", tc.reason, ve.Reason, err)
			}
		})
	}
}

func TestRegisterAfterBuildRejected(t *testing.T) {
	eng := newTestEngine(t, Options{})
	reg := NewRegistry()
	reg.MustRegister(JobSpec{Name: "a", Run: okJob()})
	if _, err := eng.Build(reg); err != nil {
		t.Fatal(err)
	}
	err := reg.Register(JobSpec{Name: "b", Run: okJob()})
	var ve *ValidationError
	if !errors.As(err, &ve) || ve.Reason != ReasonJobAlreadyBuilt {
		t.Fatalf("expected ReasonJobAlreadyBuilt, got %v", err)
	}
}

// ---- 业务二：依赖失败时独立分支继续执行，终态可解释 ----

func TestDependencyFailureIsolatesBranches(t *testing.T) {
	// root -> bad  -> child  （child 必须被跳过，且执行体绝不运行）
	//      \-> goodA -> goodB （独立分支必须照常成功）
	eng := newTestEngine(t, Options{Concurrency: 4})
	reg := NewRegistry()

	var ran int32 // 仅 mark() 作业计数：root/goodA/goodB（bad 使用独立执行体，child 被跳过）
	mark := func() RunFunc {
		return func(context.Context, int) error {
			atomic.AddInt32(&ran, 1)
			return nil
		}
	}
	reg.MustRegister(JobSpec{Name: "root", Run: mark()})
	reg.MustRegister(JobSpec{Name: "bad", Deps: []string{"root"},
		Run: func(context.Context, int) error { return errors.New("boom: non-retryable disk failure") }})
	reg.MustRegister(JobSpec{Name: "child", Deps: []string{"bad"}, Run: mark()})
	reg.MustRegister(JobSpec{Name: "goodA", Deps: []string{"root"}, Run: mark()})
	reg.MustRegister(JobSpec{Name: "goodB", Deps: []string{"goodA"}, Run: mark()})

	snap := mustBuildRun(t, eng, reg)

	if snap.State != RunFailed {
		t.Fatalf("expected RunFailed, got %s", snap.State)
	}
	want := map[string]JobState{
		"root": JobSucceeded, "bad": JobFailed, "child": JobSkipped,
		"goodA": JobSucceeded, "goodB": JobSucceeded,
	}
	for _, j := range snap.Jobs {
		if want[j.Name] != j.State {
			t.Fatalf("job %s: want %s, got %s (err=%q)", j.Name, want[j.Name], j.State, j.LastError)
		}
	}
	if ran != 3 {
		t.Fatalf("expected exactly 3 mark-jobs to run (child skipped), got %d", ran)
	}
	if snap.Succeeded != 3 || snap.Failed != 1 || snap.Skipped != 1 {
		t.Fatalf("unexpected counts: %+v", snap)
	}
	if !strings.Contains(snap.FailureReport, "root failures") ||
		!strings.Contains(snap.FailureReport, "bad") ||
		!strings.Contains(snap.FailureReport, "child") {
		t.Fatalf("failure report must explain root cause and skipped downstream:\n%s", snap.FailureReport)
	}
}

// ---- 业务三：可重试错误退避后成功；不可重试立即失败；副作用只生效一次 ----

func TestRetryBackoffAndIdempotentEffect(t *testing.T) {
	eng := newTestEngine(t, Options{Concurrency: 2, BackoffJitter: 0})
	reg := NewRegistry()

	var flakyAttempts, sideEffects int32
	effect := NewEffectGuard()

	reg.MustRegister(JobSpec{
		Name:         "flaky",
		MaxAttempts:  4,
		RetryBackoff: 5 * time.Millisecond,
		Run: func(ctx context.Context, attempt int) error {
			n := atomic.AddInt32(&flakyAttempts, 1)
			if err := effect.Do(func() error {
				atomic.AddInt32(&sideEffects, 1)
				return nil
			}); err != nil {
				return err
			}
			if n < 3 {
				return Retryable(fmt.Errorf("temporary network glitch #%d", n))
			}
			return nil
		},
	})
	var fatalCalls int32
	reg.MustRegister(JobSpec{
		Name: "fatal", MaxAttempts: 5, RetryBackoff: time.Hour,
		Run: func(context.Context, int) error {
			atomic.AddInt32(&fatalCalls, 1)
			return errors.New("permanent config error") // 未包装 Retryable，立即失败
		},
	})

	snap := mustBuildRun(t, eng, reg)

	flaky := findJob(snap, "flaky")
	fatal := findJob(snap, "fatal")
	if flaky.State != JobSucceeded || flaky.Attempts != 3 || flaky.Retries != 2 {
		t.Fatalf("flaky: %+v", flaky)
	}
	if sideEffects != 1 {
		t.Fatalf("side effect ran %d times, want exactly 1", sideEffects)
	}
	if fatal.State != JobFailed || fatal.Attempts != 1 {
		t.Fatalf("fatal must fail fast without retry: %+v", fatal)
	}
	if fatalCalls != 1 {
		t.Fatalf("fatal body executed %d times, want 1", fatalCalls)
	}
	if snap.TotalRetries != 2 {
		t.Fatalf("TotalRetries=%d want 2", snap.TotalRetries)
	}
	// 指数退避必须真实流逝：5ms + 10ms。
	if flaky.Duration < 12*time.Millisecond {
		t.Fatalf("expected exponential backoff to elapse, duration=%s", flaky.Duration)
	}
}

func TestRetryExhaustedMarksFailed(t *testing.T) {
	eng := newTestEngine(t, Options{BackoffJitter: 0})
	reg := NewRegistry()
	reg.MustRegister(JobSpec{
		Name: "j", MaxAttempts: 3, RetryBackoff: time.Millisecond,
		Run: func(context.Context, int) error { return Retryable(errors.New("nope")) },
	})
	snap := mustBuildRun(t, eng, reg)
	j := findJob(snap, "j")
	if j.State != JobFailed || j.Attempts != 3 || j.Retries != 2 {
		t.Fatalf("%+v", j)
	}
}

// 取消/超时错误即使被 Retryable 包装也不得重试。
func TestContextErrorNeverRetried(t *testing.T) {
	eng := newTestEngine(t, Options{})
	reg := NewRegistry()
	var calls int32
	reg.MustRegister(JobSpec{
		Name: "j", MaxAttempts: 5, RetryBackoff: time.Millisecond,
		Run: func(ctx context.Context, attempt int) error {
			atomic.AddInt32(&calls, 1)
			return Retryable(context.DeadlineExceeded)
		},
	})
	snap := mustBuildRun(t, eng, reg)
	if calls != 1 || findJob(snap, "j").State != JobFailed {
		t.Fatalf("calls=%d job=%+v", calls, findJob(snap, "j"))
	}
}

// ---- 业务四：取消后在执行作业全部停下，运行不滞留 running ----

func TestCancelStopsAllRunningJobs(t *testing.T) {
	eng := newTestEngine(t, Options{Concurrency: 1})
	reg := NewRegistry()

	started := make(chan string, 4)
	block := func(name string) RunFunc {
		return func(ctx context.Context, attempt int) error {
			started <- name
			<-ctx.Done() // 尊重取消信号
			return ctx.Err()
		}
	}
	reg.MustRegister(JobSpec{Name: "a", Run: block("a")})
	reg.MustRegister(JobSpec{Name: "b", Deps: []string{"a"}, Run: block("b")})
	reg.MustRegister(JobSpec{Name: "c", Deps: []string{"a"}, Run: block("c")})

	plan, err := eng.Build(reg)
	if err != nil {
		t.Fatal(err)
	}
	run := plan.NewRun(context.Background())
	run.Start()
	if got := <-started; got != "a" {
		t.Fatalf("first scheduled job should be a, got %s", got)
	}
	run.Cancel()

	done := make(chan RunSnapshot, 1)
	go func() { run.Wait(); done <- run.Snapshot() }()
	select {
	case snap := <-done:
		if snap.State != RunCanceled {
			t.Fatalf("want RunCanceled, got %s", snap.State)
		}
		if findJob(snap, "a").State != JobCanceled {
			t.Fatalf("in-flight job a must be canceled: %+v", findJob(snap, "a"))
		}
		for _, nm := range []string{"b", "c"} {
			if st := findJob(snap, nm).State; st != JobSkipped {
				t.Fatalf("not-started job %s must be skipped, got %s", nm, st)
			}
		}
	case <-time.After(2 * time.Second):
		t.Fatal("run stuck in running after cancel")
	}
}

// ---- 业务五：运行级超时 ----

func TestRunTimeout(t *testing.T) {
	eng := newTestEngine(t, Options{RunTimeout: 30 * time.Millisecond})
	reg := NewRegistry()
	reg.MustRegister(JobSpec{
		Name: "slow",
		Run: func(ctx context.Context, attempt int) error {
			select {
			case <-time.After(3 * time.Second):
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		},
	})
	snap := mustBuildRun(t, eng, reg)
	if snap.State != RunTimedOut {
		t.Fatalf("want RunTimedOut, got %s", snap.State)
	}
	if findJob(snap, "slow").State != JobCanceled {
		t.Fatalf("timed-out in-flight job must be canceled, not stuck running")
	}
}

// 退避期间超时也必须及时收尾，而不是等退避结束。
func TestTimeoutDuringBackoff(t *testing.T) {
	eng := newTestEngine(t, Options{RunTimeout: 20 * time.Millisecond, BackoffJitter: 0})
	reg := NewRegistry()
	reg.MustRegister(JobSpec{
		Name: "j", MaxAttempts: 3, RetryBackoff: 30 * time.Second,
		Run: func(ctx context.Context, attempt int) error {
			if attempt == 1 {
				return Retryable(errors.New("temp"))
			}
			return nil
		},
	})
	snap := mustBuildRun(t, eng, reg)
	if snap.State != RunTimedOut {
		t.Fatalf("want RunTimedOut, got %s", snap.State)
	}
	if findJob(snap, "j").State != JobCanceled {
		t.Fatalf("backing-off job must end canceled: %+v", findJob(snap, "j"))
	}
}

// ---- 业务六：并发度受限下的启动顺序可解释 ----

func TestConcurrencyLimitAndOrder(t *testing.T) {
	// 并发度 1：执行体真正开始运行的顺序必然等于协调器的派发顺序，
	// 因此可以严格断言“按注册顺序取槽”，而不受 goroutine 调度抖动影响。
	const limit = 1
	eng := newTestEngine(t, Options{Concurrency: limit})
	reg := NewRegistry()

	var concurrentNow int32
	var maxObserved int32
	var mu sync.Mutex
	var order []string
	release := make(chan struct{}, 4)

	for _, name := range []string{"j1", "j2", "j3", "j4"} {
		n := name
		reg.MustRegister(JobSpec{
			Name: n,
			Run: func(ctx context.Context, attempt int) error {
				cur := atomic.AddInt32(&concurrentNow, 1)
				for {
					old := atomic.LoadInt32(&maxObserved)
					if cur <= old || atomic.CompareAndSwapInt32(&maxObserved, old, cur) {
						break
					}
				}
				mu.Lock()
				order = append(order, n)
				mu.Unlock()
				select {
				case <-release:
				case <-ctx.Done():
				}
				atomic.AddInt32(&concurrentNow, -1)
				return ctx.Err()
			},
		})
	}

	plan, err := eng.Build(reg)
	if err != nil {
		t.Fatal(err)
	}
	run := plan.NewRun(context.Background())
	run.Start()

	wantOrder := []string{}
	for i := 0; i < 4; i++ {
		nm := fmt.Sprintf("j%d", i+1)
		// 等待该作业启动（串行下它一定是第 i+1 个）。
		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			mu.Lock()
			got := len(order) == i+1
			mu.Unlock()
			if got {
				break
			}
			time.Sleep(time.Millisecond)
		}
		mu.Lock()
		if len(order) != i+1 || order[i] != nm {
			mu.Unlock()
			t.Fatalf("step %d: want %s next, got order %v", i+1, nm, order)
		}
		if cur := atomic.LoadInt32(&concurrentNow); cur != 1 {
			mu.Unlock()
			t.Fatalf("only one job may run at a time, saw %d", cur)
		}
		mu.Unlock()
		wantOrder = append(wantOrder, nm)
		release <- struct{}{} // 放行，下一个才能拿到槽
	}
	final := waitSnapshot(t, run)
	if final.State != RunSucceeded {
		t.Fatalf("want success, got %s", final.State)
	}
	if maxObserved != 1 {
		t.Fatalf("expected peak concurrency 1, saw %d", maxObserved)
	}
}

func waitSnapshot(t *testing.T, run *Run) RunSnapshot {
	t.Helper()
	run.Wait()
	return run.Snapshot()
}

// ---- 业务七：并发运行 + 并发查询下的观测自洽性 ----

func TestConcurrentSnapshotsAreConsistent(t *testing.T) {
	eng := newTestEngine(t, Options{Concurrency: 3})
	reg := NewRegistry()
	for i := 0; i < 12; i++ {
		deps := []string{}
		if i > 0 {
			deps = []string{fmt.Sprintf("job%d", i-1)}
		}
		name := fmt.Sprintf("job%d", i)
		reg.MustRegister(JobSpec{
			Name: name, Deps: deps,
			Run: func(ctx context.Context, attempt int) error {
				select {
				case <-time.After(time.Duration(2) * time.Millisecond):
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			},
		})
	}

	plan, err := eng.Build(reg)
	if err != nil {
		t.Fatal(err)
	}
	run := plan.NewRun(context.Background())
	run.Start()

	var readers sync.WaitGroup
	stop := make(chan struct{})
	for r := 0; r < 6; r++ {
		readers.Add(1)
		go func() {
			defer readers.Done()
			for {
				select {
				case <-stop:
					return
				default:
					snap := run.Snapshot()
					assertSnapshotSelfConsistent(snap)
				}
			}
		}()
	}

	final := waitSnapshot(t, run)
	close(stop)
	readers.Wait()

	if final.State != RunSucceeded || final.Succeeded != 12 {
		t.Fatalf("final snapshot: %+v", final)
	}
	assertSnapshotSelfConsistent(final)
}

func assertSnapshotSelfConsistent(snap RunSnapshot) {
	bucketed := snap.Succeeded + snap.Failed + snap.Skipped + snap.Canceled + snap.Running + snap.Pending
	if bucketed != snap.Total {
		panic(fmt.Sprintf("inconsistent snapshot: state buckets %d != total %d", bucketed, snap.Total))
	}
	attemptSum := 0
	retrySum := 0
	for _, j := range snap.Jobs {
		attemptSum += j.Attempts
		if j.Retries != maxInt(j.Attempts-1, 0) {
			panic(fmt.Sprintf("job %s retries/attempts inconsistent: %+v", j.Name, j))
		}
		retrySum += j.Retries
		// 终态作业必须有结束时间与非负耗时。
		switch j.State {
		case JobSucceeded, JobFailed, JobCanceled, JobSkipped:
			if j.EndedAt.IsZero() || j.Duration < 0 {
				panic(fmt.Sprintf("terminal job %s missing end time", j.Name))
			}
		}
	}
	if attemptSum != snap.TotalAttempts {
		panic(fmt.Sprintf("attempt sum %d != TotalAttempts %d", attemptSum, snap.TotalAttempts))
	}
	if retrySum != snap.TotalRetries {
		panic(fmt.Sprintf("retry sum %d != TotalRetries %d", retrySum, snap.TotalRetries))
	}
}

// 并发度上界：limit 个作业占满槽位后，后续作业必须等待。
func TestConcurrencyPeakNeverExceedsLimit(t *testing.T) {
	const limit = 2
	eng := newTestEngine(t, Options{Concurrency: limit})
	reg := NewRegistry()

	var concurrentNow, maxObserved int32
	started := make(chan struct{}, 8)
	release := make(chan struct{})

	for i := 0; i < 6; i++ {
		reg.MustRegister(JobSpec{
			Name: fmt.Sprintf("j%d", i),
			Run: func(ctx context.Context, attempt int) error {
				cur := atomic.AddInt32(&concurrentNow, 1)
				for {
					old := atomic.LoadInt32(&maxObserved)
					if cur <= old || atomic.CompareAndSwapInt32(&maxObserved, old, cur) {
						break
					}
				}
				started <- struct{}{}
				select {
				case <-release:
				case <-ctx.Done():
				}
				atomic.AddInt32(&concurrentNow, -1)
				return ctx.Err()
			},
		})
	}
	plan, err := eng.Build(reg)
	if err != nil {
		t.Fatal(err)
	}
	run := plan.NewRun(context.Background())
	run.Start()

	<-started
	<-started
	// 占满后再给一点时间，第三个作业不得启动。
	time.Sleep(20 * time.Millisecond)
	if got := atomic.LoadInt32(&concurrentNow); got != limit {
		t.Fatalf("want %d in flight, got %d", limit, got)
	}
	close(release)

	final := waitSnapshot(t, run)
	if final.State != RunSucceeded {
		t.Fatalf("want success, got %s", final.State)
	}
	if maxObserved != limit {
		t.Fatalf("peak concurrency: want %d, got %d", limit, maxObserved)
	}
}
