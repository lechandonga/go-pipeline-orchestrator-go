package orchestrator

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestRetryWithBackoffAndEffectOnce(t *testing.T) {
	logBusiness("失败重试、退避与副作用去重")

	registry := NewRegistry()
	clock := newFakeClock()
	var attempts atomic.Int32
	var sideEffects atomic.Int32

	registry.MustRegister(Job{
		Name:        "flaky",
		MaxAttempts: 3,
		Backoff:     ExponentialBackoff(10*time.Millisecond, time.Second),
		Handler: func(_ context.Context, exec *Execution) error {
			current := attempts.Add(1)
			if current < 3 {
				return Retryable(errors.New("temporary failure"))
			}
			if _, err := exec.Effects().Once("create-row", func() (any, error) {
				sideEffects.Add(1)
				return "row-1", nil
			}); err != nil {
				return err
			}
			exec.SetOutput("done")
			return nil
		},
	})

	run, err := registry.Run(context.Background(), RunOptions{ID: "retry-run", Clock: clock})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := run.Snapshot()

	if snapshot.Status != RunSucceeded {
		t.Fatalf("want success, got %s err=%s", snapshot.Status, snapshot.Jobs["flaky"].LastError)
	}
	if got := attempts.Load(); got != 3 {
		t.Fatalf("want 3 attempts, got %d", got)
	}
	if got := sideEffects.Load(); got != 1 {
		t.Fatalf("side effect must happen exactly once, got %d", got)
	}
	if got := snapshot.Jobs["flaky"].Retries; got != 2 {
		t.Fatalf("want 2 retries, got %d", got)
	}
	sleeps := clock.sleeps()
	if len(sleeps) != 2 || sleeps[0] != 10*time.Millisecond || sleeps[1] != 20*time.Millisecond {
		t.Fatalf("want exponential waits 10ms,20ms, got %v", sleeps)
	}
	if snapshot.Jobs["flaky"].Output != "done" {
		t.Fatalf("unexpected output: %v", snapshot.Jobs["flaky"].Output)
	}
	logCase("退避重试后成功且副作用仅一次", true, snapshot.Status)
}

func TestFailedSideEffectRetriedButSuccessNotRepeated(t *testing.T) {
	logBusiness("失败副作用可重试、成功副作用不重复")

	registry := NewRegistry()
	var effectCalls atomic.Int32
	registry.MustRegister(Job{
		Name:        "idempotent",
		MaxAttempts: 3,
		Backoff:     ConstantBackoff(0),
		Handler: func(_ context.Context, exec *Execution) error {
			_, err := exec.Effects().Once("write", func() (any, error) {
				n := effectCalls.Add(1)
				if n < 2 {
					return nil, errors.New("write failed before commit")
				}
				return "committed", nil
			})
			return err
		},
	})

	run, err := registry.Run(context.Background(), RunOptions{})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := run.Snapshot()
	if snapshot.Status != RunSucceeded {
		t.Fatalf("status=%s err=%s", snapshot.Status, snapshot.Jobs["idempotent"].LastError)
	}
	if effectCalls.Load() != 2 {
		t.Fatalf("want effect attempted twice (fail then success), got %d", effectCalls.Load())
	}
	if got := snapshot.Effects["write"]; got != "committed" {
		t.Fatalf("effect snapshot = %v", got)
	}
	logCase("失败不缓存、成功仅一次", true, snapshot.Status)
}

func TestNonRetryableFailsImmediately(t *testing.T) {
	logBusiness("可重试与不可重试错误区分")

	registry := NewRegistry()
	var attempts atomic.Int32
	registry.MustRegister(Job{
		Name:        "fatal",
		MaxAttempts: 5,
		Backoff:     ConstantBackoff(time.Hour),
		Handler: func(context.Context, *Execution) error {
			attempts.Add(1)
			return NonRetryable(errors.New("invalid payload"))
		},
	})

	run, err := registry.Run(context.Background(), RunOptions{})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := run.Snapshot()
	if snapshot.Jobs["fatal"].Status != JobFailed || attempts.Load() != 1 {
		t.Fatalf("status=%s attempts=%d", snapshot.Jobs["fatal"].Status, attempts.Load())
	}
	if !snapshot.Jobs["fatal"].NonRetryable {
		t.Fatal("failure should be marked non-retryable")
	}
	logCase("不可重试错误立即失败", true, snapshot.Jobs["fatal"].Status)
}

func TestPanicIsNonRetryableFailure(t *testing.T) {
	logBusiness("处理器 panic 安全降级")

	registry := NewRegistry()
	registry.MustRegister(Job{
		Name:        "panicky",
		MaxAttempts: 3,
		Handler:     func(context.Context, *Execution) error { panic("kaboom") },
	})
	run, err := registry.Run(context.Background(), RunOptions{})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := run.Snapshot()
	if snapshot.Status != RunFailed || snapshot.Jobs["panicky"].Status != JobFailed {
		t.Fatalf("status=%s job=%+v", snapshot.Status, snapshot.Jobs["panicky"])
	}
	if snapshot.Jobs["panicky"].Attempts != 1 || !snapshot.Jobs["panicky"].NonRetryable {
		t.Fatalf("panic should fail once and be non-retryable: %+v", snapshot.Jobs["panicky"])
	}
	logCase("panic 不崩溃进程且不重试", true, snapshot.Status)
}
