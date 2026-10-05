package orchestrator

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

var errRetry = errors.New("retry me")

func TestCancelStopsRunningAndQueuedJobs(t *testing.T) {
	logBusiness("取消语义")

	registry := NewRegistry()
	started := make(chan struct{})
	allowFinish := make(chan struct{})
	var laterStarted atomic.Bool

	registry.MustRegister(Job{
		// The lexicographically smallest name is dispatched first with
		// Concurrency=1, so barrier and later are provably still queued.
		Name:        "a-slow",
		MaxAttempts: 3,
		Handler: func(ctx context.Context, _ *Execution) error {
			close(started)
			<-ctx.Done()
			<-allowFinish
			return ctx.Err()
		},
	})
	registry.MustRegister(Job{
		Name: "b-barrier",
		Handler: func(context.Context, *Execution) error {
			t.Error("b-barrier must never run")
			return nil
		},
	})
	registry.MustRegister(Job{
		Name: "c-later",
		Handler: func(context.Context, *Execution) error {
			laterStarted.Store(true)
			return nil
		},
	})

	run, err := startRun(registry, RunOptions{ID: "cancel-run", Concurrency: 1})
	if err != nil {
		t.Fatal(err)
	}
	<-started
	if err := run.Cancel(); err != nil {
		t.Fatal(err)
	}
	close(allowFinish)
	run.Wait()

	snapshot := run.Snapshot()
	if snapshot.Status != RunCancelled {
		t.Fatalf("want cancelled run, got %s", snapshot.Status)
	}
	if snapshot.Jobs["a-slow"].Status != JobCancelled {
		t.Fatalf("slow job status=%s", snapshot.Jobs["a-slow"].Status)
	}
	for _, name := range []string{"b-barrier", "c-later"} {
		if snapshot.Jobs[name].Status != JobCancelled {
			t.Fatalf("%s job status=%s, want cancelled", name, snapshot.Jobs[name].Status)
		}
	}
	if laterStarted.Load() {
		t.Fatal("later job must never start after cancellation")
	}
	logCase("取消后运行中与排队中的作业都停止", true, snapshot.Status)
}

func TestCancelDuringBackoffStopsRetry(t *testing.T) {
	logBusiness("退避期间取消")

	registry := NewRegistry()
	startedCh := make(chan struct{})
	var once atomic.Bool

	registry.MustRegister(Job{
		Name:        "retrying",
		MaxAttempts: 3,
		Backoff:     ConstantBackoff(time.Hour),
		Handler: func(ctx context.Context, _ *Execution) error {
			if once.CompareAndSwap(false, true) {
				close(startedCh)
				return errRetry
			}
			t.Error("retry must not execute after cancellation")
			return nil
		},
	})

	run, err := startRun(registry, RunOptions{ID: "backoff-cancel"})
	if err != nil {
		t.Fatal(err)
	}
	<-startedCh
	// Give the dispatcher time to enter the long backoff sleep.
	time.Sleep(30 * time.Millisecond)
	if err := run.Cancel(); err != nil {
		t.Fatal(err)
	}
	run.Wait()

	snapshot := run.Snapshot()
	if snapshot.Status != RunCancelled {
		t.Fatalf("status=%s", snapshot.Status)
	}
	if snapshot.Jobs["retrying"].Status != JobCancelled {
		t.Fatalf("job status=%s", snapshot.Jobs["retrying"].Status)
	}
	if snapshot.Jobs["retrying"].Attempts != 1 {
		t.Fatalf("attempts=%d", snapshot.Jobs["retrying"].Attempts)
	}
	logCase("退避中取消不会发起新的尝试", true, snapshot.Status)
}

func TestRunTimeoutDoesNotHang(t *testing.T) {
	logBusiness("运行级超时")

	registry := NewRegistry()
	registry.MustRegister(Job{
		Name: "hung",
		Handler: func(ctx context.Context, _ *Execution) error {
			<-ctx.Done()
			return ctx.Err()
		},
	})

	start := time.Now()
	run, err := registry.Run(context.Background(), RunOptions{
		ID: "timeout-run", Timeout: 20 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	elapsed := time.Since(start)
	snapshot := run.Snapshot()
	if snapshot.Status != RunCancelled {
		t.Fatalf("timeout should cancel run, got %s", snapshot.Status)
	}
	if elapsed > 2*time.Second {
		t.Fatalf("run hung after timeout: %s", elapsed)
	}
	if snapshot.Jobs["hung"].Status != JobCancelled {
		t.Fatalf("job status=%s", snapshot.Jobs["hung"].Status)
	}
	logCase("运行级超时后状态终止不挂起", true, snapshot.Status)
}

func TestJobTimeoutIsRetryable(t *testing.T) {
	logBusiness("单作业超时与重试")

	registry := NewRegistry()
	var attempts atomic.Int32
	registry.MustRegister(Job{
		Name:        "tight",
		Timeout:     15 * time.Millisecond,
		MaxAttempts: 2,
		Handler: func(ctx context.Context, _ *Execution) error {
			if attempts.Add(1) == 1 {
				time.Sleep(40 * time.Millisecond)
				return ctx.Err()
			}
			return nil
		},
	})

	run, err := registry.Run(context.Background(), RunOptions{})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := run.Snapshot()
	if snapshot.Status != RunSucceeded {
		t.Fatalf("want success after timeout retry, got %s: %s",
			snapshot.Status, snapshot.Jobs["tight"].LastError)
	}
	if attempts.Load() != 2 {
		t.Fatalf("want 2 attempts, got %d", attempts.Load())
	}
	logCase("作业超时可重试并成功", true, snapshot.Status)
}

func TestCancelFinishedRun(t *testing.T) {
	logBusiness("终态取消幂等")

	registry := NewRegistry()
	registry.MustRegister(Job{Name: "a", Handler: okHandler})
	run, err := registry.Run(context.Background(), RunOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := run.Cancel(); err != ErrAlreadyFinished {
		t.Fatalf("want ErrAlreadyFinished, got %v", err)
	}
	logCase("终态运行拒绝重复取消", true, err)
}
