package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"
)

// fakeClock records backoff sleeps while still honouring cancellation quickly.
type fakeClock struct {
	mu    sync.Mutex
	now   time.Time
	slept []time.Duration
}

func newFakeClock() *fakeClock {
	return &fakeClock{now: time.Unix(1_700_000_000, 0)}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Sleep(ctx context.Context, duration time.Duration) error {
	c.mu.Lock()
	c.slept = append(c.slept, duration)
	if duration > 0 {
		c.now = c.now.Add(duration)
	}
	c.mu.Unlock()

	if duration <= 0 {
		return nil
	}
	timer := time.NewTimer(time.Millisecond)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (c *fakeClock) sleeps() []time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]time.Duration(nil), c.slept...)
}

func okHandler(context.Context, *Execution) error { return nil }

func TestValidateRejectsBadDAGsBeforeExecution(t *testing.T) {
	logBusiness("DAG 校验")

	t.Run("重复注册被拒绝", func(t *testing.T) {
		registry := NewRegistry()
		if err := registry.Register(Job{Name: "a", Handler: okHandler}); err != nil {
			t.Fatal(err)
		}
		err := registry.Register(Job{Name: "a", Handler: okHandler})
		if !errors.Is(err, ErrDuplicateJob) {
			t.Fatalf("want ErrDuplicateJob, got %v", err)
		}
		logCase("重复注册", true, err)
	})

	t.Run("引用不存在的作业被拒绝", func(t *testing.T) {
		registry := NewRegistry()
		registry.MustRegister(Job{Name: "a", DependsOn: []string{"ghost"}, Handler: okHandler})
		_, err := registry.Run(context.Background(), RunOptions{})
		var validation *ValidationError
		if !errors.As(err, &validation) || !errors.Is(err, ErrUnknownDependency) {
			t.Fatalf("want ErrUnknownDependency, got %v", err)
		}
		if got := validation.JobIDs; len(got) != 1 || got[0] != "ghost" {
			t.Fatalf("unexpected unknown IDs: %v", got)
		}
		logCase("未知依赖", true, err)
	})

	t.Run("自依赖被拒绝", func(t *testing.T) {
		registry := NewRegistry()
		registry.MustRegister(Job{Name: "a", DependsOn: []string{"a"}, Handler: okHandler})
		_, err := registry.Run(context.Background(), RunOptions{})
		if !errors.Is(err, ErrUnknownDependency) {
			t.Fatalf("self dependency should be unknown/cyclic, got %v", err)
		}
		logCase("自依赖", true, err)
	})

	t.Run("依赖成环被拒绝", func(t *testing.T) {
		registry := NewRegistry()
		registry.MustRegister(Job{Name: "a", DependsOn: []string{"c"}, Handler: okHandler})
		registry.MustRegister(Job{Name: "b", DependsOn: []string{"a"}, Handler: okHandler})
		registry.MustRegister(Job{Name: "c", DependsOn: []string{"b"}, Handler: okHandler})
		registry.MustRegister(Job{Name: "root", Handler: okHandler})

		_, err := registry.Run(context.Background(), RunOptions{})
		var validation *ValidationError
		if !errors.As(err, &validation) || !errors.Is(err, ErrDependencyCycle) {
			t.Fatalf("want ErrDependencyCycle, got %v", err)
		}
		want := []string{"a", "b", "c"}
		if !sort.StringsAreSorted(validation.JobIDs) ||
			fmt.Sprint(validation.JobIDs) != fmt.Sprint(want) {
			t.Fatalf("want cyclic jobs %v, got %v", want, validation.JobIDs)
		}
		logCase("依赖成环", true, err)
	})

	t.Run("空注册表被拒绝", func(t *testing.T) {
		_, err := NewRegistry().Run(context.Background(), RunOptions{})
		if !errors.Is(err, ErrNoJobs) {
			t.Fatalf("want ErrNoJobs, got %v", err)
		}
		logCase("空注册表", true, err)
	})
}

func TestDependencyFailureSkipsOnlyDownstream(t *testing.T) {
	logBusiness("依赖失败与分支隔离")

	registry := NewRegistry()
	registry.MustRegister(Job{
		Name:    "bad",
		Handler: func(context.Context, *Execution) error { return NonRetryable(errors.New("boom")) },
	})
	registry.MustRegister(Job{
		Name:      "child",
		DependsOn: []string{"bad"},
		Handler:   okHandler,
	})
	registry.MustRegister(Job{
		Name:      "grandchild",
		DependsOn: []string{"child"},
		Handler:   okHandler,
	})
	registry.MustRegister(Job{
		Name:    "independent",
		Handler: okHandler,
	})

	run, err := registry.Run(context.Background(), RunOptions{ID: "branch-run"})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := run.Snapshot()

	if snapshot.Status != RunFailed {
		t.Fatalf("want run failed, got %s", snapshot.Status)
	}
	if snapshot.Jobs["bad"].Status != JobFailed ||
		snapshot.Jobs["child"].Status != JobSkipped ||
		snapshot.Jobs["grandchild"].Status != JobSkipped ||
		snapshot.Jobs["independent"].Status != JobSucceeded {
		t.Fatalf("unexpected job states: bad=%s child=%s grandchild=%s independent=%s",
			snapshot.Jobs["bad"].Status, snapshot.Jobs["child"].Status,
			snapshot.Jobs["grandchild"].Status, snapshot.Jobs["independent"].Status)
	}
	if got := snapshot.Jobs["grandchild"].SkippedReason; got == "" {
		t.Fatal("skipped reason should explain the failure")
	}
	if snapshot.Stats.Failed != 1 || snapshot.Stats.Skipped != 2 || snapshot.Stats.Succeeded != 1 {
		t.Fatalf("unexpected stats: %+v", snapshot.Stats)
	}
	logCase("失败下游传递跳过、独立分支成功", true, snapshot.Status)
}

func TestDependencyOutputFlowsToDependents(t *testing.T) {
	logBusiness("依赖输出传递")

	registry := NewRegistry()
	registry.MustRegister(Job{
		Name:    "producer",
		Handler: func(_ context.Context, exec *Execution) error { exec.SetOutput(42); return nil },
	})
	registry.MustRegister(Job{
		Name:      "consumer",
		DependsOn: []string{"producer"},
		Handler: func(_ context.Context, exec *Execution) error {
			value, ok := exec.Result("producer")
			if !ok || value.(int) != 42 {
				return fmt.Errorf("missing producer output: %v ok=%v", value, ok)
			}
			return nil
		},
	})

	run, err := registry.Run(context.Background(), RunOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if run.Snapshot().Status != RunSucceeded {
		t.Fatalf("status=%s", run.Snapshot().Status)
	}
	logCase("上游输出可被下游读取", true, run.Snapshot().Status)
}
