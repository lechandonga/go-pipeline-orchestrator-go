package orchestrator

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestConcurrencyLimitAndOrderIsExplainable(t *testing.T) {
	logBusiness("并发度限制与启动顺序")

	registry := NewRegistry()
	var active atomic.Int32
	var maxActive atomic.Int32

	makeHandler := func(name string) Handler {
		return func(ctx context.Context, _ *Execution) error {
			current := active.Add(1)
			for {
				seen := maxActive.Load()
				if current <= seen || maxActive.CompareAndSwap(seen, current) {
					break
				}
			}
			timer := time.NewTimer(10 * time.Millisecond)
			defer timer.Stop()
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-timer.C:
			}
			active.Add(-1)
			return nil
		}
	}

	for _, name := range []string{"a", "b", "c", "d"} {
		registry.MustRegister(Job{Name: name, Handler: makeHandler(name)})
	}

	run, err := registry.Run(context.Background(), RunOptions{Concurrency: 2})
	if err != nil {
		t.Fatal(err)
	}
	if got := maxActive.Load(); got > 2 {
		t.Fatalf("concurrency limit violated: %d", got)
	}
	snapshot := run.Snapshot()
	if snapshot.Stats.Succeeded != 4 {
		t.Fatalf("stats=%+v", snapshot.Stats)
	}

	// A slow dependency must start only after its dependency finishes.
	registry2 := NewRegistry()
	var startOrder []string
	var orderMu sync.Mutex
	registry2.MustRegister(Job{
		Name: "root",
		Handler: func(context.Context, *Execution) error {
			orderMu.Lock()
			startOrder = append(startOrder, "root")
			orderMu.Unlock()
			return nil
		},
	})
	registry2.MustRegister(Job{
		Name:      "leaf",
		DependsOn: []string{"root"},
		Handler: func(context.Context, *Execution) error {
			orderMu.Lock()
			startOrder = append(startOrder, "leaf")
			orderMu.Unlock()
			return nil
		},
	})
	run2, err := registry2.Run(context.Background(), RunOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if run2.Snapshot().Status != RunSucceeded ||
		fmt.Sprint(startOrder) != "[root leaf]" {
		t.Fatalf("dependency order violated: %v", startOrder)
	}

	var queued []string
	for _, event := range run.Events() {
		if event.Type == "queued" {
			queued = append(queued, event.Job)
		}
	}
	if len(queued) != 4 || queued[0] != "a" || queued[1] != "b" {
		t.Fatalf("ready jobs should start in sorted order: %v", queued)
	}
	logCase(fmt.Sprintf("最大并发=%d, 按名排序启动 %v, 依赖顺序 %v",
		maxActive.Load(), queued, startOrder), true, snapshot.Status)
}

func TestConcurrentSnapshotsAreSelfConsistent(t *testing.T) {
	logBusiness("并发观测一致性")

	registry := NewRegistry()
	const jobCount = 24
	for i := 0; i < jobCount; i++ {
		name := fmt.Sprintf("job-%02d", i)
		registry.MustRegister(Job{
			Name: name,
			Handler: func(ctx context.Context, _ *Execution) error {
				timer := time.NewTimer(2 * time.Millisecond)
				defer timer.Stop()
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-timer.C:
					return nil
				}
			},
		})
	}

	run, err := startRun(registry, RunOptions{ID: "observe-run", Concurrency: 3})
	if err != nil {
		t.Fatal(err)
	}

	var readers sync.WaitGroup
	stop := make(chan struct{})
	var inconsistent atomic.Bool
	for reader := 0; reader < 6; reader++ {
		readers.Add(1)
		go func() {
			defer readers.Done()
			for {
				select {
				case <-stop:
					return
				default:
					snapshot := run.Snapshot()
					stats := snapshot.Stats
					accounted := stats.Pending + stats.Queued + stats.Running + stats.Retrying +
						stats.Succeeded + stats.Failed + stats.Skipped + stats.Cancelled
					if accounted != stats.Total {
						inconsistent.Store(true)
						return
					}
					for name, job := range snapshot.Jobs {
						if job.Retries > 0 && (job.Status == JobPending || job.Status == JobQueued) {
							inconsistent.Store(true)
							_ = name
							return
						}
						if (job.Status == JobSucceeded || job.Status == JobFailed) &&
							job.Attempts == 0 {
							inconsistent.Store(true)
							return
						}
					}
				}
			}
		}()
	}

	run.Wait()
	close(stop)
	readers.Wait()

	if inconsistent.Load() {
		t.Fatal("observed an inconsistent snapshot during the run")
	}
	snapshot := run.Snapshot()
	if snapshot.Status != RunSucceeded || snapshot.Stats.Succeeded != jobCount {
		t.Fatalf("status=%s stats=%+v", snapshot.Status, snapshot.Stats)
	}
	logCase("并发查询下计数恒等式与作业状态始终自洽", true, snapshot.Status)
}
