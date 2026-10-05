// demo 展示本地任务编排器的典型用法：注册 -> 构建校验 -> 运行 -> 观测。
// 所有作业都在本进程内用本地函数模拟，不访问任何外部服务。
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/lechandonga/go-pipeline-orchestrator-go/orchestrator"
)

func main() {
	logger := log.New(os.Stdout, "[demo] ", log.LstdFlags|log.Lmicroseconds)
	eng := orchestrator.NewEngine(orchestrator.Options{
		Concurrency:   2,
		RunTimeout:    5 * time.Second,
		BackoffJitter: 0.2,
		Logger:        logger,
	})

	reg := orchestrator.NewRegistry()
	reg.MustRegister(orchestrator.JobSpec{Name: "fetch-config", Run: succeed("fetch-config", 20*time.Millisecond)})
	reg.MustRegister(orchestrator.JobSpec{Name: "fetch-data", Deps: []string{"fetch-config"},
		Run: flaky(2, 10*time.Millisecond), MaxAttempts: 3, RetryBackoff: 30 * time.Millisecond})
	reg.MustRegister(orchestrator.JobSpec{Name: "transform", Deps: []string{"fetch-data"},
		Run: succeed("transform", 20*time.Millisecond)})
	reg.MustRegister(orchestrator.JobSpec{Name: "load-db", Deps: []string{"transform"},
		Run: succeed("load-db", 20*time.Millisecond)})
	// 与主管线并行的独立分支：即使它失败，主管线仍然继续。
	reg.MustRegister(orchestrator.JobSpec{Name: "warm-cache", Deps: []string{"fetch-config"},
		Run: func(ctx context.Context, attempt int) error {
			return errors.New("cache cluster down (non-retryable)")
		}})
	reg.MustRegister(orchestrator.JobSpec{Name: "report-cache", Deps: []string{"warm-cache"},
		Run: succeed("report-cache", 0)})

	plan, err := eng.Build(reg) // 依赖成环/未知依赖/重复注册在这里被拒绝
	if err != nil {
		log.Fatalf("build rejected: %v", err)
	}

	run := plan.NewRun(context.Background())
	snap := run.RunAndWait()

	fmt.Println("\n========== RUN SUMMARY ==========")
	fmt.Printf("run %d state: %s, duration: %s, attempts: %d, retries: %d\n",
		snap.RunID, snap.State, snap.Duration, snap.TotalAttempts, snap.TotalRetries)
	for _, j := range snap.Jobs {
		line := fmt.Sprintf("  %-14s %-10s attempts=%d duration=%s", j.Name, j.State, j.Attempts, j.Duration)
		if j.LastError != "" {
			line += " err=" + j.LastError
		}
		fmt.Println(line)
	}
	if snap.FailureReport != "" {
		fmt.Println("\nfailure explanation:")
		fmt.Println(snap.FailureReport)
	}
}

func succeed(name string, d time.Duration) orchestrator.RunFunc {
	return func(ctx context.Context, attempt int) error {
		select {
		case <-time.After(d):
			fmt.Printf("[job] %s done\n", name)
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// flaky 前 failTimes 次返回可重试错误，之后成功，演示指数退避。
func flaky(failTimes int, d time.Duration) orchestrator.RunFunc {
	var n int
	return func(ctx context.Context, attempt int) error {
		n++
		select {
		case <-time.After(d):
		case <-ctx.Done():
			return ctx.Err()
		}
		if n <= failTimes {
			return orchestrator.Retryable(fmt.Errorf("fetch-data transient error #%d", n))
		}
		fmt.Println("[job] fetch-data done")
		return nil
	}
}
