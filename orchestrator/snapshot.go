package orchestrator

import (
	"fmt"
	"strings"
	"time"
)

// Snapshot 在读锁下一次性复制全部观测数据，保证并发查询看到的
// 作业状态、聚合计数与耗时来自同一时刻，不存在自相矛盾的中间态。
func (run *Run) Snapshot() RunSnapshot {
	now := run.plan.engine.now()
	run.mu.RLock()
	defer run.mu.RUnlock()
	snap := RunSnapshot{
		RunID:         run.id,
		State:         run.state,
		StartedAt:     run.startTime,
		EndedAt:       run.endTime,
		TotalRetries:  run.retries,
		TotalAttempts: run.attempts,
	}

	for _, name := range run.order {
		jt := run.jobs[name]
		jr := JobResult{
			Name:      name,
			State:     jt.state,
			Attempts:  jt.attempts,
			Retries:   maxInt(jt.attempts-1, 0),
			LastError: jt.lastErr,
			DependsOn: append([]string{}, jt.spec.Deps...),
			StartedAt: jt.startedAt,
			EndedAt:   jt.endedAt,
		}
		switch {
		case !jt.startedAt.IsZero() && !jt.endedAt.IsZero():
			jr.Duration = jt.endedAt.Sub(jt.startedAt)
		case !jt.startedAt.IsZero():
			jr.Duration = now.Sub(jt.startedAt)
		}
		snap.Jobs = append(snap.Jobs, jr)
		snap.Total++
		switch jr.State {
		case JobSucceeded:
			snap.Succeeded++
		case JobFailed:
			snap.Failed++
		case JobSkipped:
			snap.Skipped++
		case JobCanceled:
			snap.Canceled++
		case JobRunning:
			snap.Running++
		case JobPending, JobReady:
			snap.Pending++
		}
	}

	if !snap.StartedAt.IsZero() {
		if !snap.EndedAt.IsZero() {
			snap.Duration = snap.EndedAt.Sub(snap.StartedAt)
		} else {
			snap.Duration = now.Sub(snap.StartedAt)
		}
	}

	// 终态附带失败因果报告；运行中则现场生成（读路径无副作用）。
	if run.state == RunFailed || run.state == RunCanceled || run.state == RunTimedOut {
		snap.FailureReport = run.buildFailureReport()
	}

	// 自洽性兜底：非终态的快照中状态分桶之和必须等于作业总数。
	return snap
}

// State 返回当前运行状态的轻量查询。
func (run *Run) State() RunState {
	run.mu.RLock()
	defer run.mu.RUnlock()
	return run.state
}

// buildFailureReport 在锁外安全地遍历（jobRuntime 的字段由协调器单线程写，
// 终态后不再变化）。
func (run *Run) buildFailureReport() string {
	return run.buildFailureReportLocked()
}

// buildFailureReportLocked 输出“根因作业 -> 被波及的下游”的人类可读说明。
func (run *Run) buildFailureReportLocked() string {
	var roots, canceled, skipped []string
	skippedCause := map[string]string{}

	for _, name := range run.order {
		jt := run.jobs[name]
		switch jt.state {
		case JobFailed:
			roots = append(roots, fmt.Sprintf("- %s: %s (attempts=%d)", name, jt.lastErr, jt.attempts))
		case JobCanceled:
			canceled = append(canceled, fmt.Sprintf("- %s: %s (attempts=%d)", name, jt.lastErr, jt.attempts))
		case JobSkipped:
			skipped = append(skipped, name)
			skippedCause[name] = jt.lastErr
		}
	}

	var b strings.Builder
	if len(roots) > 0 {
		b.WriteString("root failures:\n")
		b.WriteString(strings.Join(roots, "\n"))
		b.WriteString("\n")
	}
	if len(canceled) > 0 {
		b.WriteString("stopped by cancellation/timeout:\n")
		b.WriteString(strings.Join(canceled, "\n"))
		b.WriteString("\n")
	}
	if len(skipped) > 0 {
		b.WriteString("downstream skipped (unaffected branches still ran):\n")
		for _, name := range skipped {
			b.WriteString(fmt.Sprintf("- %s: %s\n", name, skippedCause[name]))
		}
	}
	return strings.TrimRight(b.String(), "\n")
}

// backoff 计算第 failedAttempt 次失败后的退避时长：base * 2^(n-1)，
// 叠加比例内的对称抖动，使并发重试不会齐步唤醒。
func (run *Run) backoff(spec *JobSpec, failedAttempt int) time.Duration {
	base := spec.RetryBackoff
	if base <= 0 {
		return 0
	}
	d := base
	for i := 1; i < failedAttempt; i++ {
		d *= 2
	}
	jf := run.plan.engine.opts.BackoffJitter
	if jf > 0 {
		if jf > 0.9 {
			jf = 0.9
		}
		run.plan.engine.rngMu.Lock()
		factor := 1 + (run.plan.engine.rng.Float64()*2-1)*jf
		run.plan.engine.rngMu.Unlock()
		d = time.Duration(float64(d) * factor)
		if d < 0 {
			d = 0
		}
	}
	return d
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
