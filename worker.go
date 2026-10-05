package orchestrator

import (
	"context"
	"fmt"
	"time"
)

type workerMessage struct {
	kind      string // started | retrying | finished
	job       string
	attempt   int
	wait      time.Duration
	err       error
	nonRetry  bool
	output    any
	startedAt time.Time
	endedAt   time.Time
	duration  time.Duration
}

// SetOutput stores the job result, readable by dependents via Execution.Result.
func (e *Execution) SetOutput(value any) {
	e.run.mu.Lock()
	e.run.outputs[e.jobName] = value
	e.run.mu.Unlock()
}

func (r *Run) runJob(id string, messages chan<- workerMessage) {
	job := r.jobs[id]
	attempt := 0

	for {
		attempt++
		startedAt := r.clock.Now()
		messages <- workerMessage{
			kind: "started", job: id, attempt: attempt, startedAt: startedAt,
		}

		attemptCtx := r.ctx
		var attemptCancel context.CancelFunc
		if job.Timeout > 0 {
			attemptCtx, attemptCancel = context.WithTimeout(r.ctx, job.Timeout)
		}

		exec := &Execution{run: r, jobName: id, attempt: attempt}
		err := safeRun(job.Handler, attemptCtx, exec)
		if attemptCancel != nil {
			attemptCancel()
		}
		endedAt := r.clock.Now()
		duration := endedAt.Sub(startedAt)

		if err == nil {
			output := r.takeOutput(id)
			messages <- workerMessage{
				kind: "finished", job: id, attempt: attempt,
				startedAt: startedAt, endedAt: endedAt,
				duration: duration, output: output,
			}
			return
		}

		nonRetry := IsNonRetryable(err)
		cancelled := r.ctx.Err() != nil
		canRetry := !nonRetry && !cancelled && attempt < job.MaxAttempts
		if !canRetry {
			messages <- workerMessage{
				kind: "finished", job: id, attempt: attempt, err: err,
				nonRetry: nonRetry, startedAt: startedAt, endedAt: endedAt,
				duration: duration,
			}
			return
		}

		wait := time.Duration(0)
		if job.Backoff != nil {
			wait = job.Backoff.Delay(attempt + 1)
		}
		messages <- workerMessage{
			kind: "retrying", job: id, attempt: attempt, wait: wait, err: err,
			startedAt: startedAt, endedAt: endedAt, duration: duration,
		}

		if sleepErr := r.clock.Sleep(r.ctx, wait); sleepErr != nil {
			messages <- workerMessage{
				kind: "finished", job: id, attempt: attempt, err: sleepErr,
				startedAt: startedAt, endedAt: r.clock.Now(),
				duration: duration,
			}
			return
		}
	}
}

func (r *Run) takeOutput(id string) any {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.outputs[id]
}

func safeRun(handler Handler, ctx context.Context, exec *Execution) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = NonRetryable(fmt.Errorf("handler panic: %v", recovered))
		}
	}()
	return handler(ctx, exec)
}

// Snapshot returns a point-in-time, self-consistent view safe for concurrent
// querying. Every status is read under the same lock, so the aggregated counts
// and per-job details always describe the same instant.
func (r *Run) Snapshot() Snapshot {
	r.mu.RLock()
	defer r.mu.RUnlock()

	jobs := make(map[string]JobResult, len(r.results))
	stats := Stats{Total: len(r.results)}

	for id, result := range r.results {
		copied := *result
		copied.DependsOn = append([]string(nil), result.DependsOn...)
		copied.RetryWaits = append([]time.Duration(nil), result.RetryWaits...)
		jobs[id] = copied

		stats.Retries += result.Retries
		switch result.Status {
		case JobPending:
			stats.Pending++
		case JobQueued:
			stats.Queued++
		case JobRunning:
			stats.Running++
		case JobRetrying:
			stats.Retrying++
		case JobSucceeded:
			stats.Succeeded++
		case JobFailed:
			stats.Failed++
		case JobSkipped:
			stats.Skipped++
		case JobCancelled:
			stats.Cancelled++
		}
	}

	finishedAt := r.finishedAt
	duration := time.Duration(0)
	if !finishedAt.IsZero() {
		duration = finishedAt.Sub(r.startedAt)
	} else {
		finishedAt = r.clock.Now()
		duration = finishedAt.Sub(r.startedAt)
	}

	return Snapshot{
		RunID:      r.id,
		Status:     r.status,
		StartedAt:  r.startedAt,
		FinishedAt: finishedAt,
		Duration:   duration,
		Stats:      stats,
		Jobs:       jobs,
		Effects:    r.effects.snapshot(),
	}
}

// Wait blocks until the run finishes.
func (r *Run) Wait() {
	<-r.done
}

// Status returns the current run status.
func (r *Run) Status() RunStatus {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.status
}
