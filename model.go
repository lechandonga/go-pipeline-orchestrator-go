package orchestrator

import (
	"context"
	"time"
)

// JobStatus describes the lifecycle state of a single job.
type JobStatus string

const (
	JobPending   JobStatus = "pending"
	JobQueued    JobStatus = "queued"
	JobRunning   JobStatus = "running"
	JobRetrying  JobStatus = "retrying"
	JobSucceeded JobStatus = "succeeded"
	JobFailed    JobStatus = "failed"
	JobSkipped   JobStatus = "skipped"
	JobCancelled JobStatus = "cancelled"
)

// RunStatus describes the lifecycle state of a whole run.
type RunStatus string

const (
	RunRunning   RunStatus = "running"
	RunSucceeded RunStatus = "succeeded"
	RunFailed    RunStatus = "failed"
	RunCancelled RunStatus = "cancelled"
)

// Handler executes one job attempt. It must stop promptly when ctx is done.
// The returned error is retried unless wrapped with NonRetryable.
type Handler func(ctx context.Context, exec *Execution) error

// Backoff decides the wait before attempt number attempt (1-based: attempt 2
// is the first retry). Returning 0 means retry immediately.
type Backoff interface {
	Delay(attempt int) time.Duration
}

// BackoffFunc adapts a function to the Backoff interface.
type BackoffFunc func(attempt int) time.Duration

func (f BackoffFunc) Delay(attempt int) time.Duration {
	return f(attempt)
}

// ConstantBackoff waits the same duration before every retry.
func ConstantBackoff(delay time.Duration) Backoff {
	return BackoffFunc(func(int) time.Duration { return delay })
}

// ExponentialBackoff doubles the delay: base*2^(attempt-2) capped at max.
func ExponentialBackoff(base, max time.Duration) Backoff {
	return BackoffFunc(func(attempt int) time.Duration {
		if attempt <= 1 {
			return 0
		}
		shift := attempt - 2
		if shift > 30 {
			shift = 30
		}
		delay := base << shift
		if max > 0 && (delay > max || delay <= 0) {
			return max
		}
		return delay
	})
}

// Job is a registered unit of work.
type Job struct {
	Name      string
	DependsOn []string
	// MaxAttempts is the total number of attempts; values below 1 mean 1.
	MaxAttempts int
	// Timeout bounds every single attempt. Zero means no per-job timeout.
	Timeout time.Duration
	// Backoff controls waits between attempts. Defaults to no waiting.
	Backoff Backoff
	Handler Handler
}

// Execution is passed to each attempt.
type Execution struct {
	run     *Run
	jobName string
	attempt int
}

// JobName returns the executing job's name.
func (e *Execution) JobName() string { return e.jobName }

// Attempt returns the 1-based attempt number.
func (e *Execution) Attempt() int { return e.attempt }

// Effects returns the run-scoped side-effect store.
func (e *Execution) Effects() *Effects { return e.run.effects }

// Result returns the output produced by dependency's successful handler.
func (e *Execution) Result(dependency string) (any, bool) {
	return e.run.result(dependency)
}

// Event records one observable state transition.
type Event struct {
	Time   time.Time
	Job    string
	Type   string
	Detail string
}

// JobResult is an immutable observation of one job.
type JobResult struct {
	Name          string
	Status        JobStatus
	Attempts      int
	Retries       int
	DependsOn     []string
	StartedAt     time.Time
	FinishedAt    time.Time
	Duration      time.Duration
	RetryWaits    []time.Duration
	LastError     string
	NonRetryable  bool
	SkippedReason string
	Output        any
}

// Stats aggregates a run. Counts of terminal statuses always sum to Total.
type Stats struct {
	Total     int
	Pending   int
	Queued    int
	Running   int
	Retrying  int
	Succeeded int
	Failed    int
	Skipped   int
	Cancelled int
	Retries   int
}

// Snapshot is a point-in-time, self-consistent view of a run.
type Snapshot struct {
	RunID      string
	Status     RunStatus
	StartedAt  time.Time
	FinishedAt time.Time
	Duration   time.Duration
	Stats      Stats
	Jobs       map[string]JobResult
	Effects    map[string]any
}

// RunOptions controls one execution of the registry.
type RunOptions struct {
	ID              string
	Concurrency     int
	Timeout         time.Duration
	Clock           Clock
	ErrEventHandler func(Event)
}

// Clock allows tests to control scheduling time.
type Clock interface {
	Now() time.Time
	Sleep(ctx context.Context, duration time.Duration) error
}

type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }

func (realClock) Sleep(ctx context.Context, duration time.Duration) error {
	if duration <= 0 {
		return nil
	}
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
