package orchestrator

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"
)

// Run is one execution of a registry. Snapshot is safe for concurrent queries.
type Run struct {
	id          string
	jobs        map[string]*Job
	dag         *graph
	concurrency int
	clock       Clock
	onEvent     func(Event)
	timeout     time.Duration

	ctx     context.Context
	cancel  context.CancelFunc
	aborted bool

	mu            sync.RWMutex
	status        RunStatus
	results       map[string]*JobResult
	workersActive map[string]struct{}
	events        []Event
	outputs       map[string]any
	startedAt     time.Time
	finishedAt    time.Time
	done          chan struct{}

	effects *Effects
}

func newRun(options RunOptions, jobs map[string]*Job, dag *graph) *Run {
	clock := options.Clock
	if clock == nil {
		clock = realClock{}
	}
	concurrency := options.Concurrency
	if concurrency < 1 {
		concurrency = 1
	}

	results := make(map[string]*JobResult, len(dag.ids))
	for _, id := range dag.ids {
		job := jobs[id]
		results[id] = &JobResult{
			Name:      id,
			Status:    JobPending,
			DependsOn: append([]string(nil), job.DependsOn...),
		}
	}

	return &Run{
		id:            options.ID,
		jobs:          jobs,
		dag:           dag,
		concurrency:   concurrency,
		clock:         clock,
		onEvent:       options.ErrEventHandler,
		timeout:       options.Timeout,
		status:        RunRunning,
		results:       results,
		workersActive: make(map[string]struct{}),
		outputs:       make(map[string]any),
		done:          make(chan struct{}),
		effects:       newEffects(),
	}
}

func (r *Run) execute(parent context.Context) {
	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel := context.WithCancel(parent)
	if r.timeout > 0 {
		ctx, cancel = context.WithTimeout(parent, r.timeout)
	}
	r.ctx = ctx
	r.cancel = cancel

	r.mu.Lock()
	r.startedAt = r.clock.Now()
	r.mu.Unlock()

	r.dispatch()

	r.mu.Lock()
	r.finishedAt = r.clock.Now()
	if r.finishedAt.Before(r.startedAt) {
		r.finishedAt = r.startedAt
	}
	r.status = r.deriveFinalStatusLocked()
	for _, result := range r.results {
		if !isTerminal(result.Status) {
			result.Status = JobCancelled
		}
	}
	r.mu.Unlock()

	cancel()
	close(r.done)
}

func isTerminal(status JobStatus) bool {
	switch status {
	case JobSucceeded, JobFailed, JobSkipped, JobCancelled:
		return true
	}
	return false
}

func (r *Run) deriveFinalStatusLocked() RunStatus {
	if r.aborted {
		return RunCancelled
	}
	for _, result := range r.results {
		if result.Status == JobFailed || result.Status == JobSkipped {
			return RunFailed
		}
	}
	return RunSucceeded
}

func (r *Run) dispatch() {
	remaining := make(map[string]int, len(r.results))
	ready := make([]string, 0, len(r.results))
	for id, degree := range r.dag.indegree {
		remaining[id] = degree
		if degree == 0 {
			ready = append(ready, id)
		}
	}

	inFlight := 0
	messages := make(chan workerMessage, 16)

	enqueue := func() {
		sort.Strings(ready)
		kept := ready[:0]
		for _, id := range ready {
			if r.aborted || r.ctx.Err() != nil {
				kept = append(kept, id)
				continue
			}
			if inFlight >= r.concurrency {
				kept = append(kept, id)
				continue
			}
			result := r.results[id]
			if result.Status != JobPending {
				continue
			}
			result.Status = JobQueued
			r.appendEventLocked(id, "queued", "ready to execute")
			inFlight++
			r.workersActive[id] = struct{}{}
			go r.runJob(id, messages)
		}
		ready = kept
	}

	for {
		r.mu.Lock()
		enqueue()
		pendingCount := 0
		for id, result := range r.results {
			_, active := r.workersActive[id]
			if !isTerminal(result.Status) || active {
				pendingCount++
			}
		}
		r.mu.Unlock()

		if pendingCount == 0 {
			return
		}

		select {
		case msg := <-messages:
			r.handleMessage(msg, remaining, &ready, &inFlight)
		case <-r.ctx.Done():
			r.markCancelled()
			r.drainWorkers(messages, remaining, &ready, &inFlight)
			return
		}
	}
}

// drainWorkers consumes terminal messages from workers whose context was
// cancelled so their goroutines never block on the dispatcher channel.
func (r *Run) drainWorkers(
	messages <-chan workerMessage,
	remaining map[string]int,
	ready *[]string,
	inFlight *int,
) {
	r.mu.RLock()
	pending := make(map[string]struct{}, len(r.workersActive))
	for id := range r.workersActive {
		pending[id] = struct{}{}
	}
	r.mu.RUnlock()

	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	for len(pending) > 0 {
		select {
		case msg := <-messages:
			if msg.kind == "finished" {
				r.handleMessage(msg, remaining, ready, inFlight)
				delete(pending, msg.job)
			}
		case <-deadline.C:
			return
		}
	}
}

func (r *Run) markCancelled() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.aborted = true
	for id, result := range r.results {
		_, active := r.workersActive[id]
		if !isTerminal(result.Status) && !active {
			result.Status = JobCancelled
			r.appendEventLocked(id, "cancelled", "run cancelled or timed out before completion")
		}
	}
}

func (r *Run) handleMessage(
	msg workerMessage,
	remaining map[string]int,
	ready *[]string,
	inFlight *int,
) {
	r.mu.Lock()
	result := r.results[msg.job]

	switch msg.kind {
	case "started":
		result.Attempts = msg.attempt
		result.StartedAt = msg.startedAt
		result.Status = JobRunning
		r.appendEventLocked(msg.job, "attempt_started",
			fmt.Sprintf("attempt=%d", msg.attempt))
		r.mu.Unlock()

	case "retrying":
		result.FinishedAt = msg.endedAt
		result.Duration += msg.duration
		result.Retries++
		result.LastError = msg.err.Error()
		result.RetryWaits = append(result.RetryWaits, msg.wait)
		result.Status = JobRetrying
		r.appendEventLocked(msg.job, "retry_scheduled",
			fmt.Sprintf("after_attempt=%d error=%q wait=%s", msg.attempt, msg.err, msg.wait))
		r.mu.Unlock()
		// The worker goroutine keeps its slot while sleeping in backoff.

	case "finished":
		result.FinishedAt = msg.endedAt
		result.Duration += msg.duration
		delete(r.workersActive, msg.job)
		*inFlight--

		if r.aborted {
			result.LastError = context.Canceled.Error()
			result.Status = JobCancelled
			r.appendEventLocked(msg.job, "cancelled",
				fmt.Sprintf("attempts=%d run already aborted", msg.attempt))
			r.mu.Unlock()
			return
		}

		if msg.err == nil {
			if r.ctx.Err() != nil {
				result.Status = JobCancelled
				result.Output = nil
				r.appendEventLocked(msg.job, "cancelled",
					"success ignored because run context was cancelled")
				r.mu.Unlock()
				return
			}
			result.Status = JobSucceeded
			result.LastError = ""
			result.Output = msg.output
			r.appendEventLocked(msg.job, "succeeded",
				fmt.Sprintf("attempts=%d duration=%s", msg.attempt, result.Duration))
			r.mu.Unlock()

			next := append([]string(nil), r.dag.dependents[msg.job]...)
			sort.Strings(next)
			for _, dependent := range next {
				remaining[dependent]--
				if remaining[dependent] == 0 {
					*ready = append(*ready, dependent)
				}
			}
			return
		}

		result.LastError = msg.err.Error()
		result.NonRetryable = msg.nonRetry
		result.Status = JobFailed
		r.appendEventLocked(msg.job, string(result.Status),
			fmt.Sprintf("attempts=%d non_retryable=%t error=%q",
				msg.attempt, msg.nonRetry, msg.err))

		if result.Status == JobFailed {
			for _, id := range r.transitiveDependents(msg.job) {
				target := r.results[id]
				if target.Status == JobPending {
					target.Status = JobSkipped
					target.SkippedReason = "upstream job failed: " + msg.job
					r.appendEventLocked(id, "skipped", target.SkippedReason)
				}
			}
		}
		r.mu.Unlock()
	}
}

func (r *Run) transitiveDependents(root string) []string {
	visited := make(map[string]struct{})
	stack := append([]string(nil), r.dag.dependents[root]...)
	for len(stack) > 0 {
		id := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if _, seen := visited[id]; seen {
			continue
		}
		visited[id] = struct{}{}
		stack = append(stack, r.dag.dependents[id]...)
	}
	result := make([]string, 0, len(visited))
	for id := range visited {
		result = append(result, id)
	}
	sort.Strings(result)
	return result
}

func (r *Run) appendEventLocked(job, eventType, detail string) {
	event := Event{Time: r.clock.Now(), Job: job, Type: eventType, Detail: detail}
	r.events = append(r.events, event)
	if r.onEvent != nil {
		r.onEvent(event)
	}
}

func (r *Run) result(dependency string) (any, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	result, ok := r.results[dependency]
	if !ok || result.Status != JobSucceeded {
		return nil, false
	}
	return result.Output, true
}

// ID returns the run identifier.
func (r *Run) ID() string { return r.id }

// Cancel requests cancellation. A finished run returns ErrAlreadyFinished.
func (r *Run) Cancel() error {
	r.mu.RLock()
	finished := r.status != RunRunning
	cancel := r.cancel
	r.mu.RUnlock()
	if finished {
		return ErrAlreadyFinished
	}
	r.mu.Lock()
	r.aborted = true
	r.mu.Unlock()
	cancel()
	return nil
}

// Done is closed when the run reaches a terminal state.
func (r *Run) Done() <-chan struct{} { return r.done }

// Events returns a chronological copy of state transitions.
func (r *Run) Events() []Event {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return append([]Event(nil), r.events...)
}
