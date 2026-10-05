package orchestrator

import (
	"errors"
	"fmt"
)

var (
	// ErrDuplicateJob is returned when the same job ID is registered twice.
	ErrDuplicateJob = errors.New("duplicate job registration")
	// ErrUnknownDependency is returned when a job depends on a job that was never registered.
	ErrUnknownDependency = errors.New("unknown job dependency")
	// ErrDependencyCycle is returned when dependencies form a directed cycle.
	ErrDependencyCycle = errors.New("dependency cycle detected")
	// ErrNoJobs is returned when an empty registry is executed.
	ErrNoJobs = errors.New("no jobs registered")
	// ErrAlreadyFinished is returned when a finished run is cancelled again.
	ErrAlreadyFinished = errors.New("run already finished")
)

// ValidationError keeps the machine-readable reason and offending job IDs.
type ValidationError struct {
	Reason error
	JobIDs []string
}

func (e *ValidationError) Error() string {
	return fmt.Sprintf("%v: %v", e.Reason, e.JobIDs)
}

func (e *ValidationError) Unwrap() error {
	return e.Reason
}

// NonRetryableError marks an execution error as terminal. The job fails without
// waiting for another attempt.
type NonRetryableError struct {
	Err error
}

func (e *NonRetryableError) Error() string {
	if e.Err == nil {
		return "non-retryable error"
	}
	return e.Err.Error()
}

func (e *NonRetryableError) Unwrap() error {
	return e.Err
}

// NonRetryable wraps err so the scheduler does not retry the job.
func NonRetryable(err error) error {
	return &NonRetryableError{Err: err}
}

// IsNonRetryable reports whether err is explicitly marked non-retryable.
func IsNonRetryable(err error) bool {
	var target *NonRetryableError
	return errors.As(err, &target)
}

// RetryableError marks an execution error that may be retried. Any error that
// is not wrapped with NonRetryable is also considered retryable by default.
type RetryableError struct {
	Err error
}

func (e *RetryableError) Error() string {
	if e.Err == nil {
		return "retryable error"
	}
	return e.Err.Error()
}

func (e *RetryableError) Unwrap() error {
	return e.Err
}

// Retryable wraps err and explicitly marks it retryable.
func Retryable(err error) error {
	return &RetryableError{Err: err}
}

// IsRetryable reports whether the error is eligible for another attempt.
func IsRetryable(err error) bool {
	if err == nil {
		return false
	}
	return !IsNonRetryable(err)
}
