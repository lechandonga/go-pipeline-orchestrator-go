package orchestrator

import (
	"context"
	"fmt"
)

// Registry is a local, in-memory catalog of jobs.
type Registry struct {
	jobs map[string]*Job
}

// NewRegistry creates an empty registry.
func NewRegistry() *Registry {
	return &Registry{jobs: make(map[string]*Job)}
}

// Register adds a job. It fails with ErrDuplicateJob if name already exists.
func (r *Registry) Register(job Job) error {
	if job.Name == "" {
		return fmt.Errorf("job name must not be empty")
	}
	if job.Handler == nil {
		return fmt.Errorf("job %q handler must not be nil", job.Name)
	}
	if _, exists := r.jobs[job.Name]; exists {
		return fmt.Errorf("%w: %q", ErrDuplicateJob, job.Name)
	}
	copied := job
	copied.DependsOn = append([]string(nil), job.DependsOn...)
	if copied.MaxAttempts < 1 {
		copied.MaxAttempts = 1
	}
	r.jobs[copied.Name] = &copied
	return nil
}

// MustRegister panics when registration fails; intended for startup wiring.
func (r *Registry) MustRegister(job Job) {
	if err := r.Register(job); err != nil {
		panic(err)
	}
}

// Len reports how many jobs are registered.
func (r *Registry) Len() int {
	return len(r.jobs)
}

// Validate checks the whole DAG before any handler is started.
func (r *Registry) Validate() []error {
	_, errs := buildGraph(r.jobs)
	return errs
}

// Run executes all registered jobs and blocks until the run reaches a terminal
// state. Validation happens before any goroutine is started.
func (r *Registry) Run(parent context.Context, options RunOptions) (*Run, error) {
	dag, errs := buildGraph(r.jobs)
	if errs != nil {
		return nil, errs[0]
	}

	run := newRun(options, r.jobs, dag)
	run.execute(parent)
	return run, nil
}
