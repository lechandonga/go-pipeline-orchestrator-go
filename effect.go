package orchestrator

import "sync"

// Effects stores successful side effects for one run. A handler uses Once to
// make an external or simulated mutation exactly once across retries. A failed
// mutation is not cached, so a later attempt can perform it again.
type Effects struct {
	mu     sync.Mutex
	values map[string]any
}

func newEffects() *Effects {
	return &Effects{values: make(map[string]any)}
}

// Once runs fn only when key has no successful result. The returned value from
// the first successful call is reused on every retry.
func (e *Effects) Once(key string, fn func() (any, error)) (any, error) {
	e.mu.Lock()
	if value, ok := e.values[key]; ok {
		e.mu.Unlock()
		return value, nil
	}
	e.mu.Unlock()

	value, err := fn()
	if err != nil {
		return nil, err
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	if existing, ok := e.values[key]; ok {
		return existing, nil
	}
	e.values[key] = value
	return value, nil
}

func (e *Effects) snapshot() map[string]any {
	e.mu.Lock()
	defer e.mu.Unlock()

	result := make(map[string]any, len(e.values))
	for key, value := range e.values {
		result[key] = value
	}
	return result
}
