package orchestrator

import "sort"

type graph struct {
	// dependents maps a job to jobs that must start after it succeeds.
	dependents map[string][]string
	indegree   map[string]int
	ids        []string
}

func buildGraph(jobs map[string]*Job) (*graph, []error) {
	if len(jobs) == 0 {
		return nil, []error{ErrNoJobs}
	}

	ids := make([]string, 0, len(jobs))
	for id := range jobs {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	var errs []error
	unknown := make(map[string]struct{})
	dependents := make(map[string][]string, len(jobs))
	indegree := make(map[string]int, len(jobs))

	for _, id := range ids {
		indegree[id] = 0
	}

	for _, id := range ids {
		seen := make(map[string]struct{}, len(jobs[id].DependsOn))
		for _, dependency := range jobs[id].DependsOn {
			if _, ok := jobs[dependency]; !ok {
				unknown[dependency] = struct{}{}
				continue
			}
			if dependency == id {
				unknown[dependency] = struct{}{}
				continue
			}
			if _, duplicated := seen[dependency]; duplicated {
				continue
			}
			seen[dependency] = struct{}{}
			dependents[dependency] = append(dependents[dependency], id)
			indegree[id]++
		}
	}

	if len(unknown) > 0 {
		errs = append(errs, &ValidationError{Reason: ErrUnknownDependency, JobIDs: sortedKeys(unknown)})
	}

	// Kahn's algorithm: vertices that never become ready belong to or depend
	// transitively on a cycle.
	remaining := make(map[string]int, len(indegree))
	for id, degree := range indegree {
		remaining[id] = degree
	}
	queue := make([]string, 0)
	for _, id := range ids {
		if remaining[id] == 0 {
			queue = append(queue, id)
		}
	}
	visited := 0
	for len(queue) > 0 {
		id := queue[0]
		queue = queue[1:]
		visited++
		next := append([]string(nil), dependents[id]...)
		sort.Strings(next)
		for _, dependent := range next {
			remaining[dependent]--
			if remaining[dependent] == 0 {
				queue = append(queue, dependent)
			}
		}
	}

	if visited != len(jobs) {
		cyclic := make(map[string]struct{})
		for id, degree := range remaining {
			if degree > 0 {
				cyclic[id] = struct{}{}
			}
		}
		errs = append(errs, &ValidationError{Reason: ErrDependencyCycle, JobIDs: sortedKeys(cyclic)})
	}

	if len(errs) > 0 {
		return nil, errs
	}

	for id := range dependents {
		sort.Strings(dependents[id])
	}

	return &graph{dependents: dependents, indegree: indegree, ids: ids}, nil
}

func sortedKeys(values map[string]struct{}) []string {
	result := make([]string, 0, len(values))
	for value := range values {
		result = append(result, value)
	}
	sort.Strings(result)
	return result
}
