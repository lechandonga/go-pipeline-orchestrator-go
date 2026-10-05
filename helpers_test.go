package orchestrator

import (
	"context"
	"log"
	"sync"
)

var (
	coverageMu   sync.Mutex
	coverageSeen = map[string]bool{}
)

func logBusiness(name string) {
	coverageMu.Lock()
	defer coverageMu.Unlock()
	coverageSeen[name] = true
	log.Printf("===== 业务覆盖: %s =====", name)
}

func logCase(name string, passed bool, detail any) {
	status := "PASS"
	if !passed {
		status = "FAIL"
	}
	log.Printf("[%s] 用例: %s => %v", status, name, detail)
}

// startRun launches Run in a goroutine so the caller can cancel mid-flight.
func startRun(registry *Registry, options RunOptions) (*Run, error) {
	dag, errs := buildGraph(registry.jobs)
	if errs != nil {
		return nil, errs[0]
	}
	run := newRun(options, registry.jobs, dag)
	go run.execute(context.Background())
	return run, nil
}
