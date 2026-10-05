package orchestrator_test

import (
	"context"
	"errors"
	"fmt"
	"time"

	orchestrator "github.com/lechandonga/go-pipeline-orchestrator-go"
)

func Example() {
	registry := orchestrator.NewRegistry()

	registry.MustRegister(orchestrator.Job{
		Name: "extract",
		Handler: func(_ context.Context, exec *orchestrator.Execution) error {
			_, err := exec.Effects().Once("extract", func() (any, error) {
				return []string{"a", "b"}, nil
			})
			return err
		},
	})

	attempts := 0
	registry.MustRegister(orchestrator.Job{
		Name:        "load",
		DependsOn:   []string{"extract"},
		MaxAttempts: 3,
		Backoff:     orchestrator.ConstantBackoff(time.Millisecond),
		Handler: func(_ context.Context, _ *orchestrator.Execution) error {
			attempts++
			if attempts < 2 {
				return errors.New("transient")
			}
			return nil
		},
	})

	run, err := registry.Run(context.Background(), orchestrator.RunOptions{
		ID:          "example",
		Concurrency: 2,
	})
	if err != nil {
		fmt.Println("validation error:", err)
		return
	}

	snapshot := run.Snapshot()
	fmt.Println("run:", snapshot.Status)
	fmt.Println("extract:", snapshot.Jobs["extract"].Status)
	fmt.Println("load:", snapshot.Jobs["load"].Status, "retries:", snapshot.Stats.Retries)
	// Output:
	// run: succeeded
	// extract: succeeded
	// load: succeeded retries: 1
}
