package experiment

import (
	"testing"

	"mc714-t1/internal/balancer"
	"mc714-t1/internal/engine"
)

func TestRunAggregatesRequestedTrialCount(t *testing.T) {
	cfg := engine.DefaultConfig(balancer.RoundRobin, 30)
	summary, err := Run(cfg, 10, 42)
	if err != nil {
		t.Fatal(err)
	}
	if summary.Trials != 10 || summary.BurstSize != 30 || summary.Policy != string(balancer.RoundRobin) {
		t.Fatalf("unexpected summary identity: %#v", summary)
	}
	if summary.MeanCompleted != 30 || summary.MeanUnfinished != 0 {
		t.Fatalf("completed=%g unfinished=%g, want 30 and 0", summary.MeanCompleted, summary.MeanUnfinished)
	}
	if len(summary.Runs) != 10 {
		t.Fatalf("stored %d trials, want 10", len(summary.Runs))
	}
	if summary.StdThroughput < 0 || summary.FiniteHorizon.Throughput <= 0 {
		t.Fatalf("missing trial statistics or finite-horizon model: %#v", summary)
	}
	if len(summary.Representative.Samples) == 0 {
		t.Fatal("representative trial did not retain server monitoring samples")
	}
}

func TestRunRejectsZeroTrials(t *testing.T) {
	if _, err := Run(engine.DefaultConfig(balancer.Random, 30), 0, 42); err == nil {
		t.Fatal("Run accepted zero trials")
	}
}
