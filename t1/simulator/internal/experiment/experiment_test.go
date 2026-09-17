package experiment

import (
	"math"
	"slices"
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
	if summary.MeanCompleted != 30 || summary.MeanUnfinished != 0 || summary.MeanRejectedFull != 0 {
		t.Fatalf("completed=%g unfinished=%g rejected=%g, want 30, 0, 0", summary.MeanCompleted, summary.MeanUnfinished, summary.MeanRejectedFull)
	}
	if len(summary.Runs) != 10 {
		t.Fatalf("stored %d trials, want 10", len(summary.Runs))
	}
	if math.Abs(summary.MeanThroughput-0.15) > 1e-12 || math.Abs(summary.Analytical.Throughput-0.15) > 1e-12 {
		t.Fatalf("simulation/model throughput = %g/%g, want 0.15", summary.MeanThroughput, summary.Analytical.Throughput)
	}
	if len(summary.Representative.Samples) == 0 {
		t.Fatal("representative trial did not retain server monitoring samples")
	}
}

func TestRunPairsPoliciesButSeparatesBurstTraffic(t *testing.T) {
	random30, err := Run(engine.DefaultConfig(balancer.Random, 30), 1, 42)
	if err != nil {
		t.Fatal(err)
	}
	roundRobin30, err := Run(engine.DefaultConfig(balancer.RoundRobin, 30), 1, 42)
	if err != nil {
		t.Fatal(err)
	}
	roundRobin60, err := Run(engine.DefaultConfig(balancer.RoundRobin, 60), 1, 42)
	if err != nil {
		t.Fatal(err)
	}

	if !slices.Equal(random30.Representative.Interarrivals, roundRobin30.Representative.Interarrivals) {
		t.Fatal("policies within the same burst did not receive paired traffic")
	}
	if slices.Equal(roundRobin30.Representative.Interarrivals, roundRobin60.Representative.Interarrivals[:30]) {
		t.Fatal("different burst-size experiments reused the same traffic prefix")
	}
}

func TestRunRejectsZeroTrials(t *testing.T) {
	if _, err := Run(engine.DefaultConfig(balancer.Random, 30), 0, 42); err == nil {
		t.Fatal("Run accepted zero trials")
	}
}
