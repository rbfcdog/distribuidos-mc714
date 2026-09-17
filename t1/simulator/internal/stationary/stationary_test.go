package stationary

import (
	"math"
	"testing"

	"mc714-t1/internal/balancer"
)

func TestAnalyzeStableThreeQueueModel(t *testing.T) {
	model, err := Analyze(2.1, 1, 3)
	if err != nil {
		t.Fatal(err)
	}
	if !model.Stable {
		t.Fatal("stable model marked unstable")
	}
	assertClose(t, model.Rho, 0.7)
	assertClose(t, model.P0, 0.3)
	assertClose(t, model.ExpectedJobs, 7.0/3.0)
	assertClose(t, model.ExpectedQueueWait, 7.0/3.0)
	assertClose(t, model.ExpectedResponse, 10.0/3.0)
	assertClose(t, model.Throughput, 2.1)
	assertClose(t, model.Utilization, 0.7)
}

func TestAnalyzeUnstableFluidModel(t *testing.T) {
	model, err := Analyze(3.3, 1, 3)
	if err != nil {
		t.Fatal(err)
	}
	if model.Stable {
		t.Fatal("unstable model marked stable")
	}
	assertClose(t, model.Rho, 1.1)
	assertClose(t, model.FluidBacklogRate, 0.3)
}

func TestRunApproximatesLittleLaw(t *testing.T) {
	cfg := StableConfig(balancer.Random, 1.5)
	cfg.Horizon = 8000
	cfg.Warmup = 800
	summary, err := Run(cfg, 3, 42)
	if err != nil {
		t.Fatal(err)
	}
	if math.Abs(summary.MeanJobs-summary.MeanLittleRight) > 0.15 {
		t.Fatalf("L=%g, XW=%g", summary.MeanJobs, summary.MeanLittleRight)
	}
}

func TestPoliciesOrderResponseAtHighStableLoad(t *testing.T) {
	run := func(policy balancer.Policy) Summary {
		cfg := StableConfig(policy, 2.7)
		cfg.Horizon = 6000
		cfg.Warmup = 600
		summary, err := Run(cfg, 3, 99)
		if err != nil {
			t.Fatal(err)
		}
		return summary
	}
	random := run(balancer.Random)
	roundRobin := run(balancer.RoundRobin)
	shortestQueue := run(balancer.ShortestQueue)
	if !(shortestQueue.MeanResponse <= roundRobin.MeanResponse && roundRobin.MeanResponse <= random.MeanResponse) {
		t.Fatalf("responses shortest=%g round-robin=%g random=%g", shortestQueue.MeanResponse, roundRobin.MeanResponse, random.MeanResponse)
	}
}

func assertClose(t *testing.T, got, want float64) {
	t.Helper()
	if math.Abs(got-want) > 1e-12 {
		t.Fatalf("got %g, want %g", got, want)
	}
}
