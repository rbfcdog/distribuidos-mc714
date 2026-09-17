package stationary

import (
	"math"
	"testing"

	"mc714-t1/internal/balancer"
)

func TestAnalyzeStableThreeQueueModel(t *testing.T) {
	model, err := Analyze(2.4, 1, 3)
	if err != nil {
		t.Fatal(err)
	}
	if !model.Stable {
		t.Fatal("stable model marked unstable")
	}
	assertClose(t, model.Rho, 0.8)
	assertClose(t, model.P0, 0.2)
	assertClose(t, model.ExpectedJobs, 4)
	assertClose(t, model.ExpectedQueueWait, 4)
	assertClose(t, model.ExpectedResponse, 5)
	assertClose(t, model.Throughput, 2.4)
	assertClose(t, model.Utilization, 0.8)
}

func TestAnalyzeUnstableFluidModel(t *testing.T) {
	model, err := Analyze(UnstableLambda, 1, 3)
	if err != nil {
		t.Fatal(err)
	}
	if model.Stable {
		t.Fatal("unstable model marked stable")
	}
	assertClose(t, model.Rho, 1.1)
	assertClose(t, model.FluidBacklogRate, 0.3)
}

func TestRunMeasuresLittleLawAndConfidenceInterval(t *testing.T) {
	cfg := DefaultConfig(balancer.Random, 1.8)
	cfg.Horizon = 4000
	cfg.Warmup = 400
	summary, err := Run(cfg, 10, 42)
	if err != nil {
		t.Fatal(err)
	}
	if math.Abs(summary.MeanJobs-summary.MeanLittleRight) > 0.1 {
		t.Fatalf("L=%g, XW=%g", summary.MeanJobs, summary.MeanLittleRight)
	}
	if summary.CI95Response <= 0 {
		t.Fatalf("response CI = %g", summary.CI95Response)
	}
	if len(summary.MeanUtilizationByID) != DefaultServers {
		t.Fatalf("server utilizations = %d", len(summary.MeanUtilizationByID))
	}
}

func TestPoliciesOrderResponseAtHighStableLoad(t *testing.T) {
	run := func(policy balancer.Policy) Summary {
		cfg := DefaultConfig(policy, 2.7)
		cfg.Horizon = 5000
		cfg.Warmup = 500
		summary, err := Run(cfg, 10, 99)
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

func TestTraceCapturesQueueDynamics(t *testing.T) {
	cfg := UnstableConfig(balancer.ShortestQueue)
	cfg.Horizon = 100
	cfg.CaptureTrace = true
	summary, err := Run(cfg, 1, 7)
	if err != nil {
		t.Fatal(err)
	}
	if len(summary.Representative.Samples) == 0 {
		t.Fatal("missing trace samples")
	}
	if got := len(summary.Representative.Samples[0].Queues); got != DefaultServers {
		t.Fatalf("queue count = %d", got)
	}
}

func assertClose(t *testing.T, got, want float64) {
	t.Helper()
	if math.Abs(got-want) > 1e-12 {
		t.Fatalf("got %g, want %g", got, want)
	}
}
