package engine

import (
	rand "math/rand/v2"
	"testing"

	"mc714-t1/internal/balancer"
	"mc714-t1/pkg/mathutil"
)

func TestRunQueuesAfterConcurrentCapacity(t *testing.T) {
	arrivals, err := mathutil.NewBoundedPareto(0.001, 0.001, 1.4)
	if err != nil {
		t.Fatal(err)
	}
	cfg := Config{
		Policy:         balancer.RoundRobin,
		RequestCount:   30,
		Horizon:        1,
		ServiceTime:    0.05,
		ServerCount:    1,
		ServerCapacity: 15,
		InterArrival:   arrivals,
	}
	result, err := Run(cfg, newRNG(1), newRNG(2))
	if err != nil {
		t.Fatal(err)
	}
	if result.Completed != 30 || result.Unfinished != 0 {
		t.Fatalf("completed=%d unfinished=%d, want 30 and 0", result.Completed, result.Unfinished)
	}
	if result.Servers[0].Assigned != 30 || result.Servers[0].Completed != 30 {
		t.Fatalf("server result = %#v, want all requests assigned and completed", result.Servers[0])
	}
	if result.AverageResponseTime <= cfg.ServiceTime {
		t.Fatalf("average response time = %g, want queueing delay above service time %g", result.AverageResponseTime, cfg.ServiceTime)
	}

	maxActive, maxQueue := 0, 0
	for _, sample := range result.Samples {
		if sample.Active > maxActive {
			maxActive = sample.Active
		}
		if sample.QueueLength > maxQueue {
			maxQueue = sample.QueueLength
		}
	}
	if maxActive != 15 || maxQueue != 15 {
		t.Fatalf("max active=%d max queue=%d, want 15 and 15", maxActive, maxQueue)
	}
}

func TestRunStopsAtHorizon(t *testing.T) {
	arrivals, err := mathutil.NewBoundedPareto(0.005, 0.005, 1.4)
	if err != nil {
		t.Fatal(err)
	}
	cfg := Config{
		Policy:         balancer.RoundRobin,
		RequestCount:   3,
		Horizon:        0.01,
		ServiceTime:    0.05,
		ServerCount:    1,
		ServerCapacity: 15,
		InterArrival:   arrivals,
	}
	result, err := Run(cfg, newRNG(1), newRNG(2))
	if err != nil {
		t.Fatal(err)
	}
	if result.Duration != cfg.Horizon {
		t.Fatalf("duration = %g, want horizon %g", result.Duration, cfg.Horizon)
	}
	if result.Accepted != 2 || result.DiscardedAtHorizon != 1 || result.Completed != 0 || result.Unfinished != 2 {
		t.Fatalf("accepted=%d discarded=%d completed=%d unfinished=%d, want 2, 1, 0, 2", result.Accepted, result.DiscardedAtHorizon, result.Completed, result.Unfinished)
	}
}

func TestDefaultConfigMatchesAssignmentParameters(t *testing.T) {
	cfg := DefaultConfig(balancer.Random, 30)
	if cfg.ServerCount != 3 || cfg.ServerCapacity != 15 || cfg.ServiceTime != 0.05 || cfg.Horizon != 200 {
		t.Fatalf("unexpected default config: %#v", cfg)
	}
	if cfg.InterArrival.Alpha != 1.4 {
		t.Fatalf("alpha = %g, want Hurst-derived 1.4", cfg.InterArrival.Alpha)
	}
}

func newRNG(seed uint64) *rand.Rand {
	return rand.New(rand.NewPCG(seed, seed+1))
}
