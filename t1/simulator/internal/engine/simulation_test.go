package engine

import (
	"container/heap"
	"math"
	rand "math/rand/v2"
	"testing"

	"mc714-t1/internal/balancer"
	"mc714-t1/pkg/mathutil"
)

func TestRunUsesEntireExperimentHorizonForMetrics(t *testing.T) {
	cfg := testConfig(30, 1, []ServerConfig{{Capacity: 15, ServiceTime: 0.05, BufferCapacity: 30}})
	result, err := Run(cfg, newRNG(1), newRNG(2))
	if err != nil {
		t.Fatal(err)
	}

	if result.Duration != cfg.Horizon {
		t.Fatalf("duration = %g, want fixed horizon %g", result.Duration, cfg.Horizon)
	}
	if result.Throughput != 30 {
		t.Fatalf("throughput = %g, want completed/horizon = 30", result.Throughput)
	}
	if math.Abs(result.Utilization-0.1) > 1e-12 {
		t.Fatalf("utilization = %g, want 0.1", result.Utilization)
	}
}

func TestRunQueuesAfterConcurrentCapacity(t *testing.T) {
	cfg := testConfig(30, 1, []ServerConfig{{Capacity: 15, ServiceTime: 0.05, BufferCapacity: 30}})
	result, err := Run(cfg, newRNG(1), newRNG(2))
	if err != nil {
		t.Fatal(err)
	}
	if result.Completed != 30 || result.Unfinished != 0 || result.RejectedFull != 0 {
		t.Fatalf("completed=%d unfinished=%d rejected=%d, want 30, 0, 0", result.Completed, result.Unfinished, result.RejectedFull)
	}
	if result.AverageResponseTime <= cfg.Servers[0].ServiceTime {
		t.Fatalf("average response time = %g, want queueing delay above service time", result.AverageResponseTime)
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

func TestRunActivatesBackupWhenPrimaryBufferIsFull(t *testing.T) {
	cfg := testConfig(2, 1, []ServerConfig{
		{Capacity: 1, ServiceTime: 0.05, BufferCapacity: 0},
		{Capacity: 1, ServiceTime: 0.05, BufferCapacity: 1, Backup: true},
	})
	result, err := Run(cfg, newRNG(1), newRNG(2))
	if err != nil {
		t.Fatal(err)
	}
	if result.BackupActivations != 1 || result.RejectedFull != 0 || result.Completed != 2 {
		t.Fatalf("backup=%d rejected=%d completed=%d, want 1, 0, 2", result.BackupActivations, result.RejectedFull, result.Completed)
	}
	if result.Servers[1].Assigned != 1 || !result.Servers[1].Backup {
		t.Fatalf("backup result = %#v, want one assigned request", result.Servers[1])
	}
}

func TestRunRejectsOverflowWithoutBackup(t *testing.T) {
	cfg := testConfig(2, 1, []ServerConfig{{Capacity: 1, ServiceTime: 0.05, BufferCapacity: 0}})
	result, err := Run(cfg, newRNG(1), newRNG(2))
	if err != nil {
		t.Fatal(err)
	}
	if result.Accepted != 1 || result.RejectedFull != 1 || result.Completed != 1 {
		t.Fatalf("accepted=%d rejected=%d completed=%d, want 1, 1, 1", result.Accepted, result.RejectedFull, result.Completed)
	}
}

func TestRunStopsAtHorizon(t *testing.T) {
	cfg := testConfig(3, 0.01, []ServerConfig{{Capacity: 15, ServiceTime: 0.05, BufferCapacity: 3}})
	cfg.InterArrival = mustPareto(0.005)
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
	if len(cfg.Servers) != 3 || cfg.Horizon != 200 {
		t.Fatalf("unexpected default config: %#v", cfg)
	}
	for _, server := range cfg.Servers {
		if server.Capacity != 15 || server.ServiceTime != 0.05 || server.Backup {
			t.Fatalf("unexpected default server: %#v", server)
		}
	}
	if cfg.InterArrival.Alpha != 1.4 || cfg.InterArrival.Hurst != 0.8 {
		t.Fatalf("traffic parameters = alpha %g, H %g, want 1.4 and 0.8", cfg.InterArrival.Alpha, cfg.InterArrival.Hurst)
	}
}

func TestRunRejectsNonFiniteConfiguration(t *testing.T) {
	cfg := DefaultConfig(balancer.RoundRobin, 30)
	cfg.Horizon = math.NaN()
	if _, err := Run(cfg, newRNG(1), newRNG(2)); err == nil {
		t.Fatal("Run accepted a NaN horizon")
	}
}

func TestEventQueueOrdersNearDistinctTimesStrictly(t *testing.T) {
	queue := eventQueue{
		{time: 1.5e-12, sequence: 0},
		{time: 0, sequence: 2},
		{time: 0.75e-12, sequence: 1},
	}
	if !queue.Less(1, 2) {
		t.Fatal("earlier event must sort first even when timestamps are very close")
	}
	heap.Init(&queue)
	previous := -1.0
	for queue.Len() > 0 {
		next := heap.Pop(&queue).(event)
		if next.time < previous {
			t.Fatalf("event time %g followed %g", next.time, previous)
		}
		previous = next.time
	}
}

func testConfig(requests int, horizon float64, servers []ServerConfig) Config {
	return Config{
		Policy:       balancer.RoundRobin,
		RequestCount: requests,
		Horizon:      horizon,
		Servers:      servers,
		InterArrival: mustPareto(0.001),
	}
}

func mustPareto(interval float64) mathutil.BoundedPareto {
	arrivals, err := mathutil.NewBoundedPareto(interval, interval, 1.4, 0.5)
	if err != nil {
		panic(err)
	}
	return arrivals
}

func newRNG(seed uint64) *rand.Rand {
	return rand.New(rand.NewPCG(seed, seed+1))
}

func TestSharedQueueCompletesBurstWithWorkerPull(t *testing.T) {
	cfg := testConfig(30, 1, []ServerConfig{
		{Capacity: 1, ServiceTime: 0.05, BufferCapacity: 0},
		{Capacity: 1, ServiceTime: 0.05, BufferCapacity: 0},
		{Capacity: 1, ServiceTime: 0.05, BufferCapacity: 0},
	})
	cfg.QueueArchitecture = SharedQueue
	cfg.SharedQueueCapacity = 30
	result, err := Run(cfg, newRNG(1), newRNG(2))
	if err != nil {
		t.Fatal(err)
	}
	if result.Accepted != 30 || result.Completed != 30 || result.RejectedFull != 0 {
		t.Fatalf("accepted=%d completed=%d rejected=%d, want 30, 30, 0", result.Accepted, result.Completed, result.RejectedFull)
	}
	for _, server := range result.Servers {
		if server.Completed != 10 {
			t.Fatalf("server %d completed=%d, want 10", server.ID, server.Completed)
		}
	}
}

func TestSharedQueueReducesSkewAgainstRandomPrivateQueues(t *testing.T) {
	servers := []ServerConfig{
		{Capacity: 1, ServiceTime: 0.05, BufferCapacity: 30},
		{Capacity: 1, ServiceTime: 0.05, BufferCapacity: 30},
		{Capacity: 1, ServiceTime: 0.05, BufferCapacity: 30},
	}
	private := testConfig(30, 1, servers)
	private.Policy = balancer.Random
	shared := testConfig(30, 1, servers)
	shared.QueueArchitecture = SharedQueue
	shared.SharedQueueCapacity = 30

	privateResult, err := Run(private, newRNG(1), newRNG(2))
	if err != nil {
		t.Fatal(err)
	}
	sharedResult, err := Run(shared, newRNG(1), newRNG(2))
	if err != nil {
		t.Fatal(err)
	}
	if sharedResult.AverageResponseTime >= privateResult.AverageResponseTime {
		t.Fatalf("shared response=%g, random private response=%g", sharedResult.AverageResponseTime, privateResult.AverageResponseTime)
	}
}
