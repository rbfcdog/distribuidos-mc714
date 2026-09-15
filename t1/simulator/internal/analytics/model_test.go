package analytics

import (
	"math"
	"testing"
)

func TestCalculateFiniteBatchUsesExperimentHorizon(t *testing.T) {
	model, err := Calculate(30, 200, []Server{{Capacity: 15, ServiceTime: 0.05}})
	if err != nil {
		t.Fatal(err)
	}
	if math.Abs(model.Throughput-0.15) > 1e-12 {
		t.Fatalf("throughput = %g, want 30/200 = 0.15", model.Throughput)
	}
	wantUtilization := 30 * 0.05 / (15 * 200.0)
	if math.Abs(model.Utilization-wantUtilization) > 1e-12 {
		t.Fatalf("utilization = %g, want %g", model.Utilization, wantUtilization)
	}
	if math.Abs(model.AverageResponseTime-0.075) > 1e-12 {
		t.Fatalf("response = %g, want two service waves averaged to 0.075", model.AverageResponseTime)
	}
}

func TestCalculateFiniteBatchModelsFairRandomRouting(t *testing.T) {
	servers := []Server{{Capacity: 15, ServiceTime: 0.05}, {Capacity: 15, ServiceTime: 0.05}, {Capacity: 15, ServiceTime: 0.05}}
	model, err := Calculate(120, 200, servers)
	if err != nil {
		t.Fatal(err)
	}
	if model.TransitionProbability != 1.0/3.0 {
		t.Fatalf("transition probability = %g, want 1/3", model.TransitionProbability)
	}
	if model.AverageResponseTime <= model.NoQueueResponseTime {
		t.Fatalf("batch response = %g, want queueing above no-queue response %g", model.AverageResponseTime, model.NoQueueResponseTime)
	}
	if math.Abs(model.Completed-120) > 1e-9 {
		t.Fatalf("expected completed = %g, want 120", model.Completed)
	}
}

func TestCalculateRejectsMissingPrimaryServers(t *testing.T) {
	if _, err := Calculate(30, 200, nil); err == nil {
		t.Fatal("Calculate accepted an empty server set")
	}
}
