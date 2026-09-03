package analytics

import (
	"math"
	"testing"

	"mc714-t1/pkg/mathutil"
)

func TestCalculateFairModel(t *testing.T) {
	arrivals, err := mathutil.NewBoundedPareto(0.01, 0.01, 1.4)
	if err != nil {
		t.Fatal(err)
	}
	model, err := Calculate(arrivals, 3, 15, 0.05)
	if err != nil {
		t.Fatal(err)
	}
	if model.ArrivalRate != 100 || model.PerServerArrivalRate != 100.0/3.0 {
		t.Fatalf("unexpected routing rates: %#v", model)
	}
	if !model.Stable {
		t.Fatal("model marked a low-load system unstable")
	}
	if math.Abs(model.AverageResponseTime-0.05) > 1e-12 {
		t.Fatalf("response time = %g, want service time 0.05 with zero arrival variance", model.AverageResponseTime)
	}
}
