package mathutil

import (
	"math"
	rand "math/rand/v2"
	"testing"
)

func TestBoundedParetoSamplesStayWithinBounds(t *testing.T) {
	distribution, err := NewBoundedPareto(0.001, 0.1, 1.4, 0.8)
	if err != nil {
		t.Fatal(err)
	}
	samples := distribution.SampleSequence(rand.New(rand.NewPCG(1, 2)), 1_000)
	for _, sample := range samples {
		if sample < distribution.Lower || sample > distribution.Upper {
			t.Fatalf("sample %g outside [%g, %g]", sample, distribution.Lower, distribution.Upper)
		}
	}
}

func TestBoundedParetoSequencePreservesLongRangeDependence(t *testing.T) {
	distribution, err := NewBoundedPareto(0.001, 0.1, 1.4, 0.8)
	if err != nil {
		t.Fatal(err)
	}
	samples := distribution.SampleSequence(rand.New(rand.NewPCG(3, 4)), 2_000)
	if correlation := lagOneCorrelation(samples); correlation < 0.2 {
		t.Fatalf("lag-one correlation = %g, want at least 0.2", correlation)
	}
}

func TestDegenerateDistributionHasZeroVariance(t *testing.T) {
	distribution, err := NewBoundedPareto(0.2, 0.2, 1.4, 0.8)
	if err != nil {
		t.Fatal(err)
	}
	if got := distribution.Variance(); got != 0 {
		t.Fatalf("variance = %g, want 0", got)
	}
}

func TestBoundedParetoRejectsNonFiniteParameters(t *testing.T) {
	for _, parameters := range [][4]float64{
		{math.NaN(), 1, 1.4, 0.8},
		{0.001, math.Inf(1), 1.4, 0.8},
		{0.001, 1, math.NaN(), 0.8},
		{0.001, 1, 1.4, math.NaN()},
	} {
		if _, err := NewBoundedPareto(parameters[0], parameters[1], parameters[2], parameters[3]); err == nil {
			t.Fatalf("NewBoundedPareto accepted non-finite parameters %v", parameters)
		}
	}
}

func lagOneCorrelation(values []float64) float64 {
	mean := 0.0
	for _, value := range values {
		mean += value
	}
	mean /= float64(len(values))

	numerator := 0.0
	denominator := 0.0
	for index, value := range values {
		delta := value - mean
		denominator += delta * delta
		if index > 0 {
			numerator += delta * (values[index-1] - mean)
		}
	}
	return numerator / denominator
}
