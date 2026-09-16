package mathutil

import (
	"math"
	rand "math/rand/v2"
	"testing"
)

func TestBoundedParetoSamplesStayWithinBounds(t *testing.T) {
	distribution, err := NewBoundedPareto(0.001, 0.1, 1.4)
	if err != nil {
		t.Fatal(err)
	}
	rng := rand.New(rand.NewPCG(1, 2))
	for range 1_000 {
		sample := distribution.Sample(rng)
		if sample < distribution.Lower || sample > distribution.Upper {
			t.Fatalf("sample %g outside [%g, %g]", sample, distribution.Lower, distribution.Upper)
		}
	}
}

func TestHurstPointEightMapsToAlphaOnePointFour(t *testing.T) {
	alpha, err := AlphaForHurst(0.8)
	if err != nil {
		t.Fatal(err)
	}
	if alpha != 1.4 {
		t.Fatalf("alpha = %g, want 1.4", alpha)
	}
}

func TestDegenerateDistributionHasZeroVariance(t *testing.T) {
	distribution, err := NewBoundedPareto(0.2, 0.2, 1.4)
	if err != nil {
		t.Fatal(err)
	}
	if got := distribution.Variance(); got != 0 {
		t.Fatalf("variance = %g, want 0", got)
	}
}

func TestBoundedParetoRejectsNonFiniteParameters(t *testing.T) {
	for _, parameters := range [][3]float64{
		{math.NaN(), 1, 1.4},
		{0.001, math.Inf(1), 1.4},
		{0.001, 1, math.NaN()},
	} {
		if _, err := NewBoundedPareto(parameters[0], parameters[1], parameters[2]); err == nil {
			t.Fatalf("NewBoundedPareto accepted non-finite parameters %v", parameters)
		}
	}
	if _, err := AlphaForHurst(math.NaN()); err == nil {
		t.Fatal("AlphaForHurst accepted NaN")
	}
}
