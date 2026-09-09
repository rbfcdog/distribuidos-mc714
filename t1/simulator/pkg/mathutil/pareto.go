// Package mathutil provides traffic-distribution primitives.
package mathutil

import (
	"fmt"
	"math"
	rand "math/rand/v2"
)

// BoundedPareto is a Pareto distribution truncated to [Lower, Upper].
type BoundedPareto struct {
	Lower float64
	Upper float64
	Alpha float64
}

// NewBoundedPareto validates the distribution parameters.
func NewBoundedPareto(lower, upper, alpha float64) (BoundedPareto, error) {
	if lower <= 0 {
		return BoundedPareto{}, fmt.Errorf("bounded Pareto lower bound must be positive: %g", lower)
	}
	if upper < lower {
		return BoundedPareto{}, fmt.Errorf("bounded Pareto upper bound %g is below lower bound %g", upper, lower)
	}
	if alpha <= 0 {
		return BoundedPareto{}, fmt.Errorf("bounded Pareto alpha must be positive: %g", alpha)
	}
	return BoundedPareto{Lower: lower, Upper: upper, Alpha: alpha}, nil
}

// Sample draws one value using inverse-transform sampling.
func (p BoundedPareto) Sample(rng *rand.Rand) float64 {
	if rng == nil {
		panic("bounded Pareto sample called with nil random source")
	}
	if p.Lower == p.Upper {
		return p.Lower
	}
	term := 1 - math.Pow(p.Lower/p.Upper, p.Alpha)
	return p.Lower / math.Pow(1-rng.Float64()*term, 1/p.Alpha)
}

// Mean returns the first moment of the bounded distribution.
func (p BoundedPareto) Mean() float64 {
	return p.moment(1)
}

// Variance returns the population variance of the bounded distribution.
func (p BoundedPareto) Variance() float64 {
	variance := p.moment(2) - math.Pow(p.Mean(), 2)
	return math.Max(0, variance)
}

func (p BoundedPareto) moment(order float64) float64 {
	if p.Lower == p.Upper {
		return math.Pow(p.Lower, order)
	}
	if math.Abs(order-p.Alpha) < 1e-12 {
		denominator := 1 - math.Pow(p.Lower/p.Upper, p.Alpha)
		return p.Alpha * math.Pow(p.Lower, p.Alpha) * math.Log(p.Upper/p.Lower) / denominator
	}
	denominator := 1 - math.Pow(p.Lower/p.Upper, p.Alpha)
	return p.Alpha * math.Pow(p.Lower, p.Alpha) *
		(math.Pow(p.Upper, order-p.Alpha) - math.Pow(p.Lower, order-p.Alpha)) /
		((order - p.Alpha) * denominator)
}

// AlphaForHurst derives the common heavy-tail approximation alpha = 3 - 2H,
// valid for 0.5 < H < 1 and 1 < alpha < 2.
func AlphaForHurst(hurst float64) (float64, error) {
	if hurst <= 0.5 || hurst >= 1 {
		return 0, fmt.Errorf("Hurst parameter must be in (0.5, 1): %g", hurst)
	}
	return 3 - 2*hurst, nil
}
