package mathutil

import (
	"fmt"
	"math"
	rand "math/rand/v2"
)

type BoundedPareto struct {
	Lower float64
	Upper float64
	Alpha float64
	Hurst float64
}

func NewBoundedPareto(lower, upper, alpha, hurst float64) (BoundedPareto, error) {
	if !isFinite(lower) || lower <= 0 {
		return BoundedPareto{}, fmt.Errorf("bounded Pareto lower bound must be finite and positive: %g", lower)
	}
	if !isFinite(upper) || upper < lower {
		return BoundedPareto{}, fmt.Errorf("bounded Pareto upper bound %g must be finite and not below lower bound %g", upper, lower)
	}
	if !isFinite(alpha) || alpha <= 0 {
		return BoundedPareto{}, fmt.Errorf("bounded Pareto alpha must be finite and positive: %g", alpha)
	}
	if !isFinite(hurst) || hurst < 0.5 || hurst >= 1 {
		return BoundedPareto{}, fmt.Errorf("Hurst parameter must be in [0.5, 1): %g", hurst)
	}
	return BoundedPareto{Lower: lower, Upper: upper, Alpha: alpha, Hurst: hurst}, nil
}

func (p BoundedPareto) SampleSequence(rng *rand.Rand, count int) []float64 {
	if rng == nil {
		panic("bounded Pareto sample called with nil random source")
	}
	if count < 0 {
		panic("bounded Pareto sample count cannot be negative")
	}
	samples := make([]float64, count)
	if count == 0 {
		return samples
	}
	if p.Lower == p.Upper {
		for index := range samples {
			samples[index] = p.Lower
		}
		return samples
	}

	gaussian := fractionalGaussianNoise(rng, count, p.Hurst)
	term := 1 - math.Pow(p.Lower/p.Upper, p.Alpha)
	for index, value := range gaussian {
		unit := 0.5 * (1 + math.Erf(value/math.Sqrt2))
		samples[index] = p.Lower / math.Pow(1-unit*term, 1/p.Alpha)
	}
	return samples
}

func fractionalGaussianNoise(rng *rand.Rand, count int, hurst float64) []float64 {
	values := make([]float64, count)
	if count == 0 {
		return values
	}

	covariance := make([]float64, count)
	exponent := 2 * hurst
	for lag := range covariance {
		left := math.Abs(float64(lag - 1))
		center := float64(lag)
		right := float64(lag + 1)
		covariance[lag] = 0.5 * (math.Pow(left, exponent) - 2*math.Pow(center, exponent) + math.Pow(right, exponent))
	}

	coefficients := make([]float64, count)
	innovationVariance := covariance[0]
	values[0] = math.Sqrt(innovationVariance) * rng.NormFloat64()
	for index := 1; index < count; index++ {
		reflection := covariance[index]
		for offset := 0; offset < index-1; offset++ {
			reflection -= coefficients[offset] * covariance[index-1-offset]
		}
		reflection /= innovationVariance

		previous := append([]float64(nil), coefficients[:index-1]...)
		for offset := 0; offset < index-1; offset++ {
			coefficients[offset] = previous[offset] - reflection*previous[index-2-offset]
		}
		coefficients[index-1] = reflection

		mean := 0.0
		for offset := 0; offset < index; offset++ {
			mean += coefficients[offset] * values[index-1-offset]
		}
		innovationVariance *= 1 - reflection*reflection
		values[index] = mean + math.Sqrt(math.Max(innovationVariance, 0))*rng.NormFloat64()
	}
	return values
}

func (p BoundedPareto) Mean() float64 {
	return p.moment(1)
}

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

func isFinite(value float64) bool {
	return !math.IsNaN(value) && !math.IsInf(value, 0)
}
