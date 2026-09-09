// Package analytics provides the fair-routing queueing approximation used to
// compare theory with the discrete-event simulation.
package analytics

import (
	"fmt"
	"math"

	"mc714-t1/pkg/mathutil"
)

// Model describes a fair (1/3 for three servers) analytical approximation.
type Model struct {
	ArrivalRate          float64
	PerServerArrivalRate float64
	ServiceRate          float64
	Utilization          float64
	Throughput           float64
	QueueingDelay        float64
	AverageResponseTime  float64
	Stable               bool
}

// Calculate estimates a G/D/c queue at each server after fair routing. The
// arrival variability is obtained from the configured bounded-Pareto process;
// deterministic service has squared coefficient of variation zero.
func Calculate(arrivals mathutil.BoundedPareto, serverCount, serverCapacity int, serviceTime float64) (Model, error) {
	if serverCount <= 0 || serverCapacity <= 0 || serviceTime <= 0 {
		return Model{}, fmt.Errorf("server count, capacity, and service time must be positive")
	}
	mean := arrivals.Mean()
	if mean <= 0 || math.IsNaN(mean) || math.IsInf(mean, 0) {
		return Model{}, fmt.Errorf("bounded Pareto mean must be finite and positive")
	}

	arrivalRate := 1 / mean
	serviceRate := 1 / serviceTime
	perServerRate := arrivalRate / float64(serverCount)
	utilization := perServerRate / (float64(serverCapacity) * serviceRate)
	throughput := math.Min(arrivalRate, float64(serverCount*serverCapacity)*serviceRate)
	model := Model{
		ArrivalRate:          arrivalRate,
		PerServerArrivalRate: perServerRate,
		ServiceRate:          serviceRate,
		Utilization:          utilization,
		Throughput:           throughput,
		Stable:               utilization < 1,
	}
	if !model.Stable {
		model.QueueingDelay = math.Inf(1)
		model.AverageResponseTime = math.Inf(1)
		return model, nil
	}

	arrivalSCV := arrivals.Variance() / (mean * mean)
	waitProbability := erlangC(float64(serverCapacity)*utilization, serverCapacity, utilization)
	model.QueueingDelay = waitProbability / (float64(serverCapacity)*serviceRate - perServerRate) * (arrivalSCV / 2)
	model.AverageResponseTime = serviceTime + model.QueueingDelay
	return model, nil
}

// BurstModel is the finite-horizon counterpart of the stationary fair-routing
// model: N arrivals, last-arrival expectation N E[X], then one service time.
type BurstModel struct {
	MeanInterarrival    float64
	ExpectedLastArrival float64
	ExpectedDuration    float64
	Throughput          float64
	AverageResponseTime float64
}

// FiniteHorizon estimates no-queue metrics for a burst of n requests. Response
// time equals the constant service time; throughput is n / (n E[X] + 1/μ).
func FiniteHorizon(n int, arrivals mathutil.BoundedPareto, serviceTime float64) (BurstModel, error) {
	if n <= 0 || serviceTime <= 0 {
		return BurstModel{}, fmt.Errorf("burst size and service time must be positive")
	}
	mean := arrivals.Mean()
	if mean <= 0 || math.IsNaN(mean) || math.IsInf(mean, 0) {
		return BurstModel{}, fmt.Errorf("bounded Pareto mean must be finite and positive")
	}
	lastArrival := float64(n) * mean
	duration := lastArrival + serviceTime
	return BurstModel{
		MeanInterarrival:    mean,
		ExpectedLastArrival: lastArrival,
		ExpectedDuration:    duration,
		Throughput:          float64(n) / duration,
		AverageResponseTime: serviceTime,
	}, nil
}

func erlangC(offeredLoad float64, servers int, utilization float64) float64 {
	term := 1.0
	sum := term
	for n := 1; n < servers; n++ {
		term *= offeredLoad / float64(n)
		sum += term
	}
	tail := term * offeredLoad / float64(servers) / (1 - utilization)
	return tail / (sum + tail)
}
