package analytics

import (
	"fmt"
	"math"
)

type Server struct {
	Capacity    int
	ServiceTime float64
}

type Model struct {
	BurstSize             int
	Horizon               float64
	TransitionProbability float64
	Completed             float64
	Throughput            float64
	Utilization           float64
	NoQueueResponseTime   float64
	QueueingDelay         float64
	AverageResponseTime   float64
}

func Calculate(requestCount int, horizon float64, servers []Server) (Model, error) {
	if requestCount <= 0 {
		return Model{}, fmt.Errorf("request count must be positive: %d", requestCount)
	}
	if horizon <= 0 || math.IsNaN(horizon) || math.IsInf(horizon, 0) {
		return Model{}, fmt.Errorf("horizon must be finite and positive: %g", horizon)
	}
	if len(servers) == 0 {
		return Model{}, fmt.Errorf("at least one primary server is required")
	}

	totalCapacity := 0
	noQueueResponse := 0.0
	for index, server := range servers {
		if server.Capacity <= 0 || server.ServiceTime <= 0 || math.IsNaN(server.ServiceTime) || math.IsInf(server.ServiceTime, 0) {
			return Model{}, fmt.Errorf("server %d capacity and service time must be finite and positive", index)
		}
		totalCapacity += server.Capacity
		noQueueResponse += server.ServiceTime / float64(len(servers))
	}

	probability := 1 / float64(len(servers))
	pmf := binomialPMF(requestCount, probability)
	var expectedCompleted, expectedBusySlotTime, expectedTotalResponse float64
	for _, server := range servers {
		for assigned, mass := range pmf {
			completed, busySlotTime, totalResponse := batchMetrics(assigned, server, horizon)
			expectedCompleted += mass * float64(completed)
			expectedBusySlotTime += mass * busySlotTime
			expectedTotalResponse += mass * totalResponse
		}
	}

	model := Model{
		BurstSize:             requestCount,
		Horizon:               horizon,
		TransitionProbability: probability,
		Completed:             expectedCompleted,
		Throughput:            expectedCompleted / horizon,
		Utilization:           expectedBusySlotTime / (float64(totalCapacity) * horizon),
		NoQueueResponseTime:   noQueueResponse,
	}
	if expectedCompleted > 0 {
		model.AverageResponseTime = expectedTotalResponse / expectedCompleted
		model.QueueingDelay = model.AverageResponseTime - noQueueResponse
	}
	return model, nil
}

func batchMetrics(assigned int, server Server, horizon float64) (completed int, busySlotTime, totalResponse float64) {
	for first := 0; first < assigned; first += server.Capacity {
		jobs := min(server.Capacity, assigned-first)
		wave := first/server.Capacity + 1
		start := float64(wave-1) * server.ServiceTime
		busyDuration := math.Min(server.ServiceTime, math.Max(0, horizon-start))
		busySlotTime += float64(jobs) * busyDuration

		completion := float64(wave) * server.ServiceTime
		if completion <= horizon+1e-12 {
			completed += jobs
			totalResponse += float64(jobs) * completion
		}
	}
	return completed, busySlotTime, totalResponse
}

func binomialPMF(n int, probability float64) []float64 {
	pmf := make([]float64, n+1)
	if probability == 1 {
		pmf[n] = 1
		return pmf
	}
	pmf[0] = math.Pow(1-probability, float64(n))
	ratio := probability / (1 - probability)
	for k := 1; k <= n; k++ {
		pmf[k] = pmf[k-1] * float64(n-k+1) / float64(k) * ratio
	}
	sum := 0.0
	for _, mass := range pmf {
		sum += mass
	}
	for k := range pmf {
		pmf[k] /= sum
	}
	return pmf
}
