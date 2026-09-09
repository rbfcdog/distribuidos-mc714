// Package experiment runs independent trials and compares them with theory.
package experiment

import (
	"fmt"
	"math"
	rand "math/rand/v2"

	"mc714-t1/internal/analytics"
	"mc714-t1/internal/balancer"
	"mc714-t1/internal/engine"
)

// Trial is one independent repetition of a policy/burst scenario.
type Trial struct {
	Index               int
	Throughput          float64
	AverageResponseTime float64
	Duration            float64
	Completed           int
	Assigned            []int
}

// Summary aggregates one policy and burst-size scenario over independent runs.
type Summary struct {
	Policy                  string
	BurstSize               int
	Trials                  int
	MeanThroughput          float64
	StdThroughput           float64
	MeanResponseTime        float64
	StdResponseTime         float64
	MeanCompleted           float64
	MeanUnfinished          float64
	Analytical              analytics.Model
	FiniteHorizon           analytics.BurstModel
	ThroughputAbsoluteError float64
	ResponseAbsoluteError   float64
	Runs                    []Trial
	Representative          engine.Result
}

// Run executes trials independent simulations. The traffic seed depends only
// on trial number, so every policy sees the same arrival trace in a trial.
func Run(cfg engine.Config, trials int, seed uint64) (Summary, error) {
	if trials <= 0 {
		return Summary{}, fmt.Errorf("trial count must be positive: %d", trials)
	}
	model, err := analytics.Calculate(cfg.InterArrival, cfg.ServerCount, cfg.ServerCapacity, cfg.ServiceTime)
	if err != nil {
		return Summary{}, err
	}
	burst, err := analytics.FiniteHorizon(cfg.RequestCount, cfg.InterArrival, cfg.ServiceTime)
	if err != nil {
		return Summary{}, err
	}

	summary := Summary{
		Policy:        string(cfg.Policy),
		BurstSize:     cfg.RequestCount,
		Trials:        trials,
		Analytical:    model,
		FiniteHorizon: burst,
		Runs:          make([]Trial, 0, trials),
	}
	throughputs := make([]float64, 0, trials)
	responses := make([]float64, 0, trials)
	for trial := range trials {
		trafficRNG := rand.New(rand.NewPCG(seed+uint64(trial), 0x9e3779b97f4a7c15))
		routingRNG := rand.New(rand.NewPCG(seed+uint64(trial), policyStream(cfg.Policy)))
		result, err := engine.Run(cfg, trafficRNG, routingRNG)
		if err != nil {
			return Summary{}, fmt.Errorf("trial %d: %w", trial+1, err)
		}
		if trial == 0 {
			summary.Representative = result
		}
		assigned := make([]int, len(result.Servers))
		for index := range result.Servers {
			assigned[index] = result.Servers[index].Assigned
		}
		summary.Runs = append(summary.Runs, Trial{
			Index:               trial + 1,
			Throughput:          result.Throughput,
			AverageResponseTime: result.AverageResponseTime,
			Duration:            result.Duration,
			Completed:           result.Completed,
			Assigned:            assigned,
		})
		throughputs = append(throughputs, result.Throughput)
		responses = append(responses, result.AverageResponseTime)
		summary.MeanThroughput += result.Throughput
		summary.MeanResponseTime += result.AverageResponseTime
		summary.MeanCompleted += float64(result.Completed)
		summary.MeanUnfinished += float64(result.Unfinished)
	}

	divisor := float64(trials)
	summary.MeanThroughput /= divisor
	summary.MeanResponseTime /= divisor
	summary.MeanCompleted /= divisor
	summary.MeanUnfinished /= divisor
	summary.StdThroughput = sampleStd(throughputs)
	summary.StdResponseTime = sampleStd(responses)
	summary.ThroughputAbsoluteError = math.Abs(summary.MeanThroughput - model.Throughput)
	if !math.IsInf(model.AverageResponseTime, 0) {
		summary.ResponseAbsoluteError = math.Abs(summary.MeanResponseTime - model.AverageResponseTime)
	} else {
		summary.ResponseAbsoluteError = math.Inf(1)
	}
	return summary, nil
}

func sampleStd(values []float64) float64 {
	if len(values) < 2 {
		return 0
	}
	mean := 0.0
	for _, value := range values {
		mean += value
	}
	mean /= float64(len(values))
	sumSquares := 0.0
	for _, value := range values {
		delta := value - mean
		sumSquares += delta * delta
	}
	return math.Sqrt(sumSquares / float64(len(values)-1))
}

func policyStream(policy balancer.Policy) uint64 {
	switch policy {
	case balancer.Random:
		return 0xa24baed4963ee407
	case balancer.RoundRobin:
		return 0x9fb21c651e98df25
	case balancer.ShortestQueue:
		return 0xc13fa9a902a6328f
	default:
		return 0
	}
}
