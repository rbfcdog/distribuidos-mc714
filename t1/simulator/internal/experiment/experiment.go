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
	Utilization         float64
	Duration            float64
	Completed           int
	RejectedFull        int
	BackupActivations   int
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
	MeanUtilization         float64
	StdUtilization          float64
	MeanCompleted           float64
	MeanUnfinished          float64
	MeanRejectedFull        float64
	MeanBackupActivations   float64
	Analytical              analytics.Model
	ThroughputAbsoluteError float64
	ResponseAbsoluteError   float64
	Runs                    []Trial
	Representative          engine.Result
}

// Run executes independent simulations. The traffic seed depends only on trial
// number, so every policy sees the same arrival trace in a trial.
func Run(cfg engine.Config, trials int, seed uint64) (Summary, error) {
	if trials <= 0 {
		return Summary{}, fmt.Errorf("trial count must be positive: %d", trials)
	}

	analyticalServers := make([]analytics.Server, 0, len(cfg.Servers))
	for _, server := range cfg.Servers {
		if !server.Backup {
			analyticalServers = append(analyticalServers, analytics.Server{
				Capacity: server.Capacity, ServiceTime: server.ServiceTime,
			})
		}
	}
	model, err := analytics.Calculate(cfg.RequestCount, cfg.Horizon, analyticalServers)
	if err != nil {
		return Summary{}, err
	}

	summary := Summary{
		Policy:     string(cfg.Policy),
		BurstSize:  cfg.RequestCount,
		Trials:     trials,
		Analytical: model,
		Runs:       make([]Trial, 0, trials),
	}
	throughputs := make([]float64, 0, trials)
	responses := make([]float64, 0, trials)
	utilizations := make([]float64, 0, trials)
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
			Utilization:         result.Utilization,
			Duration:            result.Duration,
			Completed:           result.Completed,
			RejectedFull:        result.RejectedFull,
			BackupActivations:   result.BackupActivations,
			Assigned:            assigned,
		})
		throughputs = append(throughputs, result.Throughput)
		responses = append(responses, result.AverageResponseTime)
		utilizations = append(utilizations, result.Utilization)
		summary.MeanThroughput += result.Throughput
		summary.MeanResponseTime += result.AverageResponseTime
		summary.MeanUtilization += result.Utilization
		summary.MeanCompleted += float64(result.Completed)
		summary.MeanUnfinished += float64(result.Unfinished)
		summary.MeanRejectedFull += float64(result.RejectedFull)
		summary.MeanBackupActivations += float64(result.BackupActivations)
	}

	divisor := float64(trials)
	summary.MeanThroughput /= divisor
	summary.MeanResponseTime /= divisor
	summary.MeanUtilization /= divisor
	summary.MeanCompleted /= divisor
	summary.MeanUnfinished /= divisor
	summary.MeanRejectedFull /= divisor
	summary.MeanBackupActivations /= divisor
	summary.StdThroughput = sampleStd(throughputs)
	summary.StdResponseTime = sampleStd(responses)
	summary.StdUtilization = sampleStd(utilizations)
	summary.ThroughputAbsoluteError = math.Abs(summary.MeanThroughput - model.Throughput)
	summary.ResponseAbsoluteError = math.Abs(summary.MeanResponseTime - model.AverageResponseTime)
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
	case balancer.LeastWork:
		return 0x91e10da5c79e7b1d
	default:
		return 0
	}
}
