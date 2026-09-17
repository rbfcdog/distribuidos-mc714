package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"log"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"mc714-t1/internal/balancer"
	"mc714-t1/internal/domain"
	"mc714-t1/internal/engine"
	"mc714-t1/internal/experiment"
	"mc714-t1/internal/stationary"
)

const (
	trialCount  = 10
	defaultSeed = uint64(20260831)
)

type extraResult struct {
	name    string
	config  engine.Config
	summary experiment.Summary
}

func main() {
	trace := flag.Bool("trace", false, "print per-event server-state samples for each scenario's first trial")
	seed := flag.Uint64("seed", defaultSeed, "base seed for reproducible independent trials")
	resultsCSV := flag.String("results-csv", "", "optional path for summary CSV output")
	trialsCSV := flag.String("trials-csv", "", "optional path for per-trial metric CSV output")
	traceCSV := flag.String("trace-csv", "", "optional path for representative server-state CSV output")
	trafficCSV := flag.String("traffic-csv", "", "optional path for representative inter-arrival CSV output")
	extrasCSV := flag.String("extras-csv", "", "optional path for heterogeneous, backup, and multi-pool scenario CSV output")
	stationaryCSV := flag.String("stationary-csv", "", "optional path for stationary M/M/1 validation CSV output")
	runExtras := flag.Bool("extras", true, "run optional heterogeneous-server, overflow-backup, and multi-pool scenarios")
	runStationary := flag.Bool("stationary", true, "run stationary M/M/1 policy, Little-law, and fluid validation scenarios")
	flag.Parse()

	policies := []balancer.Policy{balancer.Random, balancer.RoundRobin, balancer.ShortestQueue}
	bursts := []int{30, 60, 90, 120}
	summaries := make([]experiment.Summary, 0, len(policies)*len(bursts))

	fmt.Println("MC714 load-balancer discrete-event simulation")
	fmt.Printf("servers=%d capacity/server=%d service=%.2f horizon=%.0f trials=%d\n", engine.DefaultServerCount, engine.DefaultServerCapacity, engine.DefaultServiceTime, engine.DefaultHorizon, trialCount)
	fmt.Println("policy          burst  throughput(sim/model)  response(sim/batch)  utilization(sim/model)")

	for _, policy := range policies {
		for _, burst := range bursts {
			cfg := engine.DefaultConfig(policy, burst)
			summary := mustRun(cfg, *seed)
			summaries = append(summaries, summary)
			fmt.Printf("%-15s %5d  %7.3f/%7.3f       %8.5f/%8.5f    %.7f/%.7f\n",
				policy, burst,
				summary.MeanThroughput, summary.Analytical.Throughput,
				summary.MeanResponseTime, summary.Analytical.AverageResponseTime,
				summary.MeanUtilization, summary.Analytical.Utilization,
			)
			if *trace {
				printTrace(policy, burst, summary.Representative.Samples)
			}
		}
	}

	extras := []extraResult(nil)
	if *runExtras {
		extras = runOptionalScenarios(*seed)
	}

	stationarySummaries := []stationary.Summary(nil)
	if *runStationary {
		stationarySummaries = runStationaryBenchmarks(*seed)
	}

	if *resultsCSV != "" {
		mustWrite("results CSV", writeResultsCSV(*resultsCSV, summaries))
	}
	if *trialsCSV != "" {
		mustWrite("trials CSV", writeTrialsCSV(*trialsCSV, summaries))
	}
	if *traceCSV != "" {
		mustWrite("trace CSV", writeTraceCSV(*traceCSV, summaries))
	}
	if *trafficCSV != "" {
		mustWrite("traffic CSV", writeTrafficCSV(*trafficCSV, summaries))
	}
	if *extrasCSV != "" {
		mustWrite("extras CSV", writeExtrasCSV(*extrasCSV, extras))
	}
	if *stationaryCSV != "" {
		mustWrite("stationary CSV", writeStationaryCSV(*stationaryCSV, stationarySummaries))
	}
}

func mustRun(cfg engine.Config, seed uint64) experiment.Summary {
	summary, err := experiment.Run(cfg, trialCount, seed)
	if err != nil {
		log.Fatalf("run %s/%d: %v", cfg.Policy, cfg.RequestCount, err)
	}
	return summary
}

func runOptionalScenarios(seed uint64) []extraResult {
	scenarios := []struct {
		name   string
		config engine.Config
	}{
		{"heterogeneous_round_robin", engine.HeterogeneousConfig(balancer.RoundRobin, 120)},
		{"heterogeneous_weighted_round_robin", engine.HeterogeneousConfig(balancer.WeightedRoundRobin, 120)},
		{"heterogeneous_least_work", engine.HeterogeneousConfig(balancer.LeastWork, 120)},
		{"heterogeneous_power_of_two", engine.HeterogeneousConfig(balancer.PowerOfTwo, 120)},
		{"bounded_buffers", engine.BoundedBufferConfig(balancer.ShortestQueue, 120, false)},
		{"bounded_buffers_with_backup", engine.BoundedBufferConfig(balancer.ShortestQueue, 120, true)},
		{"multi_pool_flat_least_work", engine.MultiPoolConfig(balancer.LeastWork, 120)},
		{"multi_pool_hierarchical", engine.MultiPoolConfig(balancer.HierarchicalLeastWork, 120)},
	}

	fmt.Println("\noptional scenarios")
	fmt.Println("scenario                              policy                    throughput  response  rejected  backup activations")
	results := make([]extraResult, 0, len(scenarios))
	for _, scenario := range scenarios {
		summary := mustRun(scenario.config, seed)
		results = append(results, extraResult{name: scenario.name, config: scenario.config, summary: summary})
		fmt.Printf("%-37s %-25s %10.3f  %8.5f  %8.1f  %18.1f\n",
			scenario.name, scenario.config.Policy, summary.MeanThroughput, summary.MeanResponseTime,
			summary.MeanRejectedFull, summary.MeanBackupActivations)
	}
	return results
}

func runStationaryBenchmarks(seed uint64) []stationary.Summary {
	policies := []balancer.Policy{balancer.Random, balancer.RoundRobin, balancer.ShortestQueue}
	stableLambdas := []float64{0.3, 0.9, 1.5, 2.1, 2.7}
	summaries := make([]stationary.Summary, 0, len(policies)*(len(stableLambdas)+1))

	fmt.Println("\nstationary M/M/1 validation")
	fmt.Println("policy          lambda stable  X(sim/theory)  U(sim/theory)  L(sim/XR)   R(sim/theory)  final N")
	for _, lambda := range stableLambdas {
		for _, policy := range policies {
			summary, err := stationary.Run(stationary.StableConfig(policy, lambda), trialCount, seed)
			if err != nil {
				log.Fatalf("stationary run %s/%.1f: %v", policy, lambda, err)
			}
			summaries = append(summaries, summary)
			fmt.Printf("%-15s %6.1f  yes     %5.3f/%5.3f     %5.3f/%5.3f    %5.3f/%5.3f  %6.3f/%6.3f  %7.1f\n",
				policy, lambda, summary.MeanThroughput, summary.Model.Throughput,
				summary.MeanUtilization, summary.Model.Utilization,
				summary.MeanJobs, summary.MeanLittleRight,
				summary.MeanResponse, summary.Model.ExpectedResponse,
				summary.MeanFinalJobs,
			)
		}
	}
	for _, policy := range policies {
		summary, err := stationary.Run(stationary.UnstableConfig(policy, 3.3), trialCount, seed)
		if err != nil {
			log.Fatalf("fluid run %s: %v", policy, err)
		}
		summaries = append(summaries, summary)
		fmt.Printf("%-15s %6.1f  no      %5.3f/%5.3f     %5.3f/%5.3f    %5.3f/%5.3f  %6.3f/  inf  %7.1f\n",
			policy, summary.Lambda, summary.MeanThroughput, float64(summary.Servers)*summary.Mu,
			summary.MeanUtilization, 1.0,
			summary.MeanJobs, summary.MeanLittleRight,
			summary.MeanResponse,
			summary.MeanFinalJobs,
		)
	}
	return summaries
}

func writeStationaryCSV(path string, summaries []stationary.Summary) error {
	file, err := createCSV(path)
	if err != nil {
		return err
	}
	defer file.Close()
	writer := csv.NewWriter(file)
	defer writer.Flush()
	if err := writer.Write([]string{
		"policy", "lambda", "mu", "servers", "stable", "trials", "horizon", "warmup",
		"rho", "p0", "analytical_jobs_per_server", "analytical_queue_wait", "analytical_response", "analytical_throughput", "analytical_utilization", "fluid_backlog_rate",
		"mean_throughput", "mean_utilization", "mean_jobs", "mean_response", "little_right", "little_absolute_error", "mean_final_jobs",
	}); err != nil {
		return err
	}
	for _, summary := range summaries {
		if err := writer.Write([]string{
			string(summary.Policy), float(summary.Lambda), float(summary.Mu), strconv.Itoa(summary.Servers), strconv.FormatBool(summary.Model.Stable), strconv.Itoa(summary.Trials), float(summary.Horizon), float(summary.Warmup),
			float(summary.Model.Rho), float(summary.Model.P0), float(summary.Model.ExpectedJobs), float(summary.Model.ExpectedQueueWait), float(summary.Model.ExpectedResponse), float(summary.Model.Throughput), float(summary.Model.Utilization), float(summary.Model.FluidBacklogRate),
			float(summary.MeanThroughput), float(summary.MeanUtilization), float(summary.MeanJobs), float(summary.MeanResponse), float(summary.MeanLittleRight), float(math.Abs(summary.MeanJobs - summary.MeanLittleRight)), float(summary.MeanFinalJobs),
		}); err != nil {
			return err
		}
	}
	writer.Flush()
	return writer.Error()
}

func printTrace(policy balancer.Policy, burst int, samples []domain.ServerSample) {
	fmt.Printf("trace policy=%s burst=%d time server active queue completed capacity backup\n", policy, burst)
	for _, sample := range samples {
		if math.IsNaN(sample.Time) {
			continue
		}
		fmt.Printf("trace %-14s %5d %.6f %d %d %d %d %d %t\n", policy, burst, sample.Time, sample.ServerID, sample.Active, sample.QueueLength, sample.Completed, sample.Capacity, sample.Backup)
	}
}

func writeResultsCSV(path string, summaries []experiment.Summary) error {
	file, err := createCSV(path)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()
	if err := writer.Write([]string{
		"policy", "burst_size", "trials",
		"mean_throughput", "std_throughput", "analytical_throughput",
		"mean_response_time", "std_response_time", "analytical_response_time", "no_queue_response_time",
		"mean_utilization", "std_utilization", "analytical_utilization",
		"mean_completed", "mean_unfinished", "mean_rejected_full", "mean_backup_activations",
		"throughput_absolute_error", "response_absolute_error",
	}); err != nil {
		return err
	}
	for _, summary := range summaries {
		if err := writer.Write([]string{
			summary.Policy, strconv.Itoa(summary.BurstSize), strconv.Itoa(summary.Trials),
			float(summary.MeanThroughput), float(summary.StdThroughput), float(summary.Analytical.Throughput),
			float(summary.MeanResponseTime), float(summary.StdResponseTime), float(summary.Analytical.AverageResponseTime), float(summary.Analytical.NoQueueResponseTime),
			float(summary.MeanUtilization), float(summary.StdUtilization), float(summary.Analytical.Utilization),
			float(summary.MeanCompleted), float(summary.MeanUnfinished), float(summary.MeanRejectedFull), float(summary.MeanBackupActivations),
			float(summary.ThroughputAbsoluteError), float(summary.ResponseAbsoluteError),
		}); err != nil {
			return err
		}
	}
	writer.Flush()
	return writer.Error()
}

func writeTrialsCSV(path string, summaries []experiment.Summary) error {
	file, err := createCSV(path)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()
	if err := writer.Write([]string{
		"policy", "burst_size", "trial", "throughput", "response_time", "utilization", "duration", "completed", "rejected_full", "backup_activations",
		"assigned_0", "assigned_1", "assigned_2", "assigned_3",
	}); err != nil {
		return err
	}
	for _, summary := range summaries {
		for _, trial := range summary.Runs {
			assigned := []int{0, 0, 0, 0}
			copy(assigned, trial.Assigned)
			if err := writer.Write([]string{
				summary.Policy, strconv.Itoa(summary.BurstSize), strconv.Itoa(trial.Index),
				float(trial.Throughput), float(trial.AverageResponseTime), float(trial.Utilization), float(trial.Duration),
				strconv.Itoa(trial.Completed), strconv.Itoa(trial.RejectedFull), strconv.Itoa(trial.BackupActivations),
				strconv.Itoa(assigned[0]), strconv.Itoa(assigned[1]), strconv.Itoa(assigned[2]), strconv.Itoa(assigned[3]),
			}); err != nil {
				return err
			}
		}
	}
	writer.Flush()
	return writer.Error()
}

func writeTraceCSV(path string, summaries []experiment.Summary) error {
	file, err := createCSV(path)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()
	if err := writer.Write([]string{"policy", "burst_size", "time", "server_id", "active", "queue_length", "completed", "capacity", "is_backup"}); err != nil {
		return err
	}
	for _, summary := range summaries {
		for _, sample := range summary.Representative.Samples {
			if err := writer.Write([]string{
				summary.Policy, strconv.Itoa(summary.BurstSize), float(sample.Time), strconv.Itoa(sample.ServerID),
				strconv.Itoa(sample.Active), strconv.Itoa(sample.QueueLength), strconv.Itoa(sample.Completed), strconv.Itoa(sample.Capacity), strconv.FormatBool(sample.Backup),
			}); err != nil {
				return err
			}
		}
	}
	writer.Flush()
	return writer.Error()
}

func writeTrafficCSV(path string, summaries []experiment.Summary) error {
	file, err := createCSV(path)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()
	if err := writer.Write([]string{"burst_size", "index", "interarrival"}); err != nil {
		return err
	}
	seen := map[int]struct{}{}
	for _, summary := range summaries {
		if _, exists := seen[summary.BurstSize]; exists {
			continue
		}
		seen[summary.BurstSize] = struct{}{}
		for index, interval := range summary.Representative.Interarrivals {
			if err := writer.Write([]string{strconv.Itoa(summary.BurstSize), strconv.Itoa(index + 1), float(interval)}); err != nil {
				return err
			}
		}
	}
	writer.Flush()
	return writer.Error()
}

func writeExtrasCSV(path string, extras []extraResult) error {
	file, err := createCSV(path)
	if err != nil {
		return err
	}
	defer file.Close()
	writer := csv.NewWriter(file)
	defer writer.Flush()
	if err := writer.Write([]string{
		"scenario", "policy", "servers", "mean_throughput", "mean_response_time", "mean_utilization", "mean_rejected_full", "mean_backup_activations",
	}); err != nil {
		return err
	}
	for _, result := range extras {
		if err := writer.Write([]string{
			result.name, result.summary.Policy, describeServers(result.config.Servers),
			float(result.summary.MeanThroughput), float(result.summary.MeanResponseTime), float(result.summary.MeanUtilization),
			float(result.summary.MeanRejectedFull), float(result.summary.MeanBackupActivations),
		}); err != nil {
			return err
		}
	}
	writer.Flush()
	return writer.Error()
}

func describeServers(servers []engine.ServerConfig) string {
	parts := make([]string, len(servers))
	for index, server := range servers {
		role := "primary"
		if server.Backup {
			role = "backup"
		}
		parts[index] = fmt.Sprintf("%s:c%d/s%.3f/b%d", role, server.Capacity, server.ServiceTime, server.BufferCapacity)
	}
	return strings.Join(parts, ";")
}

func mustWrite(label string, err error) {
	if err != nil {
		log.Fatalf("write %s: %v", label, err)
	}
}

func createCSV(path string) (*os.File, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	return os.Create(path)
}

func float(value float64) string {
	return strconv.FormatFloat(value, 'g', -1, 64)
}
