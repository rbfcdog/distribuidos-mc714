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

	"mc714-t1/internal/balancer"
	"mc714-t1/internal/engine"
	"mc714-t1/internal/experiment"
	"mc714-t1/internal/stationary"
)

const (
	trialCount  = 10
	defaultSeed = uint64(20260917)
)

func main() {
	policyFlag := flag.String("policy", "all", "balancing policy: all, random, round_robin, or shortest_queue")
	trace := flag.Bool("trace", false, "print event-by-event queue dynamics for the first replica of every configuration")
	seed := flag.Uint64("seed", defaultSeed, "base seed for reproducible replicas")
	resultsCSV := flag.String("results-csv", "", "optional path for summary CSV output")
	trialsCSV := flag.String("trials-csv", "", "optional path for per-replica CSV output")
	traceCSV := flag.String("trace-csv", "", "optional path for event trace CSV output")
	heterogeneousCSV := flag.String("heterogeneous-csv", "", "optional path for the heterogeneous-server extension CSV")
	boundedParetoArchitecturesCSV := flag.String("bounded-pareto-architectures-csv", "", "optional path for the Bounded Pareto architecture-extension CSV")
	flag.Parse()

	policies, err := selectPolicies(*policyFlag)
	if err != nil {
		log.Fatal(err)
	}
	captureTrace := *trace || *traceCSV != ""
	stable := runStable(policies, *seed, captureTrace)
	unstable := runUnstable(policies, *seed, captureTrace)
	summaries := append(stable, unstable...)
	if *trace {
		printTrace(summaries)
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
	if *heterogeneousCSV != "" {
		mustWrite("heterogeneous CSV", writeHeterogeneousCSV(*heterogeneousCSV, runHeterogeneous(*seed)))
	}
	if *boundedParetoArchitecturesCSV != "" {
		mustWrite("Bounded Pareto architectures CSV", writeBoundedParetoArchitecturesCSV(*boundedParetoArchitecturesCSV, runBoundedParetoArchitectures(*seed)))
	}
}

var heterogeneousRates = []float64{1.5, 1.0, 0.5}

func runHeterogeneous(seed uint64) []stationary.Summary {
	summaries := make([]stationary.Summary, 0, 2)
	for _, policy := range []balancer.Policy{balancer.RoundRobin, balancer.WeightedRoundRobin} {
		summaries = append(summaries, mustRun(stationary.HeterogeneousConfig(policy, 2.4, heterogeneousRates), seed))
	}
	return summaries
}

func writeHeterogeneousCSV(path string, summaries []stationary.Summary) error {
	file, err := createCSV(path)
	if err != nil {
		return err
	}
	defer file.Close()
	writer := csv.NewWriter(file)
	defer writer.Flush()
	if err := writer.Write([]string{
		"policy", "lambda", "mu_0", "mu_1", "mu_2", "trials", "horizon", "warmup",
		"mean_throughput", "ci95_throughput", "mean_response", "ci95_response", "mean_final_jobs", "ci95_final_jobs",
		"mean_utilization_0", "mean_utilization_1", "mean_utilization_2",
	}); err != nil {
		return err
	}
	for _, summary := range summaries {
		if err := writer.Write([]string{
			string(summary.Policy), float(summary.Lambda), float(heterogeneousRates[0]), float(heterogeneousRates[1]), float(heterogeneousRates[2]), strconv.Itoa(summary.Trials), float(summary.Horizon), float(summary.Warmup),
			float(summary.MeanThroughput), float(summary.CI95Throughput), float(summary.MeanResponse), float(summary.CI95Response), float(summary.MeanFinalJobs), float(ci95FinalJobs(summary.Runs)),
			float(summary.MeanUtilizationByID[0]), float(summary.MeanUtilizationByID[1]), float(summary.MeanUtilizationByID[2]),
		}); err != nil {
			return err
		}
	}
	writer.Flush()
	return writer.Error()
}

type boundedParetoArchitecture struct {
	name         string
	architecture string
	config       engine.Config
}

type boundedParetoResult struct {
	boundedParetoArchitecture
	summary experiment.Summary
}

func runBoundedParetoArchitectures(seed uint64) []boundedParetoResult {
	scenarios := []boundedParetoArchitecture{
		{name: "private_queues", architecture: "filas_privadas", config: engine.PrivateQueueStressConfig(balancer.LeastWork, 120)},
		{name: "shared_queue", architecture: "fila_compartilhada", config: engine.SharedQueueConfig(120)},
		{name: "bounded_buffers", architecture: "buffers_limitados", config: engine.BoundedBufferConfig(balancer.LeastWork, 120, false)},
		{name: "backup_overflow", architecture: "backup_de_overflow", config: engine.BoundedBufferConfig(balancer.LeastWork, 120, true)},
		{name: "multi_pool", architecture: "pools_hierarquicos", config: engine.MultiPoolConfig(balancer.HierarchicalLeastWork, 120)},
	}
	results := make([]boundedParetoResult, 0, len(scenarios))
	for _, scenario := range scenarios {
		summary, err := experiment.Run(scenario.config, trialCount, seed)
		if err != nil {
			log.Fatalf("run Bounded Pareto scenario %s: %v", scenario.name, err)
		}
		results = append(results, boundedParetoResult{boundedParetoArchitecture: scenario, summary: summary})
	}
	return results
}

func writeBoundedParetoArchitecturesCSV(path string, results []boundedParetoResult) error {
	file, err := createCSV(path)
	if err != nil {
		return err
	}
	defer file.Close()
	writer := csv.NewWriter(file)
	defer writer.Flush()
	if err := writer.Write([]string{
		"scenario", "architecture", "policy", "requests", "servers", "horizon", "trials",
		"mean_throughput", "ci95_throughput", "mean_response", "ci95_response",
		"mean_utilization", "mean_unfinished", "mean_rejected_full", "mean_backup_activations",
	}); err != nil {
		return err
	}
	for _, result := range results {
		summary := result.summary
		if err := writer.Write([]string{
			result.name, result.architecture, summary.Policy, strconv.Itoa(summary.BurstSize), strconv.Itoa(len(result.config.Servers)), float(result.config.Horizon), strconv.Itoa(summary.Trials),
			float(summary.MeanThroughput), float(experimentCI95(summary.StdThroughput, summary.Trials)), float(summary.MeanResponseTime), float(experimentCI95(summary.StdResponseTime, summary.Trials)),
			float(summary.MeanUtilization), float(summary.MeanUnfinished), float(summary.MeanRejectedFull), float(summary.MeanBackupActivations),
		}); err != nil {
			return err
		}
	}
	writer.Flush()
	return writer.Error()
}

func experimentCI95(sampleStd float64, trials int) float64 {
	if trials < 2 {
		return 0
	}
	return 2.2621571628540993 * sampleStd / math.Sqrt(float64(trials))
}

func selectPolicies(value string) ([]balancer.Policy, error) {
	if value == "all" {
		return []balancer.Policy{balancer.Random, balancer.RoundRobin, balancer.ShortestQueue}, nil
	}
	policy := balancer.Policy(value)
	if !policy.Valid() || (policy != balancer.Random && policy != balancer.RoundRobin && policy != balancer.ShortestQueue) {
		return nil, fmt.Errorf("unsupported required policy %q", value)
	}
	return []balancer.Policy{policy}, nil
}

func runStable(policies []balancer.Policy, seed uint64, captureTrace bool) []stationary.Summary {
	lambdas := []float64{0.6, 1.2, 1.8, 2.4, 2.7}
	summaries := make([]stationary.Summary, 0, len(policies)*len(lambdas))
	fmt.Println("MC714 load-balancer simulation")
	fmt.Printf("servers=%d FCFS queues, mu=%.1f, horizon=%.0f, warmup=%.0f, replicas=%d\n", stationary.DefaultServers, stationary.DefaultMu, stationary.DefaultHorizon, stationary.DefaultWarmup, trialCount)
	fmt.Println("policy          lambda  X ± CI95        R ± CI95        N ± CI95        U0/U1/U2")
	for _, lambda := range lambdas {
		for _, policy := range policies {
			cfg := stationary.DefaultConfig(policy, lambda)
			cfg.CaptureTrace = captureTrace
			summary := mustRun(cfg, seed)
			summaries = append(summaries, summary)
			fmt.Printf("%-15s %4.1f  %6.3f ± %-6.3f  %6.3f ± %-6.3f  %6.3f ± %-6.3f  %.3f/%.3f/%.3f\n",
				policy, lambda, summary.MeanThroughput, summary.CI95Throughput,
				summary.MeanResponse, summary.CI95Response, summary.MeanJobs, summary.CI95Jobs,
				summary.MeanUtilizationByID[0], summary.MeanUtilizationByID[1], summary.MeanUtilizationByID[2])
		}
	}
	return summaries
}

func runUnstable(policies []balancer.Policy, seed uint64, captureTrace bool) []stationary.Summary {
	summaries := make([]stationary.Summary, 0, len(policies))
	fmt.Println("\nunstable experiment")
	fmt.Println("policy          lambda  final N ± CI95   fluid slope")
	for _, policy := range policies {
		cfg := stationary.UnstableConfig(policy)
		cfg.CaptureTrace = captureTrace
		summary := mustRun(cfg, seed)
		summaries = append(summaries, summary)
		fmt.Printf("%-15s %4.1f  %8.1f ± %-6.1f  %.3f\n", policy, summary.Lambda, summary.MeanFinalJobs, ci95FinalJobs(summary.Runs), summary.Model.FluidBacklogRate)
	}
	return summaries
}

func mustRun(cfg stationary.Config, seed uint64) stationary.Summary {
	summary, err := stationary.Run(cfg, trialCount, seed)
	if err != nil {
		log.Fatalf("run %s/%.1f: %v", cfg.Policy, cfg.Lambda, err)
	}
	return summary
}

func printTrace(summaries []stationary.Summary) {
	for _, summary := range summaries {
		for _, sample := range summary.Representative.Samples {
			fmt.Printf("trace policy=%s lambda=%.1f time=%.6f jobs=%d queues=%d,%d,%d completed=%d,%d,%d\n",
				summary.Policy, summary.Lambda, sample.Time, sample.Jobs,
				sample.Queues[0], sample.Queues[1], sample.Queues[2],
				sample.Completed[0], sample.Completed[1], sample.Completed[2])
		}
	}
}

func writeResultsCSV(path string, summaries []stationary.Summary) error {
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
		"mean_throughput", "ci95_throughput", "mean_utilization", "ci95_utilization", "mean_jobs", "ci95_jobs", "mean_response", "ci95_response", "little_right", "ci95_little_right", "little_absolute_error", "mean_final_jobs",
		"mean_utilization_0", "mean_utilization_1", "mean_utilization_2", "mean_assigned_0", "mean_assigned_1", "mean_assigned_2",
	}); err != nil {
		return err
	}
	for _, summary := range summaries {
		if err := writer.Write([]string{
			string(summary.Policy), float(summary.Lambda), float(summary.Mu), strconv.Itoa(summary.Servers), strconv.FormatBool(summary.Model.Stable), strconv.Itoa(summary.Trials), float(summary.Horizon), float(summary.Warmup),
			float(summary.Model.Rho), float(summary.Model.P0), float(summary.Model.ExpectedJobs), float(summary.Model.ExpectedQueueWait), float(summary.Model.ExpectedResponse), float(summary.Model.Throughput), float(summary.Model.Utilization), float(summary.Model.FluidBacklogRate),
			float(summary.MeanThroughput), float(summary.CI95Throughput), float(summary.MeanUtilization), float(summary.CI95Utilization), float(summary.MeanJobs), float(summary.CI95Jobs), float(summary.MeanResponse), float(summary.CI95Response), float(summary.MeanLittleRight), float(summary.CI95LittleRight), float(math.Abs(summary.MeanJobs - summary.MeanLittleRight)), float(summary.MeanFinalJobs),
			float(summary.MeanUtilizationByID[0]), float(summary.MeanUtilizationByID[1]), float(summary.MeanUtilizationByID[2]), float(summary.MeanAssigned[0]), float(summary.MeanAssigned[1]), float(summary.MeanAssigned[2]),
		}); err != nil {
			return err
		}
	}
	writer.Flush()
	return writer.Error()
}

func writeTrialsCSV(path string, summaries []stationary.Summary) error {
	file, err := createCSV(path)
	if err != nil {
		return err
	}
	defer file.Close()
	writer := csv.NewWriter(file)
	defer writer.Flush()
	if err := writer.Write([]string{"policy", "lambda", "trial", "throughput", "utilization", "average_jobs", "average_response", "little_right", "final_jobs", "utilization_0", "utilization_1", "utilization_2", "assigned_0", "assigned_1", "assigned_2"}); err != nil {
		return err
	}
	for _, summary := range summaries {
		for trial, result := range summary.Runs {
			if err := writer.Write([]string{
				string(summary.Policy), float(summary.Lambda), strconv.Itoa(trial + 1), float(result.Throughput), float(result.Utilization), float(result.AverageJobs), float(result.AverageResponse), float(result.LittleRight), strconv.Itoa(result.FinalJobs),
				float(result.UtilizationByID[0]), float(result.UtilizationByID[1]), float(result.UtilizationByID[2]), strconv.Itoa(result.Assigned[0]), strconv.Itoa(result.Assigned[1]), strconv.Itoa(result.Assigned[2]),
			}); err != nil {
				return err
			}
		}
	}
	writer.Flush()
	return writer.Error()
}

func writeTraceCSV(path string, summaries []stationary.Summary) error {
	file, err := createCSV(path)
	if err != nil {
		return err
	}
	defer file.Close()
	writer := csv.NewWriter(file)
	defer writer.Flush()
	if err := writer.Write([]string{"policy", "lambda", "time", "jobs", "queue_0", "queue_1", "queue_2", "completed_0", "completed_1", "completed_2"}); err != nil {
		return err
	}
	for _, summary := range summaries {
		for _, sample := range summary.Representative.Samples {
			if err := writer.Write([]string{
				string(summary.Policy), float(summary.Lambda), float(sample.Time), strconv.Itoa(sample.Jobs), strconv.Itoa(sample.Queues[0]), strconv.Itoa(sample.Queues[1]), strconv.Itoa(sample.Queues[2]), strconv.Itoa(sample.Completed[0]), strconv.Itoa(sample.Completed[1]), strconv.Itoa(sample.Completed[2]),
			}); err != nil {
				return err
			}
		}
	}
	writer.Flush()
	return writer.Error()
}

func ci95FinalJobs(runs []stationary.Trial) float64 {
	values := make([]float64, len(runs))
	for index, run := range runs {
		values[index] = float64(run.FinalJobs)
	}
	if len(values) < 2 {
		return 0
	}
	mean := 0.0
	for _, value := range values {
		mean += value
	}
	mean /= float64(len(values))
	variance := 0.0
	for _, value := range values {
		delta := value - mean
		variance += delta * delta
	}
	return 2.2621571628540993 * math.Sqrt(variance/float64(len(values)-1)) / math.Sqrt(float64(len(values)))
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
