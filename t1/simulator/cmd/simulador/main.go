// simulador runs the required policy and burst-size experiment matrix.
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
	"mc714-t1/internal/domain"
	"mc714-t1/internal/engine"
	"mc714-t1/internal/experiment"
)

const (
	trialCount  = 10
	defaultSeed = uint64(20260831)
)

func main() {
	trace := flag.Bool("trace", false, "print per-event server-state samples for each scenario's first trial")
	seed := flag.Uint64("seed", defaultSeed, "base seed for reproducible independent trials")
	resultsCSV := flag.String("results-csv", "", "optional path for summary CSV output")
	trialsCSV := flag.String("trials-csv", "", "optional path for per-trial metric CSV output")
	traceCSV := flag.String("trace-csv", "", "optional path for representative server-state CSV output")
	trafficCSV := flag.String("traffic-csv", "", "optional path for representative inter-arrival CSV output")
	flag.Parse()

	policies := []balancer.Policy{balancer.Random, balancer.RoundRobin, balancer.ShortestQueue}
	bursts := []int{30, 60, 90, 120}
	summaries := make([]experiment.Summary, 0, len(policies)*len(bursts))

	fmt.Println("MC714 load-balancer discrete-event simulation")
	fmt.Printf("servers=%d capacity/server=%d service=%.2f horizon=%.0f trials=%d\n", engine.DefaultServerCount, engine.DefaultServerCapacity, engine.DefaultServiceTime, engine.DefaultHorizon, trialCount)
	fmt.Println("policy          burst  throughput(sim/finite/stat)  response(sim/finite/stat)")

	for _, policy := range policies {
		for _, burst := range bursts {
			cfg := engine.DefaultConfig(policy, burst)
			summary, err := experiment.Run(cfg, trialCount, *seed)
			if err != nil {
				log.Fatalf("run %s/%d: %v", policy, burst, err)
			}
			summaries = append(summaries, summary)
			fmt.Printf("%-15s %5d  %7.3f/%7.3f/%7.3f  %8.5f/%8.5f/%8.5f\n",
				policy,
				burst,
				summary.MeanThroughput,
				summary.FiniteHorizon.Throughput,
				summary.Analytical.Throughput,
				summary.MeanResponseTime,
				summary.FiniteHorizon.AverageResponseTime,
				summary.Analytical.AverageResponseTime,
			)
			if *trace {
				printTrace(policy, burst, summary.Representative.Samples)
			}
		}
	}

	if *resultsCSV != "" {
		if err := writeResultsCSV(*resultsCSV, summaries); err != nil {
			log.Fatalf("write results CSV: %v", err)
		}
	}
	if *trialsCSV != "" {
		if err := writeTrialsCSV(*trialsCSV, summaries); err != nil {
			log.Fatalf("write trials CSV: %v", err)
		}
	}
	if *traceCSV != "" {
		if err := writeTraceCSV(*traceCSV, summaries); err != nil {
			log.Fatalf("write trace CSV: %v", err)
		}
	}
	if *trafficCSV != "" {
		if err := writeTrafficCSV(*trafficCSV, summaries); err != nil {
			log.Fatalf("write traffic CSV: %v", err)
		}
	}
}

func printTrace(policy balancer.Policy, burst int, samples []domain.ServerSample) {
	fmt.Printf("trace policy=%s burst=%d time server active queue completed\n", policy, burst)
	for _, sample := range samples {
		if math.IsNaN(sample.Time) {
			continue
		}
		fmt.Printf("trace %-14s %5d %.6f %d %d %d %d\n", policy, burst, sample.Time, sample.ServerID, sample.Active, sample.QueueLength, sample.Completed)
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
		"mean_throughput", "std_throughput", "finite_horizon_throughput", "stationary_throughput",
		"mean_response_time", "std_response_time", "finite_horizon_response_time", "stationary_response_time",
		"mean_completed", "mean_unfinished", "utilization",
		"throughput_absolute_error", "response_absolute_error",
	}); err != nil {
		return err
	}
	for _, summary := range summaries {
		if err := writer.Write([]string{
			summary.Policy,
			strconv.Itoa(summary.BurstSize),
			strconv.Itoa(summary.Trials),
			float(summary.MeanThroughput),
			float(summary.StdThroughput),
			float(summary.FiniteHorizon.Throughput),
			float(summary.Analytical.Throughput),
			float(summary.MeanResponseTime),
			float(summary.StdResponseTime),
			float(summary.FiniteHorizon.AverageResponseTime),
			float(summary.Analytical.AverageResponseTime),
			float(summary.MeanCompleted),
			float(summary.MeanUnfinished),
			float(summary.Analytical.Utilization),
			float(summary.ThroughputAbsoluteError),
			float(summary.ResponseAbsoluteError),
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
		"policy", "burst_size", "trial", "throughput", "response_time", "duration", "completed",
		"assigned_0", "assigned_1", "assigned_2",
	}); err != nil {
		return err
	}
	for _, summary := range summaries {
		for _, trial := range summary.Runs {
			assigned := []int{0, 0, 0}
			copy(assigned, trial.Assigned)
			if err := writer.Write([]string{
				summary.Policy,
				strconv.Itoa(summary.BurstSize),
				strconv.Itoa(trial.Index),
				float(trial.Throughput),
				float(trial.AverageResponseTime),
				float(trial.Duration),
				strconv.Itoa(trial.Completed),
				strconv.Itoa(assigned[0]),
				strconv.Itoa(assigned[1]),
				strconv.Itoa(assigned[2]),
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
	if err := writer.Write([]string{"policy", "burst_size", "time", "server_id", "active", "queue_length", "completed"}); err != nil {
		return err
	}
	for _, summary := range summaries {
		for _, sample := range summary.Representative.Samples {
			if err := writer.Write([]string{
				summary.Policy,
				strconv.Itoa(summary.BurstSize),
				float(sample.Time),
				strconv.Itoa(sample.ServerID),
				strconv.Itoa(sample.Active),
				strconv.Itoa(sample.QueueLength),
				strconv.Itoa(sample.Completed),
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
			if err := writer.Write([]string{
				strconv.Itoa(summary.BurstSize),
				strconv.Itoa(index + 1),
				float(interval),
			}); err != nil {
				return err
			}
		}
	}
	writer.Flush()
	return writer.Error()
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
