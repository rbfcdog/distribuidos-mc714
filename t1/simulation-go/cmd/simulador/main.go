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
	traceCSV := flag.String("trace-csv", "", "optional path for representative server-state CSV output")
	flag.Parse()

	policies := []balancer.Policy{balancer.Random, balancer.RoundRobin, balancer.ShortestQueue}
	bursts := []int{30, 60, 90, 120}
	summaries := make([]experiment.Summary, 0, len(policies)*len(bursts))

	fmt.Println("MC714 load-balancer discrete-event simulation")
	fmt.Printf("servers=%d capacity/server=%d service=%.2f horizon=%.0f trials=%d\n", engine.DefaultServerCount, engine.DefaultServerCapacity, engine.DefaultServiceTime, engine.DefaultHorizon, trialCount)
	fmt.Println("policy          burst  throughput(sim/theory)  response(sim/theory)  |Δthroughput|  |Δresponse|")

	for _, policy := range policies {
		for _, burst := range bursts {
			cfg := engine.DefaultConfig(policy, burst)
			summary, err := experiment.Run(cfg, trialCount, *seed)
			if err != nil {
				log.Fatalf("run %s/%d: %v", policy, burst, err)
			}
			summaries = append(summaries, summary)
			fmt.Printf("%-15s %5d  %9.3f/%9.3f  %9.5f/%9.5f  %13.3f  %11.5f\n",
				policy,
				burst,
				summary.MeanThroughput,
				summary.Analytical.Throughput,
				summary.MeanResponseTime,
				summary.Analytical.AverageResponseTime,
				summary.ThroughputAbsoluteError,
				summary.ResponseAbsoluteError,
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
	if *traceCSV != "" {
		if err := writeTraceCSV(*traceCSV, summaries); err != nil {
			log.Fatalf("write trace CSV: %v", err)
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
		"policy", "burst_size", "trials", "mean_throughput", "theoretical_throughput",
		"mean_response_time", "theoretical_response_time", "mean_completed", "mean_unfinished",
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
			float(summary.Analytical.Throughput),
			float(summary.MeanResponseTime),
			float(summary.Analytical.AverageResponseTime),
			float(summary.MeanCompleted),
			float(summary.MeanUnfinished),
			float(summary.ThroughputAbsoluteError),
			float(summary.ResponseAbsoluteError),
		}); err != nil {
			return err
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

func createCSV(path string) (*os.File, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	return os.Create(path)
}

func float(value float64) string {
	return strconv.FormatFloat(value, 'g', -1, 64)
}
