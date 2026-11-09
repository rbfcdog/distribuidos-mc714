// Command load records open-loop Poisson arrivals without retries or coordinated omission.
package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"math/rand/v2"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"mc714-t2/internal/wire"
)

type options struct {
	algorithm, targets, runID, output            string
	rate                                         float64
	warmup, duration, clientTimeout              time.Duration
	seed                                         int64
	workers, queue, waitMS, holdMS, writePercent int
}

func parseOptions(args []string) (options, error) {
	var o options
	f := flag.NewFlagSet("load", flag.ContinueOnError)
	f.StringVar(&o.algorithm, "algorithm", "", "ra or abd")
	f.StringVar(&o.targets, "targets", "", "comma-separated node1..N HTTP base URLs")
	f.StringVar(&o.runID, "run-id", "", "expected cluster run ID")
	f.StringVar(&o.output, "output", "", "raw JSONL evidence path (must not exist)")
	f.Float64Var(&o.rate, "rate", 0, "aggregate Poisson arrivals per second")
	f.DurationVar(&o.warmup, "warmup", 10*time.Second, "warmup offer window")
	f.DurationVar(&o.duration, "duration", 60*time.Second, "measurement offer window")
	f.DurationVar(&o.clientTimeout, "client-timeout", 2*time.Second, "per-request deadline")
	f.Int64Var(&o.seed, "seed", 1, "reproducible arrival and operation seed")
	f.IntVar(&o.workers, "workers", 32, "bounded HTTP worker count")
	f.IntVar(&o.queue, "queue", 128, "bounded pending arrival capacity")
	f.IntVar(&o.waitMS, "wait-ms", 1000, "server operation deadline (1..3000 ms)")
	f.IntVar(&o.holdMS, "hold-ms", 5, "RA work duration (0..500 ms)")
	f.IntVar(&o.writePercent, "write-percent", 50, "ABD write percentage (0..100)")
	if err := f.Parse(args); err != nil {
		return o, err
	}
	if f.NArg() != 0 {
		return o, fmt.Errorf("unexpected arguments: %v", f.Args())
	}
	if o.algorithm != "ra" && o.algorithm != "abd" {
		return o, errors.New("-algorithm must be ra or abd")
	}
	if !validID(o.runID) || o.output == "" {
		return o, errors.New("valid -run-id and nonempty -output are required")
	}
	if o.rate <= 0 || math.IsNaN(o.rate) || math.IsInf(o.rate, 0) {
		return o, errors.New("-rate must be positive and finite")
	}
	if o.warmup < 0 || o.duration <= 0 || o.clientTimeout <= 0 {
		return o, errors.New("warmup must be nonnegative; duration and client-timeout must be positive")
	}
	if o.workers <= 0 || o.queue < 0 {
		return o, errors.New("workers must be positive and queue nonnegative")
	}
	if o.waitMS < 1 || o.waitMS > 3000 || o.holdMS < 0 || o.holdMS > 500 || o.writePercent < 0 || o.writePercent > 100 {
		return o, errors.New("wait-ms must be 1..3000, hold-ms 0..500, write-percent 0..100")
	}
	return o, nil
}

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil && !errors.Is(err, flag.ErrHelp) {
		fmt.Fprintln(os.Stderr, "load:", err)
		os.Exit(1)
	}
}

func run(args []string, lifecycle io.Writer) error {
	o, err := parseOptions(args)
	if err != nil {
		return err
	}
	targets, err := parseTargets(o.targets)
	if err != nil {
		return err
	}
	transport := &http.Transport{
		Proxy:        nil,
		DialContext:  (&net.Dialer{Timeout: o.clientTimeout, KeepAlive: 30 * time.Second}).DialContext,
		MaxIdleConns: o.workers + len(targets), MaxIdleConnsPerHost: o.workers,
		IdleConnTimeout: 90 * time.Second, ResponseHeaderTimeout: o.clientTimeout,
		DisableCompression: true,
	}
	client := &http.Client{Transport: transport, Timeout: o.clientTimeout,
		CheckRedirect: func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse },
	}
	defer client.CloseIdleConnections()
	cluster, err := discover(client, targets, o.runID)
	if err != nil {
		return err
	}
	file, err := os.OpenFile(o.output, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0644)
	if err != nil {
		return err
	}
	closed := false
	defer func() {
		if !closed {
			_ = file.Close()
		}
	}()
	records := &recorder{encoder: json.NewEncoder(file)}
	records.emit(map[string]any{
		"type": "metadata", "schema": 1, "algorithm": o.algorithm, "run_id": o.runID,
		"cluster_id": cluster, "targets": targets, "nodes": len(targets), "rate": o.rate,
		"seed": o.seed, "workers": o.workers, "queue": o.queue, "wait_ms": o.waitMS,
		"client_timeout_ns": o.clientTimeout.Nanoseconds(), "hold_ms": o.holdMS,
		"write_percent": o.writePercent, "warmup_ns": o.warmup.Nanoseconds(), "duration_ns": o.duration.Nanoseconds(),
	})
	var template any = wire.ABDRequest{TimeoutMS: int64(o.waitMS)}
	if o.algorithm == "ra" {
		template = wire.ExecuteRequest{HoldMS: int64(o.holdMS), WaitMS: int64(o.waitMS)}
	}
	requestBody, err := json.Marshal(template)
	if err != nil {
		return err
	}
	w := &workload{o: o, client: client, targets: targets, cluster: cluster, records: records, requestBody: requestBody,
		jobs: make(chan job, o.queue), arrivals: rand.New(rand.NewPCG(uint64(o.seed), 0)),
		choices: rand.New(rand.NewPCG(uint64(o.seed), 1)),
	}
	var workers sync.WaitGroup
	for range o.workers {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for j := range w.jobs {
				w.perform(j)
				w.pending.Done()
			}
		}()
	}
	defer func() { close(w.jobs); workers.Wait() }()
	if err := w.phase("warmup", o.warmup, nil); err != nil {
		return err
	}
	if err := w.snapshots("baseline"); err != nil {
		return err
	}
	if err := w.phase("measurement", o.duration, lifecycle); err != nil {
		return err
	}
	if err := w.snapshots("final"); err != nil {
		return err
	}
	if err := records.failure(); err != nil {
		return err
	}
	if err := file.Sync(); err != nil {
		return fmt.Errorf("sync raw evidence: %w", err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("close raw evidence: %w", err)
	}
	closed = true
	return json.NewEncoder(lifecycle).Encode(map[string]any{"event": "finished"})
}

func parseTargets(raw string) ([]string, error) {
	targets := strings.Split(raw, ",")
	if len(targets) != 3 && len(targets) != 5 && len(targets) != 7 {
		return nil, errors.New("targets must contain 3, 5 or 7 nodes")
	}
	seen := make(map[string]bool, len(targets))
	for i, raw := range targets {
		u, err := url.Parse(strings.TrimSpace(raw))
		if err != nil || u.Scheme != "http" || u.Host == "" || u.Hostname() == "" || u.User != nil || (u.Path != "" && u.Path != "/") || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || u.Opaque != "" {
			return nil, fmt.Errorf("invalid HTTP base URL %q", raw)
		}
		u.Path = ""
		targets[i] = u.String()
		if seen[targets[i]] {
			return nil, errors.New("duplicate target URL")
		}
		seen[targets[i]] = true
	}
	return targets, nil
}

type recorder struct {
	mu      sync.Mutex
	encoder *json.Encoder
	err     error
}

func (r *recorder) emit(value any) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.err == nil {
		r.err = r.encoder.Encode(value)
	}
}

func (r *recorder) failure() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.err
}

type operation struct {
	Type           string `json:"type"`
	Phase          string `json:"phase"`
	ID             int64  `json:"id"`
	NodeID         string `json:"node_id"`
	Operation      string `json:"operation"`
	ScheduledNS    int64  `json:"scheduled_ns"`
	GeneratorLagNS int64  `json:"generator_lag_ns"`
	StartNS        *int64 `json:"start_ns"`
	EndNS          int64  `json:"end_ns"`
	HTTPStatus     int    `json:"http_status"`
	Outcome        string `json:"outcome"`
	WriteOutcome   string `json:"write_outcome,omitempty"`
	Value          *int64 `json:"value,omitempty"`
	WaitNS         *int64 `json:"wait_ns,omitempty"`
	HeldNS         *int64 `json:"held_ns,omitempty"`
	DurationNS     *int64 `json:"duration_ns,omitempty"`
	Error          string `json:"error,omitempty"`
}

type job struct {
	row    operation
	target int
	origin time.Time
}

type workload struct {
	o                 options
	client            *http.Client
	targets           []string
	cluster           string
	records           *recorder
	jobs              chan job
	requestBody       []byte
	pending           sync.WaitGroup
	arrivals, choices *rand.Rand
	id                int64
}

// Each exponential interval advances the intended schedule, never the wakeup time.
// The independent choices stream keeps identical seeds paired across cluster sizes.
func nextArrival(rng *rand.Rand, previous, rate float64) float64 {
	return previous + rng.ExpFloat64()/rate*float64(time.Second)
}

func (w *workload) phase(name string, duration time.Duration, lifecycle io.Writer) error {
	origin := time.Now()
	w.records.emit(map[string]any{"type": "phase", "phase": name, "event": "start", "unix_ns": origin.UnixNano()})
	if err := w.records.failure(); err != nil {
		return err
	}
	if lifecycle != nil {
		if err := json.NewEncoder(lifecycle).Encode(map[string]any{"event": "measurement_start", "unix_ns": origin.UnixNano()}); err != nil {
			return err
		}
	}
	var phaseErr error
	for scheduled := nextArrival(w.arrivals, 0, w.o.rate); scheduled < float64(duration); scheduled = nextArrival(w.arrivals, scheduled, w.o.rate) {
		if err := w.records.failure(); err != nil {
			phaseErr = err
			break
		}
		ns := int64(scheduled)
		if delay := time.Until(origin.Add(time.Duration(ns))); delay > 0 {
			time.Sleep(delay)
		}
		if w.id == math.MaxInt64 {
			phaseErr = errors.New("operation ID exhausted")
			break
		}
		w.id++
		node := int(w.choices.Uint64() % uint64(len(w.targets)))
		kind := "execute"
		choice := int(w.choices.Uint64() % 100)
		if w.o.algorithm == "abd" {
			kind = "read"
			if choice < w.o.writePercent {
				kind = "write"
			}
		}
		row := operation{Type: "operation", Phase: name, ID: w.id, NodeID: fmt.Sprintf("node%d", node+1), Operation: kind, ScheduledNS: ns}
		if kind == "write" {
			value := w.id
			row.Value = &value
		}
		offered := time.Since(origin).Nanoseconds()
		row.GeneratorLagNS = offered - ns
		w.offer(job{row: row, target: node, origin: origin})
	}
	// Even an empty Poisson window lasts its full configured duration.
	if phaseErr == nil {
		if delay := time.Until(origin.Add(duration)); delay > 0 {
			time.Sleep(delay)
		}
	}
	w.pending.Wait()
	end := time.Now()
	w.records.emit(map[string]any{"type": "phase", "phase": name, "event": "end", "unix_ns": end.UnixNano(), "elapsed_ns": end.Sub(origin).Nanoseconds(), "wall_elapsed_ns": end.UnixNano() - origin.UnixNano()})
	if phaseErr != nil {
		return phaseErr
	}
	return w.records.failure()
}

func (w *workload) offer(j job) {
	w.pending.Add(1)
	select {
	case w.jobs <- j:
	default:
		j.row.EndNS = time.Since(j.origin).Nanoseconds()
		j.row.Outcome, j.row.Error = "queue_full", "bounded worker queue is full; request not sent"
		if j.row.Operation == "write" {
			j.row.WriteOutcome = "not_applied"
		}
		w.records.emit(j.row)
		w.pending.Done()
	}
}
