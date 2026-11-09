package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"mc714-t2/internal/wire"
)

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil && !errors.Is(err, flag.ErrHelp) {
		fmt.Fprintln(os.Stderr, "lab:", err)
		os.Exit(1)
	}
}

func run(args []string, output io.Writer) error {
	flags := flag.NewFlagSet("lab", flag.ContinueOnError)
	action := flags.String("action", "health", "health, ra, observe, read, write, or increment")
	url := flags.String("url", "http://127.0.0.1:8080", "node base URL")
	timeout := flags.Duration("timeout", 5*time.Second, "maximum request or graceful shutdown duration")
	holdMS := flags.Int64("hold-ms", 50, "critical-section work duration in milliseconds (0..500)")
	waitMS := flags.Int64("wait-ms", 3000, "operation deadline in milliseconds (1..3000)")
	value := flags.Int64("value", 0, "register value (required explicitly for write)")
	listen := flags.String("listen", "127.0.0.1:18089", "observer HTTP listen address")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("unexpected arguments: %v", flags.Args())
	}
	if *timeout <= 0 {
		return errors.New("-timeout must be positive")
	}
	if *holdMS < 0 || *holdMS > 500 || *waitMS < 1 || *waitMS > 3000 {
		return errors.New("-hold-ms must be 0..500 and -wait-ms must be 1..3000")
	}
	valueProvided := false
	flags.Visit(func(f *flag.Flag) {
		if f.Name == "value" {
			valueProvided = true
		}
	})
	if *action == "write" && !valueProvided {
		return errors.New("-action write requires explicit -value")
	}
	if *action != "write" && valueProvided {
		return errors.New("-value is only valid with -action write")
	}
	if *action == "observe" {
		return observe(*listen, *timeout, output)
	}
	client := &http.Client{Timeout: *timeout}
	defer client.CloseIdleConnections()
	switch *action {
	case "health":
		health, err := probe(client, *url)
		if err != nil {
			return err
		}
		return json.NewEncoder(output).Encode(health)
	case "ra":
		client.CheckRedirect = func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse }
		result, err := execute(client, *url, wire.ExecuteRequest{HoldMS: *holdMS, WaitMS: *waitMS})
		if err != nil {
			return err
		}
		return json.NewEncoder(output).Encode(result)
	case "read", "write":
		input := wire.ABDRequest{TimeoutMS: *waitMS}
		if *action == "write" {
			input.Value = value
		}
		result, err := executeABD(client, *url, *action, input)
		if err != nil {
			return err
		}
		return json.NewEncoder(output).Encode(result)
	case "increment":
		result, err := executeIncrement(client, *url, wire.IncrementRequest{TimeoutMS: *waitMS})
		if err != nil {
			return err
		}
		return json.NewEncoder(output).Encode(result)
	default:
		return fmt.Errorf("unknown -action %q", *action)
	}
}

func probe(client *http.Client, baseURL string) (wire.Health, error) {
	var health wire.Health
	response, err := client.Get(strings.TrimRight(baseURL, "/") + "/healthz")
	if err != nil {
		return health, fmt.Errorf("probe: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return health, fmt.Errorf("probe: HTTP %d", response.StatusCode)
	}
	const maxResponseBytes = 4096
	body, err := io.ReadAll(io.LimitReader(response.Body, maxResponseBytes+1))
	if err != nil {
		return health, fmt.Errorf("read health response: %w", err)
	}
	if len(body) > maxResponseBytes {
		return health, errors.New("health response exceeds 4096 bytes")
	}
	if err := json.Unmarshal(body, &health); err != nil {
		return health, fmt.Errorf("decode health response: %w", err)
	}
	if health.Status != "ok" || strings.TrimSpace(health.NodeID) == "" || strings.TrimSpace(health.RunID) == "" {
		return health, errors.New("response does not identify a healthy node and execution")
	}
	return health, nil
}

func execute(client *http.Client, baseURL string, input wire.ExecuteRequest) (wire.ExecuteResult, error) {
	var result wire.ExecuteResult
	body, err := json.Marshal(input)
	if err != nil {
		return result, fmt.Errorf("encode execute request: %w", err)
	}
	response, err := client.Post(strings.TrimRight(baseURL, "/")+"/ra/execute", "application/json", bytes.NewReader(body))
	if err != nil {
		return result, fmt.Errorf("execute (outcome may be indeterminate; do not retry automatically): %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return result, fmt.Errorf("execute: HTTP %d (outcome may be indeterminate)", response.StatusCode)
	}
	const maxResponseBytes = 4096
	body, err = io.ReadAll(io.LimitReader(response.Body, maxResponseBytes+1))
	if err != nil {
		return result, fmt.Errorf("read execute response (outcome may be indeterminate): %w", err)
	}
	if len(body) > maxResponseBytes {
		return result, errors.New("execute response exceeds 4096 bytes (outcome may be indeterminate)")
	}
	var completion struct {
		wire.ExecuteResult
		WaitNS *int64 `json:"wait_ns"`
		HeldNS *int64 `json:"held_ns"`
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&completion); err != nil {
		return result, fmt.Errorf("decode execute response (outcome may be indeterminate): %w", err)
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		return result, errors.New("execute response must contain one JSON object (outcome may be indeterminate)")
	}
	if completion.WaitNS == nil || completion.HeldNS == nil {
		return result, errors.New("execute response requires wait_ns and held_ns (outcome may be indeterminate)")
	}
	result = completion.ExecuteResult
	result.WaitNS, result.HeldNS = *completion.WaitNS, *completion.HeldNS
	if result.Status != "completed" || !validIdentity(result.NodeID) || !validIdentity(result.RunID) ||
		!validClusterID(result.ClusterID) || result.Request.Owner != result.NodeID ||
		result.Request.Sequence == 0 || result.WaitNS < 0 || result.HeldNS < 0 {
		return result, errors.New("execute response does not prove completion for a valid request (outcome may be indeterminate)")
	}
	return result, nil
}

func observe(address string, timeout time.Duration, output io.Writer) error {
	recorder := newObserver(output)
	mux := http.NewServeMux()
	recorder.Register(mux)
	server := &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 2 * time.Second,
		ReadTimeout:       5 * time.Second,
		WriteTimeout:      5 * time.Second,
		IdleTimeout:       30 * time.Second,
		MaxHeaderBytes:    16 << 10,
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}
	defer server.Close()
	serveErr := make(chan error, 1)
	go func() { serveErr <- server.Serve(listener) }()
	slog.Info("observer listening", "address", listener.Addr().String())
	select {
	case err := <-serveErr:
		return fmt.Errorf("serve: %w", err)
	case <-ctx.Done():
		stop()
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("shutdown: %w", err)
	}
	if err := <-serveErr; !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("serve during shutdown: %w", err)
	}
	slog.Info("observer stopped")
	return nil
}
