package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"mc714-t2/internal/abd"
	"mc714-t2/internal/config"
	"mc714-t2/internal/counter"
	"mc714-t2/internal/ra"
	"mc714-t2/internal/wire"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stderr, nil)))
	if err := run(os.Args[1:]); err != nil && !errors.Is(err, flag.ErrHelp) {
		slog.Error("node failed", "error", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	flags := flag.NewFlagSet("node", flag.ContinueOnError)
	id := flags.String("id", "", "unique node identifier (required)")
	runID := flags.String("run-id", "", "experiment identifier (required)")
	listen := flags.String("listen", "127.0.0.1:8080", "HTTP listen address")
	membership := flags.String("config", "", "membership JSON path (required)")
	observer := flags.String("observer", "", "optional correctness observer base URL")
	dataDir := flags.String("data-dir", ".state", "durable state root (scoped by run and node)")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("unexpected arguments: %v", flags.Args())
	}
	if strings.TrimSpace(*dataDir) == "" {
		return errors.New("-data-dir must not be empty")
	}
	cluster, err := config.Load(*membership, *id, *runID)
	if err != nil {
		return fmt.Errorf("load cluster: %w", err)
	}

	body, err := json.Marshal(wire.Health{Status: "ok", NodeID: *id, RunID: *runID})
	if err != nil {
		return fmt.Errorf("encode health response: %w", err)
	}
	clusterBody, err := json.Marshal(cluster)
	if err != nil {
		return fmt.Errorf("encode cluster response: %w", err)
	}
	service, err := ra.NewService(cluster, *observer)
	if err != nil {
		return fmt.Errorf("start Ricart-Agrawala: %w", err)
	}
	defer service.Close()
	replicaDir := filepath.Join(*dataDir, cluster.RunID, cluster.NodeID)
	replication, err := abd.NewService(cluster, replicaDir)
	if err != nil {
		return fmt.Errorf("start ABD: %w", err)
	}
	defer func() {
		if err := replication.Close(); err != nil {
			slog.Error("ABD close failed", "error", err)
		}
	}()
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(body)
	})
	mux.HandleFunc("GET /cluster", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(clusterBody)
	})
	service.Register(mux)
	replication.Register(mux)
	counter.NewService(cluster, service, replication).Register(mux)
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
	listener, err := net.Listen("tcp", *listen)
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}
	defer server.Close()
	serveErr := make(chan error, 1)
	go func() { serveErr <- server.Serve(listener) }()
	slog.Info("node listening", "node_id", *id, "run_id", *runID, "cluster_id", cluster.ClusterID, "members", len(cluster.Members), "address", listener.Addr().String())

	select {
	case err := <-serveErr:
		return fmt.Errorf("serve: %w", err)
	case <-ctx.Done():
		stop()
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("shutdown: %w", err)
	}
	if err := <-serveErr; !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("serve during shutdown: %w", err)
	}
	slog.Info("node stopped", "node_id", *id, "run_id", *runID)
	return nil
}
