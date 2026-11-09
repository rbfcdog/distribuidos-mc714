// Package counter composes RA exclusion with the durable ABD register.
package counter

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"math"
	"net/http"
	"time"

	"mc714-t2/internal/abd"
	"mc714-t2/internal/config"
	"mc714-t2/internal/ra"
	"mc714-t2/internal/wire"
)

var ErrOverflow = errors.New("increment would overflow int64")

type Service struct {
	cluster  config.Cluster
	guard    *ra.Service
	register *abd.Service
}

func NewService(cluster config.Cluster, guard *ra.Service, register *abd.Service) *Service {
	return &Service{cluster: cluster, guard: guard, register: register}
}

func (s *Service) Register(mux *http.ServeMux) {
	mux.HandleFunc("POST /increment", s.handleIncrement)
}

func (s *Service) handleIncrement(w http.ResponseWriter, r *http.Request) {
	started := time.Now()
	outcome := "not_applied"
	var request wire.RequestID
	fail := func(status int, err error) {
		var identity *wire.RequestID
		if request != (wire.RequestID{}) {
			identity = &request
		}
		slog.Warn("increment_failure", "run", s.cluster.RunID, "node", s.cluster.NodeID, "request", request, "outcome", outcome, "error", err)
		writeJSON(w, status, wire.ABDError{Status: "error", Error: err.Error(), Outcome: outcome, Request: identity})
	}
	if s.cluster.Mode != config.Integrated {
		fail(http.StatusForbidden, errors.New("increment requires integrated mode"))
		return
	}
	input, err := decodeIncrement(r.Body)
	if err != nil {
		fail(http.StatusBadRequest, err)
		return
	}
	// Only acquisition follows the client connection. Once HELD, both ABD
	// operations share the original bounded budget, even after a disconnect.
	operationCtx, cancelOperation := context.WithDeadline(context.Background(), started.Add(time.Duration(input.TimeoutMS)*time.Millisecond))
	defer cancelOperation()
	acquireCtx, cancelAcquire := context.WithCancel(r.Context())
	defer cancelAcquire()
	stop := context.AfterFunc(operationCtx, cancelAcquire)
	defer stop()
	var before int64
	var written wire.ABDResult
	result, err := s.guard.RunExclusive(acquireCtx, func() error {
		read, err := s.register.Read(operationCtx)
		if err != nil {
			return err
		}
		before = read.Register.Value
		if before == math.MaxInt64 {
			return ErrOverflow
		}
		outcome = "indeterminate"
		written, err = s.register.Write(operationCtx, before+1)
		var operationError *abd.OperationError
		if errors.As(err, &operationError) && operationError.Outcome == "not_applied" {
			outcome = "not_applied"
		}
		return err
	})
	request = result.Request
	if err != nil {
		status := http.StatusServiceUnavailable
		var operationError *abd.OperationError
		switch {
		case errors.Is(err, ra.ErrBusy):
			status = http.StatusTooManyRequests
		case errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled):
			status = http.StatusGatewayTimeout
		case errors.Is(err, ErrOverflow):
			status = http.StatusConflict
		case errors.As(err, &operationError):
			status = operationError.Status
		}
		fail(status, err)
		return
	}
	slog.Info("increment_completed", "run", s.cluster.RunID, "node", s.cluster.NodeID, "request", request, "before", before, "tag", written.Register.Tag)
	writeJSON(w, http.StatusOK, wire.IncrementResult{
		Status: "completed", NodeID: s.cluster.NodeID, RunID: s.cluster.RunID, ClusterID: s.cluster.ClusterID,
		Request: request, Before: &before, Register: written.Register, DurationNS: time.Since(started).Nanoseconds(),
	})
}

func decodeIncrement(reader io.Reader) (wire.IncrementRequest, error) {
	limited := &io.LimitedReader{R: reader, N: 1025}
	decoder := json.NewDecoder(limited)
	var request wire.IncrementRequest
	if token, err := decoder.Token(); err != nil || token != json.Delim('{') {
		return request, errors.New("increment requires one JSON object")
	}
	seen := false
	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return request, err
		}
		if token != "timeout_ms" || seen {
			return request, errors.New("unknown or duplicate increment field")
		}
		seen = true
		var value *int64
		if err := decoder.Decode(&value); err != nil || value == nil {
			return request, errors.New("timeout_ms must be a nonnull integer")
		}
		request.TimeoutMS = *value
	}
	if token, err := decoder.Token(); err != nil || token != json.Delim('}') {
		return request, errors.New("invalid increment object")
	}
	if _, err := decoder.Token(); err != io.EOF || limited.N == 0 {
		return request, errors.New("increment must be one JSON object of at most 1 KiB")
	}
	if !seen || request.TimeoutMS < 1 || request.TimeoutMS > 3000 {
		return request, errors.New("timeout_ms must be 1..3000")
	}
	return request, nil
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
