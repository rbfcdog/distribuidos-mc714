package abd

import (
	"context"
	"errors"
	"log/slog"
	"math"
	"net/http"
	"time"

	"mc714-t2/internal/wire"
)

// OperationError preserves whether a failed write may already be visible.
type OperationError struct {
	Status  int
	Outcome string
	Request *wire.RequestID
	Cause   error
}

func (e *OperationError) Error() string { return e.Cause.Error() }
func (e *OperationError) Unwrap() error { return e.Cause }

// Read executes both ABD phases through the same coordinator used by HTTP.
func (s *Service) Read(ctx context.Context) (wire.ABDResult, error) {
	result, failure := s.operate(ctx, "read", nil)
	if failure != nil {
		return result, failure
	}
	return result, nil
}

// Write is the trusted Go operation. The public HTTP handler separately enforces
// the application mode; read-modify-write callers must hold distributed exclusion.
func (s *Service) Write(ctx context.Context, value int64) (wire.ABDResult, error) {
	result, failure := s.operate(ctx, "write", &value)
	if failure != nil {
		return result, failure
	}
	return result, nil
}

func (s *Service) operate(ctx context.Context, operation string, value *int64) (wire.ABDResult, *OperationError) {
	started := time.Now()
	ctx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(s.ctx, cancel)
	defer stop()
	defer cancel()
	outcome := "failed"
	if operation == "write" {
		outcome = "not_applied"
	}
	fail := func(status int, cause error, request *wire.RequestID) (wire.ABDResult, *OperationError) {
		return wire.ABDResult{}, &OperationError{Status: status, Outcome: outcome, Request: request, Cause: cause}
	}
	// ponytail: one coordinator per node; split the read/write gate if measured contention warrants it.
	select {
	case s.gate <- struct{}{}:
		defer func() { <-s.gate }()
	default:
		return fail(http.StatusTooManyRequests, errors.New("ABD coordinator busy"), nil)
	}
	if err := operationContext(ctx, s.ctx); err != nil {
		return fail(http.StatusServiceUnavailable, err, nil)
	}
	if s.sequence == math.MaxUint64 {
		return fail(http.StatusServiceUnavailable, errors.New("ABD operation sequence exhausted"), nil)
	}
	s.sequence++
	request := wire.RequestID{Owner: s.cluster.NodeID, Sequence: s.sequence}
	phase := "query"
	var err error
	defer func() {
		if err != nil {
			slog.Warn("abd_failure", "run", s.cluster.RunID, "node", s.cluster.NodeID, "request", request, "operation", operation, "phase", phase, "outcome", outcome, "error", err)
		}
	}()
	slog.Info("abd_phase", "run", s.cluster.RunID, "node", s.cluster.NodeID, "request", request, "operation", operation, "phase", phase)
	register, _, err := s.quorum(ctx, request, nil)
	if err == nil && operation == "write" {
		if register.Tag.Counter == math.MaxUint64 {
			err = errors.New("ABD register counter exhausted")
		} else {
			register = wire.Register{Tag: wire.Tag{Counter: register.Tag.Counter + 1, WriterID: s.cluster.NodeID}, Value: *value}
		}
	}
	if err == nil {
		phase = "update"
		slog.Info("abd_phase", "run", s.cluster.RunID, "node", s.cluster.NodeID, "request", request, "operation", operation, "phase", phase)
		var propagated bool
		_, propagated, err = s.quorum(ctx, request, &register)
		if operation == "write" && propagated {
			outcome = "indeterminate"
		}
	}
	if err != nil {
		status := http.StatusServiceUnavailable
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
			status = http.StatusGatewayTimeout
		} else if errors.Is(err, ErrConflict) {
			status = http.StatusConflict
		}
		return fail(status, err, &request)
	}
	slog.Info("abd_completed", "run", s.cluster.RunID, "node", s.cluster.NodeID, "request", request, "operation", operation, "tag", register.Tag)
	return wire.ABDResult{
		Status: "completed", Operation: operation, NodeID: s.cluster.NodeID, RunID: s.cluster.RunID,
		ClusterID: s.cluster.ClusterID, Request: request, Register: &register, DurationNS: time.Since(started).Nanoseconds(),
	}, nil
}
