// Package abd implements a durable multiwriter ABD register.
package abd

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"mc714-t2/internal/config"
	"mc714-t2/internal/faults"
	"mc714-t2/internal/wire"
)

const (
	messageTimeout = 500 * time.Millisecond
	retryDelay     = 100 * time.Millisecond
)

// Service coordinates operations independently of replica request handling.
type Service struct {
	cluster   config.Cluster
	store     *Store
	client    *http.Client
	ctx       context.Context
	cancel    context.CancelFunc
	gate      chan struct{}
	sequence  uint64 // guarded by gate
	messages  atomic.Uint64
	attempts  atomic.Uint64
	closeOnce sync.Once
	closeErr  error
}

// ServiceOptions configures optional rejoin and fault injection.
type ServiceOptions struct {
	AllowRejoin bool
	Faults      faults.Config
}

func NewService(cluster config.Cluster, dir string) (*Service, error) {
	return NewServiceOpts(cluster, dir, ServiceOptions{})
}

func NewServiceOpts(cluster config.Cluster, dir string, opts ServiceOptions) (*Service, error) {
	store, err := OpenStoreOpts(dir, cluster, OpenOptions{AllowRejoin: opts.AllowRejoin})
	if err != nil {
		return nil, err
	}
	rejoined, err := store.MarkStartedOrRejoin(opts.AllowRejoin)
	if err != nil {
		return nil, errors.Join(err, store.Close())
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	// Transport detaches dials from request cancellation; bound DNS/TCP separately.
	transport.DialContext = (&net.Dialer{Timeout: messageTimeout, KeepAlive: 30 * time.Second}).DialContext
	transport.MaxIdleConns = 16
	transport.MaxIdleConnsPerHost = 2
	transport.MaxConnsPerHost = 2
	transport.ResponseHeaderTimeout = messageTimeout
	ctx, cancel := context.WithCancel(context.Background())
	service := &Service{
		cluster: cluster, store: store, ctx: ctx, cancel: cancel,
		gate: make(chan struct{}, 1),
		client: &http.Client{
			Transport:     faults.Wrap(transport, opts.Faults),
			Timeout:       messageTimeout,
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		},
	}
	if rejoined {
		// Catch up from a majority before accepting client writes.
		syncCtx, syncCancel := context.WithTimeout(ctx, 3*time.Second)
		_, syncErr := service.Read(syncCtx)
		syncCancel()
		if syncErr != nil {
			_ = service.Close()
			return nil, fmt.Errorf("rejoin sync from majority: %w", syncErr)
		}
	}
	return service, nil
}

func (s *Service) Register(mux *http.ServeMux) {
	mux.HandleFunc("POST /abd/messages", s.handleMessage)
	mux.HandleFunc("POST /abd/read", func(w http.ResponseWriter, r *http.Request) { s.handleOperation(w, r, "read") })
	mux.HandleFunc("POST /abd/write", func(w http.ResponseWriter, r *http.Request) { s.handleOperation(w, r, "write") })
	mux.HandleFunc("GET /abd/state", s.handleState)
}

// Close is called after draining the HTTP server. Cancellation also releases an
// active coordinator, joining its phase workers before closing durable storage.
func (s *Service) Close() error {
	s.closeOnce.Do(func() {
		s.cancel()
		s.gate <- struct{}{}
		defer func() { <-s.gate }()
		s.client.CloseIdleConnections()
		s.closeErr = s.store.Close()
	})
	return s.closeErr
}

func (s *Service) nextMessage() (uint64, error) {
	for {
		current := s.messages.Load()
		if current == math.MaxUint64 {
			return 0, errors.New("ABD message counter exhausted")
		}
		if s.messages.CompareAndSwap(current, current+1) {
			return current + 1, nil
		}
	}
}

func (s *Service) frame(kind wire.Kind, to string, request wire.RequestID, register *wire.Register) (wire.Message, error) {
	id, err := s.nextMessage()
	if err != nil {
		return wire.Message{}, err
	}
	return wire.Message{
		Version: wire.ProtocolVersion, RunID: s.cluster.RunID, ClusterID: s.cluster.ClusterID,
		From: s.cluster.NodeID, To: to, MessageID: id, Kind: kind, Request: request, Register: register,
	}, nil
}

func (s *Service) handleMessage(w http.ResponseWriter, r *http.Request) {
	message, err := wire.DecodeMessage(r.Body, s.cluster)
	if err != nil || r.Context().Err() != nil {
		http.Error(w, "invalid message or context", http.StatusBadRequest)
		return
	}
	if message.Kind != wire.ABDQuery && message.Kind != wire.ABDUpdate {
		http.Error(w, "only ABD query and update messages are supported", http.StatusUnprocessableEntity)
		return
	}
	if s.ctx.Err() != nil {
		http.Error(w, "service closed", http.StatusServiceUnavailable)
		return
	}
	// Reserve a response ID before mutating durable state; exhaustion never ACKs.
	reply, err := s.frame(wire.ABDState, message.From, message.Request, nil)
	if err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	var register wire.Register
	if message.Kind == wire.ABDUpdate {
		reply.Kind = wire.ABDAck
		register, err = s.store.Apply(*message.Register)
	} else {
		register, err = s.store.Read()
	}
	if err != nil {
		status := http.StatusServiceUnavailable
		if errors.Is(err, ErrConflict) {
			status = http.StatusConflict
		}
		http.Error(w, err.Error(), status)
		return
	}
	reply.Register = &register
	writeJSON(w, http.StatusOK, reply)
}

func (s *Service) handleState(w http.ResponseWriter, _ *http.Request) {
	register, err := s.store.Read()
	if err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	writeJSON(w, http.StatusOK, struct {
		NodeID            string        `json:"node_id"`
		RunID             string        `json:"run_id"`
		ClusterID         string        `json:"cluster_id"`
		Register          wire.Register `json:"register"`
		LogicalMessages   uint64        `json:"logical_messages"`
		TransportAttempts uint64        `json:"transport_attempts"`
	}{s.cluster.NodeID, s.cluster.RunID, s.cluster.ClusterID, register, s.messages.Load(), s.attempts.Load()})
}

func decodeOperation(reader io.Reader, operation string) (wire.ABDRequest, error) {
	limited := &io.LimitedReader{R: reader, N: 1025}
	decoder := json.NewDecoder(limited)
	var request wire.ABDRequest
	if token, err := decoder.Token(); err != nil || token != json.Delim('{') {
		return request, errors.New("ABD operation requires one JSON object")
	}
	seen := make(map[string]bool, 2)
	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return request, err
		}
		key, ok := token.(string)
		if !ok || seen[key] || (key != "timeout_ms" && key != "value") || (key == "value" && operation != "write") {
			return request, errors.New("unknown, duplicate or forbidden ABD field")
		}
		seen[key] = true
		var value *int64
		if err := decoder.Decode(&value); err != nil || value == nil {
			return request, errors.New("ABD fields must be nonnull integers")
		}
		if key == "timeout_ms" {
			request.TimeoutMS = *value
		} else {
			request.Value = value
		}
	}
	if token, err := decoder.Token(); err != nil || token != json.Delim('}') {
		return request, errors.New("invalid ABD object")
	}
	if _, err := decoder.Token(); err != io.EOF || limited.N == 0 {
		return request, errors.New("ABD operation must be one JSON object of at most 1 KiB")
	}
	if !seen["timeout_ms"] || request.TimeoutMS < 1 || request.TimeoutMS > 3000 || (operation == "write" && request.Value == nil) {
		return request, errors.New("timeout_ms must be 1..3000; writes require value")
	}
	return request, nil
}

func (s *Service) handleOperation(w http.ResponseWriter, r *http.Request, operation string) {
	outcome := "failed"
	if operation == "write" {
		outcome = "not_applied"
		if s.cluster.Mode != config.Standalone {
			writeJSON(w, http.StatusForbidden, wire.ABDError{
				Status: "error", Error: "direct writes are disabled in integrated mode", Outcome: outcome,
			})
			return
		}
	}
	input, err := decodeOperation(r.Body, operation)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, wire.ABDError{Status: "error", Error: err.Error(), Outcome: outcome})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), time.Duration(input.TimeoutMS)*time.Millisecond)
	defer cancel()
	result, failure := s.operate(ctx, operation, input.Value)
	if failure != nil {
		writeJSON(w, failure.Status, wire.ABDError{
			Status: "error", Error: failure.Error(), Outcome: failure.Outcome, Request: failure.Request,
		})
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func operationContext(operation, service context.Context) error {
	if err := service.Err(); err != nil {
		return err
	}
	return operation.Err()
}

type phaseReply struct {
	register wire.Register
	err      error
}

// quorum always counts the local durable replica exactly once. Workers are
// canceled and joined before returning, including when the first majority wins.
func (s *Service) quorum(ctx context.Context, request wire.RequestID, proposed *wire.Register) (wire.Register, bool, error) {
	if err := operationContext(ctx, s.ctx); err != nil {
		return wire.Register{}, false, err
	}
	kind := wire.ABDQuery
	var local wire.Register
	var err error
	propagated := proposed != nil
	if proposed != nil {
		kind = wire.ABDUpdate
		// An unsuccessful durable local Apply may already have reached disk.
		local, err = s.store.Apply(*proposed)
	} else {
		local, err = s.store.Read()
	}
	if err != nil {
		return wire.Register{}, propagated, err
	}
	if proposed != nil {
		if err := validACK(local, *proposed); err != nil {
			return wire.Register{}, propagated, err
		}
	}
	messages := make([]wire.Message, 0, len(s.cluster.Members)-1)
	for _, member := range s.cluster.Members {
		if member.ID == s.cluster.NodeID {
			continue
		}
		message, err := s.frame(kind, member.ID, request, proposed)
		if err != nil {
			return wire.Register{}, propagated, err
		}
		messages = append(messages, message)
	}
	phaseCtx, cancel := context.WithCancel(ctx)
	var workers sync.WaitGroup
	defer func() { cancel(); workers.Wait() }()
	replies := make(chan phaseReply, len(messages))
	for i, message := range messages {
		workers.Add(1)
		go func(target string, message wire.Message) {
			defer workers.Done()
			register, err := s.deliver(phaseCtx, target, message)
			replies <- phaseReply{register: register, err: err}
		}(s.peerURL(message.To), messages[i])
	}
	best := local
	seen := map[wire.Tag]int64{local.Tag: local.Value}
	for votes := 1; votes < s.cluster.Quorum; {
		select {
		case <-ctx.Done():
			return wire.Register{}, propagated, ctx.Err()
		case reply := <-replies:
			if reply.err != nil {
				return wire.Register{}, propagated, reply.err
			}
			register := reply.register
			if value, ok := seen[register.Tag]; ok && value != register.Value {
				return wire.Register{}, propagated, fmt.Errorf("%w: observed unequal values for tag %v", ErrConflict, register.Tag)
			}
			seen[register.Tag] = register.Value
			if register.Tag.Compare(best.Tag) > 0 {
				best = register
			}
			votes++
		}
	}
	if err := operationContext(ctx, s.ctx); err != nil {
		return wire.Register{}, propagated, err
	}
	return best, propagated, nil
}

func (s *Service) peerURL(id string) string {
	for _, member := range s.cluster.Members {
		if member.ID == id {
			return strings.TrimRight(member.URL, "/") + "/abd/messages"
		}
	}
	return "" // Membership was already validated by config.Load.
}

func validACK(register, proposed wire.Register) error {
	order := register.Tag.Compare(proposed.Tag)
	if order < 0 {
		return errors.New("ABD ACK is older than proposed tag")
	}
	if order == 0 && register.Value != proposed.Value {
		return fmt.Errorf("%w: ACK changed value for proposed tag", ErrConflict)
	}
	return nil
}

func (s *Service) deliver(ctx context.Context, target string, message wire.Message) (wire.Register, error) {
	body, err := json.Marshal(message)
	if err != nil {
		return wire.Register{}, err
	}
	for ctx.Err() == nil {
		attemptCtx, cancel := context.WithTimeout(ctx, messageTimeout)
		register, err := s.attempt(attemptCtx, target, body, message)
		cancel()
		if err == nil || errors.Is(err, ErrConflict) {
			return register, err
		}
		timer := time.NewTimer(retryDelay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return wire.Register{}, ctx.Err()
		case <-timer.C:
		}
	}
	return wire.Register{}, ctx.Err()
}

func (s *Service) attempt(ctx context.Context, target string, body []byte, message wire.Message) (wire.Register, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, target, bytes.NewReader(body))
	if err != nil {
		return wire.Register{}, err
	}
	request.Header.Set("Content-Type", "application/json")
	s.attempts.Add(1)
	response, err := s.client.Do(request)
	if err != nil {
		return wire.Register{}, err
	}
	defer response.Body.Close()
	if response.StatusCode == http.StatusConflict {
		return wire.Register{}, fmt.Errorf("%w: ABD peer %s rejected update", ErrConflict, message.To)
	}
	if response.StatusCode != http.StatusOK {
		return wire.Register{}, fmt.Errorf("ABD peer %s returned HTTP %d", message.To, response.StatusCode)
	}
	reply, err := wire.DecodeMessage(response.Body, s.cluster)
	if err != nil {
		return wire.Register{}, err
	}
	expected := wire.ABDState
	if message.Kind == wire.ABDUpdate {
		expected = wire.ABDAck
	}
	if reply.Kind != expected || reply.Request != message.Request || reply.From != message.To || reply.To != s.cluster.NodeID {
		return wire.Register{}, errors.New("ABD response does not match peer, kind or request")
	}
	if err := ctx.Err(); err != nil {
		return wire.Register{}, err
	}
	if message.Kind == wire.ABDUpdate {
		if err := validACK(*reply.Register, *message.Register); err != nil {
			return wire.Register{}, err
		}
	}
	return *reply.Register, nil
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
