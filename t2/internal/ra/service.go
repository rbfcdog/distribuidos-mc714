package ra

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"mc714-t2/internal/config"
	"mc714-t2/internal/wire"
)

const (
	messageTimeout  = 500 * time.Millisecond
	retryDelay      = 100 * time.Millisecond
	observerTimeout = 250 * time.Millisecond
)

// Service owns bounded HTTP delivery; the engine never performs network I/O.
type Service struct {
	cluster     config.Cluster
	engine      *Engine
	client      *http.Client
	observerURL string
	ctx         context.Context
	cancel      context.CancelFunc
	workers     sync.WaitGroup
	peers       map[string]*replyOutbox
	attempts    atomic.Uint64
}

// One immutable latest reply per peer is enough: requests are sequential at
// their owner, and an older reply can never authorize a newer acquisition.
type replyOutbox struct {
	url       string
	mu        sync.Mutex
	latest    wire.Message
	delivered uint64
	wake      chan struct{}
}

func NewService(cluster config.Cluster, observerURL string) (*Service, error) {
	if observerURL != "" {
		u, err := url.Parse(observerURL)
		if err != nil || u == nil || (u.Scheme != "http" && u.Scheme != "https") || u.Hostname() == "" || u.User != nil || u.Opaque != "" || u.RawQuery != "" || u.ForceQuery || strings.Contains(observerURL, "#") || u.RawPath != "" || (u.Path != "" && u.Path != "/") {
			return nil, errors.New("observer URL must be an HTTP(S) base URL without credentials, query, fragment or non-root path")
		}
		if port := u.Port(); port != "" {
			number, err := strconv.Atoi(port)
			if err != nil || number < 1 || number > 65535 {
				return nil, errors.New("observer URL port must be in 1..65535")
			}
		} else if strings.HasSuffix(u.Host, ":") {
			return nil, errors.New("observer URL has an empty port")
		}
		observerURL = strings.TrimRight(u.String(), "/") + "/events"
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	// Transport detaches dials from request cancellation; bound DNS/TCP separately.
	transport.DialContext = (&net.Dialer{Timeout: messageTimeout, KeepAlive: 30 * time.Second}).DialContext
	transport.MaxIdleConns = 16
	transport.MaxIdleConnsPerHost = 2
	transport.MaxConnsPerHost = 2
	transport.ResponseHeaderTimeout = messageTimeout
	ctx, cancel := context.WithCancel(context.Background())
	s := &Service{
		cluster: cluster, engine: NewEngine(cluster), observerURL: observerURL,
		ctx: ctx, cancel: cancel, peers: make(map[string]*replyOutbox, len(cluster.Members)),
		client: &http.Client{Transport: transport, Timeout: messageTimeout, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }},
	}
	for _, member := range cluster.Members {
		if member.ID == cluster.NodeID {
			continue
		}
		peer := &replyOutbox{url: strings.TrimRight(member.URL, "/") + "/ra/messages", wake: make(chan struct{}, 1)}
		s.peers[member.ID] = peer
		s.workers.Add(1)
		go s.deliverReplies(peer)
	}
	return s, nil
}

func (s *Service) Register(mux *http.ServeMux) {
	mux.HandleFunc("POST /ra/messages", s.handleMessage)
	mux.HandleFunc("GET /ra/state", s.handleState)
	mux.HandleFunc("POST /ra/execute", s.handleExecute)
}

// RunExclusive keeps the RA guard through work and observer cleanup. ctx only
// cancels acquisition; callers must independently bound work after entry.
func (s *Service) RunExclusive(ctx context.Context, work func() error) (wire.ExecuteResult, error) {
	if work == nil {
		return wire.ExecuteResult{}, errors.New("critical-section work is required")
	}
	started := time.Now()
	attempt, err := s.acquire(ctx)
	if err != nil {
		return wire.ExecuteResult{
			NodeID: s.cluster.NodeID, RunID: s.cluster.RunID,
			ClusterID: s.cluster.ClusterID, Request: attempt.Request,
		}, err
	}
	return s.critical(attempt.Request, started, work)
}

func (s *Service) Close() {
	s.cancel()
	s.workers.Wait()
	s.client.CloseIdleConnections()
}

func (s *Service) handleMessage(w http.ResponseWriter, r *http.Request) {
	message, err := wire.DecodeMessage(r.Body, s.cluster)
	if err != nil || r.Context().Err() != nil {
		http.Error(w, "invalid message or context", http.StatusBadRequest)
		return
	}
	if message.Kind != wire.RARequest && message.Kind != wire.RAReply {
		http.Error(w, "only Ricart-Agrawala messages are supported", http.StatusUnprocessableEntity)
		return
	}
	if s.ctx.Err() != nil {
		http.Error(w, "service closed", http.StatusServiceUnavailable)
		return
	}
	replies, err := s.engine.Handle(message)
	if err != nil {
		http.Error(w, err.Error(), http.StatusConflict)
		return
	}
	s.dispatch(replies)
	w.WriteHeader(http.StatusNoContent)
}

func (s *Service) handleState(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(struct {
		Snapshot
		TransportAttempts uint64 `json:"transport_attempts"`
	}{s.engine.Snapshot(), s.attempts.Load()})
}

func (s *Service) handleExecute(w http.ResponseWriter, r *http.Request) {
	request, err := decodeExecute(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	// The callback remains bounded even if the client disconnects after entry.
	ctx, cancel := context.WithTimeout(r.Context(), time.Duration(request.WaitMS)*time.Millisecond)
	stop := context.AfterFunc(s.ctx, cancel)
	defer stop()
	defer cancel()
	result, err := s.RunExclusive(ctx, func() error {
		time.Sleep(time.Duration(request.HoldMS) * time.Millisecond)
		return nil
	})
	if err != nil {
		slog.Warn("ra_acquisition_or_work_failure", "run", s.cluster.RunID, "node", s.cluster.NodeID, "request", result.Request, "error", err)
		status := http.StatusServiceUnavailable
		if errors.Is(err, ErrBusy) {
			status = http.StatusTooManyRequests
		} else if errors.Is(err, context.DeadlineExceeded) {
			status = http.StatusGatewayTimeout
		}
		http.Error(w, err.Error(), status)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(result)
}

func decodeExecute(reader io.Reader) (wire.ExecuteRequest, error) {
	limited := &io.LimitedReader{R: reader, N: 1025}
	decoder := json.NewDecoder(limited)
	var request wire.ExecuteRequest
	if token, err := decoder.Token(); err != nil || token != json.Delim('{') {
		return request, errors.New("execute requires one JSON object")
	}
	seen := make(map[string]bool, 2)
	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return request, err
		}
		key, ok := token.(string)
		if !ok || seen[key] || (key != "hold_ms" && key != "wait_ms") {
			return request, errors.New("unknown or duplicate execute field")
		}
		seen[key] = true
		var value *int64
		if err := decoder.Decode(&value); err != nil || value == nil {
			return request, errors.New("execute fields must be integers")
		}
		if key == "hold_ms" {
			request.HoldMS = *value
		} else {
			request.WaitMS = *value
		}
	}
	if token, err := decoder.Token(); err != nil || token != json.Delim('}') {
		return request, errors.New("invalid execute object")
	}
	if _, err := decoder.Token(); err != io.EOF || limited.N == 0 {
		return request, errors.New("execute must be one JSON object of at most 1 KiB")
	}
	if request.WaitMS < 1 || request.WaitMS > 3000 || request.HoldMS < 0 || request.HoldMS > 500 {
		return request, errors.New("wait_ms must be 1..3000 and hold_ms must be 0..500")
	}
	return request, nil
}

func (s *Service) acquire(ctx context.Context) (Attempt, error) {
	if err := s.ctx.Err(); err != nil {
		return Attempt{}, err
	}
	if err := ctx.Err(); err != nil {
		return Attempt{}, err
	}
	attempt, err := s.engine.Begin()
	if err != nil {
		return attempt, err
	}
	deliveryCtx, stop := context.WithCancel(ctx)
	var deliveries sync.WaitGroup
	for _, message := range attempt.Messages {
		deliveries.Add(1)
		go func() {
			defer deliveries.Done()
			s.deliverRequest(deliveryCtx, message)
		}()
	}
	select {
	case <-attempt.Ready:
	case <-ctx.Done():
	}
	stop()
	deliveries.Wait()
	if err = ctx.Err(); err == nil {
		err = s.ctx.Err()
	}
	if err == nil {
		err = s.engine.Enter(attempt.Request)
	}
	if err != nil {
		replies, cancelErr := s.engine.Cancel(attempt.Request)
		s.dispatch(replies)
		return attempt, errors.Join(err, cancelErr)
	}
	return attempt, nil
}

// Once HELD, all work and both observer calls are bounded independently of the
// client connection. Every path attempts exit before releasing permissions.
func (s *Service) critical(request wire.RequestID, started time.Time, work func() error) (result wire.ExecuteResult, err error) {
	entered := time.Now()
	result = wire.ExecuteResult{Status: "completed", NodeID: s.cluster.NodeID, RunID: s.cluster.RunID, ClusterID: s.cluster.ClusterID, Request: request, WaitNS: entered.Sub(started).Nanoseconds()}
	slog.Info("ra_enter", "run", s.cluster.RunID, "node", s.cluster.NodeID, "request_owner", request.Owner, "request_sequence", request.Sequence)
	defer func() {
		if s.observerURL != "" {
			err = errors.Join(err, s.observe("exit", request))
		}
		replies, releaseErr := s.engine.Release(request)
		s.dispatch(replies)
		err = errors.Join(err, releaseErr)
		result.HeldNS = time.Since(entered).Nanoseconds()
		if err != nil {
			result.Status = ""
		}
		slog.Info("ra_exit", "run", s.cluster.RunID, "node", s.cluster.NodeID, "request_owner", request.Owner, "request_sequence", request.Sequence, "error", err)
	}()
	if s.observerURL != "" {
		if err = s.observe("enter", request); err != nil {
			return result, err
		}
	}
	return result, work()
}

func (s *Service) observe(phase string, request wire.RequestID) error {
	ctx, cancel := context.WithTimeout(context.Background(), observerTimeout)
	defer cancel()
	event := wire.CriticalEvent{Phase: phase, NodeID: s.cluster.NodeID, RunID: s.cluster.RunID, ClusterID: s.cluster.ClusterID, Request: request}
	body, err := json.Marshal(event)
	if err != nil {
		return err
	}
	status, err := s.post(ctx, s.observerURL, body, false)
	if err != nil {
		return fmt.Errorf("observer %s: %w", phase, err)
	}
	if status < 200 || status >= 300 {
		return fmt.Errorf("observer %s: HTTP %d", phase, status)
	}
	return nil
}

func (s *Service) dispatch(messages []wire.Message) {
	for _, message := range messages {
		peer := s.peers[message.To]
		if peer == nil {
			continue
		}
		peer.mu.Lock()
		if message.Request.Sequence > peer.latest.Request.Sequence {
			peer.latest = message
		}
		peer.mu.Unlock()
		select {
		case peer.wake <- struct{}{}:
		default:
		}
	}
}

func (s *Service) deliverReplies(peer *replyOutbox) {
	defer s.workers.Done()
	var encodedID uint64
	var body []byte
	for {
		if s.ctx.Err() != nil {
			return
		}
		peer.mu.Lock()
		message := peer.latest
		pending := message.Request.Sequence > peer.delivered
		peer.mu.Unlock()
		if !pending {
			select {
			case <-peer.wake:
				continue
			case <-s.ctx.Done():
				return
			}
		}
		if message.MessageID != encodedID {
			var err error
			body, err = json.Marshal(message)
			if err != nil {
				return // wire.Message contains only JSON-encodable fields.
			}
			encodedID = message.MessageID
		}
		ctx, cancel := context.WithTimeout(s.ctx, messageTimeout)
		status, err := s.post(ctx, peer.url, body, true)
		cancel()
		if err == nil && status == http.StatusNoContent {
			peer.mu.Lock()
			if message.Request.Sequence > peer.delivered {
				peer.delivered = message.Request.Sequence
			}
			peer.mu.Unlock()
			continue
		}
		if !waitRetry(s.ctx, peer.wake) {
			return
		}
	}
}

func (s *Service) deliverRequest(ctx context.Context, message wire.Message) {
	peer := s.peers[message.To]
	if peer == nil {
		return
	}
	body, err := json.Marshal(message)
	if err != nil {
		return
	}
	for ctx.Err() == nil {
		attemptCtx, cancel := context.WithTimeout(ctx, messageTimeout)
		status, err := s.post(attemptCtx, peer.url, body, true)
		cancel()
		if err == nil && status == http.StatusNoContent {
			return
		}
		if !waitRetry(ctx, nil) {
			return
		}
	}
}

func waitRetry(ctx context.Context, wake <-chan struct{}) bool {
	timer := time.NewTimer(retryDelay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	case <-wake:
		return true
	}
}

func (s *Service) post(ctx context.Context, target string, body []byte, protocol bool) (int, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, target, bytes.NewReader(body))
	if err != nil {
		return 0, err
	}
	request.Header.Set("Content-Type", "application/json")
	if protocol {
		s.attempts.Add(1)
	}
	response, err := s.client.Do(request)
	if err != nil {
		return 0, err
	}
	defer response.Body.Close()
	return response.StatusCode, nil
}
