// Package fd implements a heartbeat failure detector (bonus algorithm).
package fd

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"
	"time"

	"mc714-t2/internal/config"
	"mc714-t2/internal/faults"
)

const (
	defaultInterval = 200 * time.Millisecond
	defaultTimeout  = 800 * time.Millisecond
	maxBody         = 2 << 10
)

// Service periodically heartbeats peers and tracks suspicion.
type Service struct {
	cluster  config.Cluster
	interval time.Duration
	timeout  time.Duration
	client   *http.Client
	ctx      context.Context
	cancel   context.CancelFunc

	mu       sync.Mutex
	lastSeen map[string]time.Time
	seq      uint64
}

type heartbeat struct {
	From      string `json:"from"`
	RunID     string `json:"run_id"`
	ClusterID string `json:"cluster_id"`
	Sequence  uint64 `json:"sequence"`
	SentUnixN int64  `json:"sent_unix_ns"`
}

// PeerStatus is exported for GET /fd/state.
type PeerStatus struct {
	ID        string `json:"id"`
	LastSeenN int64  `json:"last_seen_unix_ns"`
	Suspected bool   `json:"suspected"`
	AgeMS     int64  `json:"age_ms"`
}

// State is the detector snapshot.
type State struct {
	NodeID    string       `json:"node_id"`
	RunID     string       `json:"run_id"`
	ClusterID string       `json:"cluster_id"`
	IntervalMS int64       `json:"interval_ms"`
	TimeoutMS  int64       `json:"timeout_ms"`
	Peers     []PeerStatus `json:"peers"`
}

// NewService starts background heartbeats. Faults wrap outbound transport.
func NewService(cluster config.Cluster, faultCfg faults.Config) (*Service, error) {
	if len(cluster.Members) < 2 {
		return nil, errors.New("failure detector requires at least two members")
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.DialContext = (&net.Dialer{Timeout: 500 * time.Millisecond, KeepAlive: 30 * time.Second}).DialContext
	transport.ResponseHeaderTimeout = 500 * time.Millisecond
	transport.MaxIdleConnsPerHost = 2
	ctx, cancel := context.WithCancel(context.Background())
	s := &Service{
		cluster:  cluster,
		interval: defaultInterval,
		timeout:  defaultTimeout,
		client: &http.Client{
			Transport: faults.Wrap(transport, faultCfg),
			Timeout:   500 * time.Millisecond,
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
		ctx:      ctx,
		cancel:   cancel,
		lastSeen: make(map[string]time.Time, len(cluster.Members)),
	}
	now := time.Now()
	for _, member := range cluster.Members {
		if member.ID != cluster.NodeID {
			s.lastSeen[member.ID] = now
		}
	}
	go s.loop()
	return s, nil
}

// Register attaches HTTP handlers.
func (s *Service) Register(mux *http.ServeMux) {
	mux.HandleFunc("POST /fd/heartbeat", s.handleHeartbeat)
	mux.HandleFunc("GET /fd/state", s.handleState)
}

// Close stops heartbeats.
func (s *Service) Close() {
	s.cancel()
	s.client.CloseIdleConnections()
}

func (s *Service) loop() {
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			s.broadcast()
		}
	}
}

func (s *Service) broadcast() {
	s.mu.Lock()
	s.seq++
	seq := s.seq
	s.mu.Unlock()
	body, err := json.Marshal(heartbeat{
		From: s.cluster.NodeID, RunID: s.cluster.RunID, ClusterID: s.cluster.ClusterID,
		Sequence: seq, SentUnixN: time.Now().UnixNano(),
	})
	if err != nil {
		return
	}
	for _, member := range s.cluster.Members {
		if member.ID == s.cluster.NodeID {
			continue
		}
		member := member
		go func() {
			ctx, cancel := context.WithTimeout(s.ctx, 500*time.Millisecond)
			defer cancel()
			req, err := http.NewRequestWithContext(ctx, http.MethodPost, member.URL+"/fd/heartbeat", bytes.NewReader(body))
			if err != nil {
				return
			}
			req.Header.Set("Content-Type", "application/json")
			resp, err := s.client.Do(req)
			if err != nil {
				return
			}
			io.Copy(io.Discard, io.LimitReader(resp.Body, 512))
			resp.Body.Close()
		}()
	}
}

func (s *Service) handleHeartbeat(w http.ResponseWriter, r *http.Request) {
	limited := io.LimitReader(r.Body, maxBody+1)
	data, err := io.ReadAll(limited)
	if err != nil || len(data) > maxBody {
		http.Error(w, "invalid heartbeat", http.StatusBadRequest)
		return
	}
	var msg heartbeat
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&msg); err != nil {
		http.Error(w, "invalid heartbeat", http.StatusBadRequest)
		return
	}
	if msg.RunID != s.cluster.RunID || msg.ClusterID != s.cluster.ClusterID || !s.cluster.HasMember(msg.From) || msg.From == s.cluster.NodeID || msg.Sequence == 0 {
		http.Error(w, "heartbeat context mismatch", http.StatusConflict)
		return
	}
	s.mu.Lock()
	s.lastSeen[msg.From] = time.Now()
	s.mu.Unlock()
	w.WriteHeader(http.StatusNoContent)
}

func (s *Service) handleState(w http.ResponseWriter, _ *http.Request) {
	state := s.Snapshot()
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(state)
}

// Snapshot returns the current suspicion view.
func (s *Service) Snapshot() State {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	peers := make([]PeerStatus, 0, len(s.lastSeen))
	for _, member := range s.cluster.Members {
		if member.ID == s.cluster.NodeID {
			continue
		}
		seen := s.lastSeen[member.ID]
		age := now.Sub(seen)
		peers = append(peers, PeerStatus{
			ID: member.ID, LastSeenN: seen.UnixNano(),
			Suspected: age > s.timeout, AgeMS: age.Milliseconds(),
		})
	}
	return State{
		NodeID: s.cluster.NodeID, RunID: s.cluster.RunID, ClusterID: s.cluster.ClusterID,
		IntervalMS: s.interval.Milliseconds(), TimeoutMS: s.timeout.Milliseconds(), Peers: peers,
	}
}

// Suspected reports whether peer is currently suspected.
func (s *Service) Suspected(peer string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	seen, ok := s.lastSeen[peer]
	if !ok {
		return true
	}
	return time.Since(seen) > s.timeout
}

// String helps debugging.
func (s *Service) String() string {
	return fmt.Sprintf("fd(node=%s timeout=%s)", s.cluster.NodeID, s.timeout)
}
