package main

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"

	"mc714-t2/internal/wire"
)

var errActiveConflict = errors.New("node already has another active request; evidence is incomplete")

type observerReport struct {
	RunID         string `json:"run_id"`
	ClusterID     string `json:"cluster_id"`
	Entries       uint64 `json:"entries"`
	Exits         uint64 `json:"exits"`
	Active        int    `json:"active"`
	Violations    uint64 `json:"violations"`
	InvalidEvents uint64 `json:"invalid_events"`
}

type observedNode struct {
	active    uint64
	completed uint64
}

// observer records evidence only: an overlap never delays or denies entry.
// Retaining just the active and latest completed IDs bounds memory to seven nodes.
type observer struct {
	mu     sync.Mutex
	output *json.Encoder
	nodes  map[string]observedNode
	report observerReport
}

func newObserver(output io.Writer) *observer {
	return &observer{output: json.NewEncoder(output), nodes: make(map[string]observedNode)}
}

func (o *observer) Register(mux *http.ServeMux) {
	mux.HandleFunc("POST /events", o.event)
	mux.HandleFunc("GET /report", o.getReport)
}

func validIdentity(id string) bool {
	return strings.TrimSpace(id) != "" && len(id) <= 64
}

func validClusterID(id string) bool {
	if len(id) != 64 {
		return false
	}
	_, err := hex.DecodeString(id)
	return err == nil
}

func (o *observer) event(w http.ResponseWriter, r *http.Request) {
	var event wire.CriticalEvent
	r.Body = http.MaxBytesReader(w, r.Body, 4096)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	err := decoder.Decode(&event)
	if err == nil {
		if tail := decoder.Decode(new(any)); !errors.Is(tail, io.EOF) {
			err = errors.New("event must contain one JSON object")
		}
	}
	if err == nil && (event.Phase != "enter" && event.Phase != "exit" ||
		!validIdentity(event.NodeID) || !validIdentity(event.RunID) || !validClusterID(event.ClusterID) ||
		event.Request.Owner != event.NodeID || event.Request.Sequence == 0) {
		err = errors.New("invalid event phase, identity, or request")
	}

	o.mu.Lock()
	status := http.StatusBadRequest
	if err == nil {
		status = http.StatusConflict
		err = o.record(event)
		if err == errActiveConflict {
			status = http.StatusOK
		}
	}
	if err != nil {
		o.report.InvalidEvents++
	}
	entry := struct {
		wire.CriticalEvent
		Accepted bool   `json:"accepted"`
		Error    string `json:"error,omitempty"`
	}{CriticalEvent: event, Accepted: err == nil}
	if err != nil {
		entry.Error = err.Error()
	}
	if logErr := o.output.Encode(entry); logErr != nil {
		if err == nil {
			o.report.InvalidEvents++
		}
		o.mu.Unlock()
		http.Error(w, "observer evidence output failed", http.StatusServiceUnavailable)
		return
	}
	o.mu.Unlock()
	if err != nil && status != http.StatusOK {
		http.Error(w, err.Error(), status)
		return
	}
	w.WriteHeader(http.StatusOK)
}

// record is called with mu held. It never waits for another critical section.
func (o *observer) record(event wire.CriticalEvent) error {
	if o.report.RunID != "" && (event.RunID != o.report.RunID || event.ClusterID != o.report.ClusterID) {
		return errors.New("event run or cluster does not match recorder")
	}
	node, exists := o.nodes[event.NodeID]
	sequence := event.Request.Sequence
	if event.Phase == "exit" {
		if !exists {
			return errors.New("exit has no recorded entry")
		}
		if sequence == node.completed {
			return nil
		}
		if sequence != node.active {
			return errors.New("exit has no matching active entry")
		}
		node.active = 0
		node.completed = sequence
		o.nodes[event.NodeID] = node
		o.report.Exits++
		o.report.Active--
		return nil
	}
	if sequence == node.active || sequence == node.completed {
		return nil
	}
	if sequence <= node.completed {
		return errors.New("entry is stale")
	}
	if node.active != 0 {
		// ponytail: one active slot per node; conflicting activity invalidates proof instead of growing history.
		o.report.Violations++
		return errActiveConflict
	}
	if !exists && len(o.nodes) == 7 {
		return errors.New("observer supports at most seven nodes")
	}
	if o.report.Active != 0 {
		o.report.Violations++
	}
	if o.report.RunID == "" {
		o.report.RunID, o.report.ClusterID = event.RunID, event.ClusterID
	}
	node.active = sequence
	o.nodes[event.NodeID] = node
	o.report.Entries++
	o.report.Active++
	return nil
}

func (o *observer) getReport(w http.ResponseWriter, _ *http.Request) {
	o.mu.Lock()
	report := o.report
	o.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(report)
}
