package main

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"reflect"
	"sync"
	"time"

	"mc714-t2/internal/config"
	"mc714-t2/internal/wire"
)

const maxResponseBytes = 64 << 10

func validID(id string) bool {
	if len(id) == 0 || len(id) > 64 {
		return false
	}
	for _, c := range id {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '_' || c == '-') {
			return false
		}
	}
	return true
}

func validHash(hash string) bool {
	if len(hash) != 64 {
		return false
	}
	_, err := hex.DecodeString(hash)
	return err == nil
}

// Consume ordinary bodies to EOF for connection reuse, but never drain an
// oversized/untrusted response indefinitely. Closing it sacrifices that socket.
func readBody(response *http.Response) ([]byte, error) {
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, maxResponseBytes+1))
	if err != nil {
		return nil, err
	}
	if len(body) > maxResponseBytes {
		return nil, errors.New("response body exceeds 65536 bytes")
	}
	return body, nil
}

func decodeStrict(body []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		return errors.New("response must contain exactly one JSON object")
	}
	return nil
}

func getJSON(client *http.Client, target string) ([]byte, error) {
	response, err := client.Get(target)
	if err != nil {
		return nil, err
	}
	body, err := readBody(response)
	if err != nil {
		return nil, err
	}
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("HTTP %d: %s", response.StatusCode, body)
	}
	return body, nil
}

func discover(client *http.Client, targets []string, runID string) (string, error) {
	clusters := make([]config.Cluster, len(targets))
	failures := make([]error, len(targets))
	var pending sync.WaitGroup
	for i, target := range targets {
		pending.Add(1)
		go func() {
			defer pending.Done()
			body, err := getJSON(client, target+"/cluster")
			if err == nil {
				err = decodeStrict(body, &clusters[i])
			}
			failures[i] = err
		}()
	}
	pending.Wait()
	for i, c := range clusters {
		if failures[i] != nil {
			return "", fmt.Errorf("discover node%d: %w", i+1, failures[i])
		}
		if c.NodeID != fmt.Sprintf("node%d", i+1) || c.RunID != runID || c.Mode != config.Standalone || !validHash(c.ClusterID) || c.Quorum != len(targets)/2+1 || len(c.Members) != len(targets) {
			return "", fmt.Errorf("node%d does not identify the expected standalone cluster", i+1)
		}
		seen := make(map[string]bool, len(targets))
		for _, member := range c.Members {
			if !validID(member.ID) || seen[member.ID] || member.URL == "" {
				return "", fmt.Errorf("node%d has invalid or duplicate membership", i+1)
			}
			seen[member.ID] = true
		}
		for n := range targets {
			if !seen[fmt.Sprintf("node%d", n+1)] {
				return "", fmt.Errorf("node%d membership is missing node%d", i+1, n+1)
			}
		}
		if c.ClusterID != clusters[0].ClusterID || !reflect.DeepEqual(c.Members, clusters[0].Members) {
			return "", fmt.Errorf("node%d disagrees on cluster hash or membership", i+1)
		}
	}
	return clusters[0].ClusterID, nil
}

func httpOutcome(status int) string {
	switch status {
	case http.StatusTooManyRequests:
		return "busy"
	case http.StatusRequestTimeout, http.StatusGatewayTimeout:
		return "deadline"
	case http.StatusBadGateway, http.StatusServiceUnavailable:
		return "unavailable"
	default:
		return "error"
	}
}

func (w *workload) perform(j job) {
	row := j.row
	body := w.requestBody
	if row.Operation == "write" {
		var err error
		body, err = json.Marshal(wire.ABDRequest{TimeoutMS: int64(w.o.waitMS), Value: row.Value})
		if err != nil {
			row.Outcome, row.Error = "error", err.Error()
			row.EndNS = time.Since(j.origin).Nanoseconds()
			w.records.emit(row)
			return
		}
	}
	path := "/abd/" + row.Operation
	if row.Operation == "execute" {
		path = "/ra/execute"
	}
	request, err := http.NewRequest(http.MethodPost, w.targets[j.target]+path, bytes.NewReader(body))
	if err != nil {
		row.Outcome, row.Error = "error", err.Error()
		row.EndNS = time.Since(j.origin).Nanoseconds()
		w.records.emit(row)
		return
	}
	request.Header.Set("Content-Type", "application/json")
	// A replayable Body lets Transport retry stale keepalive failures. Disable it:
	// one offered operation is at most one outbound request, even before first byte.
	request.GetBody = nil
	started := time.Now()
	startNS := started.Sub(j.origin).Nanoseconds()
	row.StartNS = &startNS
	if row.Operation == "write" {
		row.WriteOutcome = "indeterminate"
	}
	response, err := w.client.Do(request)
	if err != nil {
		row.Outcome = "transport_error"
		var netErr net.Error
		if errors.Is(err, context.DeadlineExceeded) || errors.As(err, &netErr) && netErr.Timeout() {
			row.Outcome = "deadline"
		}
		row.Error = err.Error()
	} else {
		row.HTTPStatus = response.StatusCode
		body, err = readBody(response)
		if response.StatusCode != http.StatusOK {
			row.Outcome = httpOutcome(response.StatusCode)
			row.Error = fmt.Sprintf("HTTP %d: %s", response.StatusCode, body)
			if err != nil {
				row.Error = fmt.Sprintf("HTTP %d: read response: %v", response.StatusCode, err)
			} else if row.Operation != "execute" {
				w.failureABD(&row, body)
			}
		} else if err != nil {
			row.Outcome, row.Error = "invalid_response", err.Error()
		} else {
			row.EndNS = time.Since(j.origin).Nanoseconds()
			if err := w.completion(&row, body); err != nil {
				row.Outcome, row.Error = "invalid_response", err.Error()
			} else {
				row.Outcome, row.WriteOutcome = "completed", ""
			}
		}
	}
	if row.EndNS == 0 {
		row.EndNS = time.Since(j.origin).Nanoseconds()
	}
	w.records.emit(row)
}

func (w *workload) failureABD(row *operation, body []byte) {
	var failure wire.ABDError
	err := decodeStrict(body, &failure)
	if err == nil && (failure.Status != "error" || failure.Error == "" || row.HTTPStatus < 400 || row.HTTPStatus > 599) {
		err = errors.New("invalid error status")
	}
	if err == nil && failure.Request != nil && (failure.Request.Owner != row.NodeID || failure.Request.Sequence == 0) {
		err = errors.New("invalid error request identity")
	}
	if err == nil {
		if row.Operation == "write" && (failure.Outcome == "not_applied" || failure.Outcome == "indeterminate") {
			row.WriteOutcome = failure.Outcome
		} else if row.Operation != "read" || failure.Outcome != "failed" {
			err = errors.New("invalid error outcome")
		}
	}
	if err != nil {
		row.Error += "; malformed ABD error: " + err.Error()
	}
}

func (w *workload) completion(row *operation, body []byte) error {
	if row.StartNS == nil || row.EndNS < *row.StartNS {
		return errors.New("invalid local request timing")
	}
	elapsed := row.EndNS - *row.StartNS
	if row.Operation == "execute" {
		var result struct {
			wire.ExecuteResult
			WaitNS *int64 `json:"wait_ns"`
			HeldNS *int64 `json:"held_ns"`
		}
		if err := decodeStrict(body, &result); err != nil {
			return err
		}
		if result.Status != "completed" || result.NodeID != row.NodeID || result.RunID != w.o.runID || result.ClusterID != w.cluster || result.Request.Owner != row.NodeID || result.Request.Sequence == 0 {
			return errors.New("RA completion identity or request mismatch")
		}
		if result.WaitNS == nil || result.HeldNS == nil || *result.WaitNS < 0 || *result.HeldNS < 0 || *result.HeldNS > elapsed || *result.WaitNS > elapsed-*result.HeldNS {
			return errors.New("RA completion requires nonnegative wait_ns and held_ns within client elapsed time")
		}
		row.WaitNS, row.HeldNS = result.WaitNS, result.HeldNS
		return nil
	}
	var result struct {
		wire.ABDResult
		DurationNS *int64 `json:"duration_ns"`
		Register   *struct {
			Tag *struct {
				Counter  *uint64 `json:"counter"`
				WriterID *string `json:"writer_id"`
			} `json:"tag"`
			Value *int64 `json:"value"`
		} `json:"register"`
	}
	if err := decodeStrict(body, &result); err != nil {
		return err
	}
	if result.Status != "completed" || result.Operation != row.Operation || result.NodeID != row.NodeID || result.RunID != w.o.runID || result.ClusterID != w.cluster || result.Request.Owner != row.NodeID || result.Request.Sequence == 0 {
		return errors.New("ABD completion identity, operation or request mismatch")
	}
	if result.DurationNS == nil || *result.DurationNS < 0 || *result.DurationNS > elapsed {
		return errors.New("ABD completion requires nonnegative duration_ns within client elapsed time")
	}
	register := result.Register
	if register == nil || register.Tag == nil || register.Tag.Counter == nil || register.Tag.WriterID == nil || register.Value == nil {
		return errors.New("ABD completion requires a full register")
	}
	counter, writer, value := *register.Tag.Counter, *register.Tag.WriterID, *register.Value
	if counter == 0 {
		if writer != "" || value != 0 {
			return errors.New("invalid ABD initial register")
		}
	} else {
		member := false
		for i := range w.targets {
			if writer == fmt.Sprintf("node%d", i+1) {
				member = true
				break
			}
		}
		if !member {
			return errors.New("ABD register writer is not a member")
		}
	}
	if row.Operation == "write" && (row.Value == nil || value != *row.Value || counter == 0 || writer != row.NodeID) {
		return errors.New("ABD completion does not confirm the attempted write")
	}
	row.DurationNS = result.DurationNS
	if row.Operation == "read" {
		row.Value = register.Value
	}
	return nil
}

type snapshotNode struct {
	NodeID string          `json:"node_id"`
	State  json.RawMessage `json:"state,omitempty"`
	Error  string          `json:"error,omitempty"`
}

func (w *workload) snapshots(stage string) error {
	nodes := make([]snapshotNode, len(w.targets))
	var pending sync.WaitGroup
	for i, target := range w.targets {
		pending.Add(1)
		go func() {
			defer pending.Done()
			nodes[i].NodeID = fmt.Sprintf("node%d", i+1)
			body, err := getJSON(w.client, target+"/"+w.o.algorithm+"/state")
			if err == nil {
				err = w.validateState(nodes[i].NodeID, body)
			}
			if err != nil {
				nodes[i].Error = err.Error()
			} else {
				nodes[i].State = json.RawMessage(body)
			}
		}()
	}
	pending.Wait()
	w.records.emit(map[string]any{"type": "snapshot", "stage": stage, "nodes": nodes})
	if err := w.records.failure(); err != nil {
		return err
	}
	if stage == "baseline" {
		for _, node := range nodes {
			if node.Error != "" {
				return fmt.Errorf("baseline snapshot %s: %s", node.NodeID, node.Error)
			}
		}
	}
	return nil // Missing final nodes after injected faults are evidence, not fatal errors.
}

func (w *workload) validateState(node string, body []byte) error {
	var state struct {
		NodeID            string  `json:"node_id"`
		RunID             string  `json:"run_id"`
		ClusterID         string  `json:"cluster_id"`
		LogicalMessages   *uint64 `json:"logical_messages"`
		TransportAttempts *uint64 `json:"transport_attempts"`
	}
	if err := json.Unmarshal(body, &state); err != nil {
		return err
	}
	if state.LogicalMessages == nil || state.TransportAttempts == nil {
		return errors.New("snapshot requires logical_messages and transport_attempts")
	}
	// RA state has no identity fields. Its wrapper uses the discovered endpoint.
	if w.o.algorithm == "abd" && (state.NodeID != node || state.RunID != w.o.runID || state.ClusterID != w.cluster) {
		return errors.New("ABD snapshot identity mismatch")
	}
	return nil
}
