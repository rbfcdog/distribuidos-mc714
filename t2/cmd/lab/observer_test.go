package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"mc714-t2/internal/wire"
)

func eventFor(node, phase string, sequence uint64) wire.CriticalEvent {
	return wire.CriticalEvent{Phase: phase, NodeID: node, RunID: "run1", ClusterID: strings.Repeat("a", 64),
		Request: wire.RequestID{Owner: node, Sequence: sequence}}
}

func observerHandler(output io.Writer) http.Handler {
	mux := http.NewServeMux()
	newObserver(output).Register(mux)
	return mux
}

func sendEvent(t *testing.T, handler http.Handler, event wire.CriticalEvent, status int) {
	t.Helper()
	body, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/events", bytes.NewReader(body)))
	if response.Code != status {
		t.Fatalf("event %+v: HTTP %d, want %d: %s", event, response.Code, status, response.Body)
	}
}

func reportFrom(t *testing.T, handler http.Handler) observerReport {
	t.Helper()
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/report", nil))
	var report observerReport
	if response.Code != http.StatusOK {
		t.Fatalf("report: HTTP %d", response.Code)
	}
	if err := json.Unmarshal(response.Body.Bytes(), &report); err != nil {
		t.Fatal(err)
	}
	return report
}

func TestObserverRecordsOverlappingEntriesWithoutGrantingPermission(t *testing.T) {
	var output bytes.Buffer
	handler := observerHandler(&output)
	server := httptest.NewServer(handler)
	defer server.Close()
	client := &http.Client{Timeout: time.Second}
	defer client.CloseIdleConnections()
	start := make(chan struct{})
	results := make(chan error, 2)
	for _, node := range []string{"n1", "n2"} {
		go func(node string) {
			<-start
			body, err := json.Marshal(eventFor(node, "enter", 1))
			if err != nil {
				results <- err
				return
			}
			response, err := client.Post(server.URL+"/events", "application/json", bytes.NewReader(body))
			if err == nil {
				response.Body.Close()
				if response.StatusCode != http.StatusOK {
					err = fmt.Errorf("overlap denied: HTTP %d", response.StatusCode)
				}
			}
			results <- err
		}(node)
	}
	close(start)
	for range 2 {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
	if report := reportFrom(t, handler); report.Entries != 2 || report.Active != 2 || report.Violations != 1 || report.InvalidEvents != 0 {
		t.Fatalf("overlap not preserved as evidence: %+v", report)
	}
	for _, node := range []string{"n1", "n2"} {
		sendEvent(t, handler, eventFor(node, "exit", 1), http.StatusOK)
	}
	if report := reportFrom(t, handler); report.Exits != 2 || report.Active != 0 || report.Violations != 1 {
		t.Fatalf("overlap evidence disappeared on exit: %+v", report)
	}
	decoder := json.NewDecoder(&output)
	for range 4 {
		var logged struct {
			wire.CriticalEvent
			Accepted bool `json:"accepted"`
		}
		if err := decoder.Decode(&logged); err != nil || !logged.Accepted || logged.Request.Sequence != 1 {
			t.Fatalf("missing synchronized JSON event: %+v, %v", logged, err)
		}
	}
}

func TestObserverDuplicatesAndStaleExitPreserveLaterActive(t *testing.T) {
	handler := observerHandler(io.Discard)
	for _, phase := range []string{"enter", "enter", "exit", "exit", "enter"} {
		sendEvent(t, handler, eventFor("n1", phase, 1), http.StatusOK)
	}
	sendEvent(t, handler, eventFor("n1", "enter", 3), http.StatusOK)
	// Replayed completed exit is idempotent; an unknown older exit is invalid.
	sendEvent(t, handler, eventFor("n1", "exit", 1), http.StatusOK)
	sendEvent(t, handler, eventFor("n1", "exit", 2), http.StatusConflict)
	if report := reportFrom(t, handler); report.Entries != 2 || report.Exits != 1 || report.Active != 1 || report.InvalidEvents != 1 || report.Violations != 0 {
		t.Fatalf("duplicate/stale event changed evidence: %+v", report)
	}
	sendEvent(t, handler, eventFor("n1", "exit", 3), http.StatusOK)
	if report := reportFrom(t, handler); report.Exits != 2 || report.Active != 0 {
		t.Fatalf("later active request was lost: %+v", report)
	}
}

func TestObserverRejectsMismatchAndMalformedEvidence(t *testing.T) {
	handler := observerHandler(io.Discard)
	sendEvent(t, handler, eventFor("n1", "exit", 1), http.StatusConflict)
	if report := reportFrom(t, handler); report.RunID != "" || report.ClusterID != "" {
		t.Fatalf("invalid first event pinned recorder: %+v", report)
	}
	sendEvent(t, handler, eventFor("n1", "enter", 1), http.StatusOK)
	for _, change := range []func(*wire.CriticalEvent){
		func(e *wire.CriticalEvent) { e.RunID = "other" },
		func(e *wire.CriticalEvent) { e.ClusterID = strings.Repeat("b", 64) },
	} {
		event := eventFor("n1", "exit", 1)
		change(&event)
		sendEvent(t, handler, event, http.StatusConflict)
	}
	body, err := json.Marshal(eventFor("n1", "exit", 1))
	if err != nil {
		t.Fatal(err)
	}
	malformed := []string{`{`, `null`, string(body) + ` {}`, strings.TrimSuffix(string(body), "}") + `,"extra":1}`, strings.Repeat(" ", 4097)}
	for _, change := range []func(*wire.CriticalEvent){
		func(e *wire.CriticalEvent) { e.Phase = "grant" },
		func(e *wire.CriticalEvent) { e.Request.Owner = "n2" },
		func(e *wire.CriticalEvent) { e.Request.Sequence = 0 },
		func(e *wire.CriticalEvent) { e.RunID = strings.Repeat("x", 65) },
		func(e *wire.CriticalEvent) { e.NodeID = strings.Repeat("x", 65); e.Request.Owner = e.NodeID },
		func(e *wire.CriticalEvent) { e.ClusterID = strings.Repeat("z", 64) },
	} {
		event := eventFor("n1", "exit", 1)
		change(&event)
		body, err := json.Marshal(event)
		if err != nil {
			t.Fatal(err)
		}
		malformed = append(malformed, string(body))
	}
	for _, body := range malformed {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/events", strings.NewReader(body)))
		if response.Code != http.StatusBadRequest {
			t.Fatalf("accepted malformed event %q: HTTP %d", body, response.Code)
		}
	}
	if report := reportFrom(t, handler); report.InvalidEvents != uint64(3+len(malformed)) || report.Entries != 1 || report.Exits != 0 || report.Active != 1 || report.Violations != 0 {
		t.Fatalf("invalid evidence was hidden or changed active state: %+v", report)
	}
}

type failingEvidenceWriter struct{}

func (failingEvidenceWriter) Write([]byte) (int, error) { return 0, errors.New("disk full") }

func TestObserverOutputFailureInvalidatesEvidence(t *testing.T) {
	handler := observerHandler(failingEvidenceWriter{})
	sendEvent(t, handler, eventFor("n1", "enter", 1), http.StatusServiceUnavailable)
	if report := reportFrom(t, handler); report.InvalidEvents != 1 || report.Entries != 1 || report.Active != 1 {
		t.Fatalf("output failure did not invalidate evidence: %+v", report)
	}
}

func TestObserverBoundsNodeMemory(t *testing.T) {
	handler := observerHandler(io.Discard)
	for i := 1; i <= 7; i++ {
		node := fmt.Sprintf("n%d", i)
		sendEvent(t, handler, eventFor(node, "enter", 1), http.StatusOK)
		sendEvent(t, handler, eventFor(node, "exit", 1), http.StatusOK)
	}
	sendEvent(t, handler, eventFor("n8", "enter", 1), http.StatusConflict)
	if report := reportFrom(t, handler); report.Entries != 7 || report.Exits != 7 || report.Active != 0 || report.InvalidEvents != 1 {
		t.Fatalf("node bound failed: %+v", report)
	}
}

func TestObserverAcknowledgesSameNodeOverlapWithoutReplacingActive(t *testing.T) {
	var output bytes.Buffer
	handler := observerHandler(&output)
	sendEvent(t, handler, eventFor("n1", "enter", 1), http.StatusOK)
	sendEvent(t, handler, eventFor("n1", "enter", 2), http.StatusOK)
	if report := reportFrom(t, handler); report.Active != 1 || report.Entries != 1 || report.Violations != 1 || report.InvalidEvents != 1 {
		t.Fatalf("same-node conflict was hidden: %+v", report)
	}
	sendEvent(t, handler, eventFor("n1", "exit", 2), http.StatusConflict)
	if report := reportFrom(t, handler); report.Active != 1 || report.Exits != 0 || report.InvalidEvents != 2 {
		t.Fatalf("conflicting exit cleared original active request: %+v", report)
	}
	sendEvent(t, handler, eventFor("n1", "exit", 1), http.StatusOK)
	if report := reportFrom(t, handler); report.Active != 0 || report.Exits != 1 || report.Violations != 1 {
		t.Fatalf("original active request was lost: %+v", report)
	}
	decoder := json.NewDecoder(&output)
	var logged struct {
		Accepted bool   `json:"accepted"`
		Error    string `json:"error"`
	}
	if err := decoder.Decode(&logged); err != nil {
		t.Fatal(err)
	}
	if err := decoder.Decode(&logged); err != nil || logged.Accepted || logged.Error == "" {
		t.Fatalf("conflicting activity missing from JSON evidence: %+v, %v", logged, err)
	}
}
