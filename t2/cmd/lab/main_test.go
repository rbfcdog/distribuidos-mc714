package main

import (
	"bytes"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestProbeRejectsUnhealthyResponses(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status int
		body   string
	}{
		{"unavailable", http.StatusServiceUnavailable, `{"status":"ok","node_id":"n1","run_id":"r1"}`},
		{"malformed", http.StatusOK, `{"status":`},
		{"missing identity", http.StatusOK, `{"status":"ok"}`},
		{"oversized", http.StatusOK, strings.Repeat(" ", 4097)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(tc.body))
			}))
			defer server.Close()
			if _, err := probe(&http.Client{Timeout: time.Second}, server.URL); err == nil {
				t.Fatal("probe reported an unhealthy response as healthy")
			}
		})
	}
}

func TestProbeTimesOut(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	defer server.Close()
	_, err := probe(&http.Client{Timeout: 50 * time.Millisecond}, server.URL)
	var timeout interface{ Timeout() bool }
	if !errors.As(err, &timeout) || !timeout.Timeout() {
		t.Fatalf("expected timeout, got %v", err)
	}
}

func TestRunRARejectsErrorsAndInvalidCompletion(t *testing.T) {
	valid := `{"status":"completed","node_id":"n1","run_id":"r1","cluster_id":"` + strings.Repeat("a", 64) + `","request":{"owner":"n1","sequence":1},"wait_ns":0,"held_ns":0}`
	for _, tc := range []struct {
		name   string
		status int
		body   string
	}{
		{"server error", http.StatusServiceUnavailable, valid},
		{"timeout", http.StatusGatewayTimeout, valid},
		{"redirect", http.StatusTemporaryRedirect, valid},
		{"permanent redirect", http.StatusPermanentRedirect, valid},
		{"malformed", http.StatusOK, `{"status":`},
		{"missing identity", http.StatusOK, `{"status":"completed"}`},
		{"not completed", http.StatusOK, strings.Replace(valid, `"completed"`, `"entered"`, 1)},
		{"wrong owner", http.StatusOK, strings.Replace(valid, `"owner":"n1"`, `"owner":"n2"`, 1)},
		{"zero sequence", http.StatusOK, strings.Replace(valid, `"sequence":1`, `"sequence":0`, 1)},
		{"negative wait", http.StatusOK, strings.Replace(valid, `"wait_ns":0`, `"wait_ns":-1`, 1)},
		{"negative hold", http.StatusOK, strings.Replace(valid, `"held_ns":0`, `"held_ns":-1`, 1)},
		{"missing wait", http.StatusOK, strings.Replace(valid, `,"wait_ns":0`, ``, 1)},
		{"missing hold", http.StatusOK, strings.Replace(valid, `,"held_ns":0`, ``, 1)},
		{"null hold", http.StatusOK, strings.Replace(valid, `"held_ns":0`, `"held_ns":null`, 1)},
		{"invalid cluster", http.StatusOK, strings.Replace(valid, strings.Repeat("a", 64), strings.Repeat("g", 64), 1)},
		{"multiple values", http.StatusOK, valid + ` {}`},
		{"unknown field", http.StatusOK, strings.TrimSuffix(valid, "}") + `,"extra":1}`},
		{"oversized", http.StatusOK, valid + strings.Repeat(" ", 4097)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				calls.Add(1)
				w.Header().Set("Location", "/ra/execute")
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(tc.body))
			}))
			defer server.Close()
			var output bytes.Buffer
			err := run([]string{"-action", "ra", "-url", server.URL}, &output)
			if err == nil || output.Len() != 0 || calls.Load() != 1 {
				t.Fatalf("invalid completion reported or request retried: error=%v, output=%q, calls=%d", err, output.String(), calls.Load())
			}
		})
	}
}

func TestRunRejectsInvalidFlags(t *testing.T) {
	for _, args := range [][]string{
		{"-action", "unknown"}, {"-timeout", "0s"}, {"-timeout", "-1s"},
		{"-action", "ra", "-hold-ms", "-1"}, {"-action", "ra", "-hold-ms", "501"},
		{"-action", "ra", "-wait-ms", "0"}, {"-action", "ra", "-wait-ms", "3001"},
	} {
		var output bytes.Buffer
		if err := run(args, &output); err == nil || output.Len() != 0 {
			t.Fatalf("invalid flags %v: error=%v, output=%q", args, err, output.String())
		}
	}
}
