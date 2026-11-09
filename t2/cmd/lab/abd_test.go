package main

import (
	"bytes"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

func rejectABDResponse(t *testing.T, operation string, status int, body string) error {
	t.Helper()
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Location", "/abd/"+operation)
		w.WriteHeader(status)
		_, _ = io.WriteString(w, body)
	}))
	defer server.Close()
	args := []string{"-action", operation, "-url", server.URL, "-wait-ms", "123"}
	if operation == "write" {
		args = append(args, "-value", "0")
	}
	var output bytes.Buffer
	err := run(args, &output)
	if err == nil || output.Len() != 0 || calls.Load() != 1 {
		t.Fatalf("invalid completion accepted or operation retried: error=%v, output=%q, calls=%d", err, output.String(), calls.Load())
	}
	return err
}

func TestRunABDRejectsInvalidCompletion(t *testing.T) {
	valid := `{"status":"completed","operation":"read","node_id":"n1","run_id":"r1","cluster_id":"` + strings.Repeat("a", 64) + `","request":{"owner":"n1","sequence":1},"register":{"tag":{"counter":1,"writer_id":"n1"},"value":0},"duration_ns":0}`
	for _, tc := range []struct {
		name, old, replacement string
	}{
		{"missing node", `,"node_id":"n1"`, ``},
		{"null node", `"node_id":"n1"`, `"node_id":null`},
		{"missing run", `,"run_id":"r1"`, ``},
		{"invalid cluster", strings.Repeat("a", 64), strings.Repeat("g", 64)},
		{"not completed", `"completed"`, `"entered"`},
		{"wrong operation", `"operation":"read"`, `"operation":"write"`},
		{"wrong owner", `"owner":"n1"`, `"owner":"n2"`},
		{"zero sequence", `"sequence":1`, `"sequence":0`},
		{"missing request", `,"request":{"owner":"n1","sequence":1}`, ``},
		{"null request", `"request":{"owner":"n1","sequence":1}`, `"request":null`},
		{"missing duration", `,"duration_ns":0`, ``},
		{"null duration", `"duration_ns":0`, `"duration_ns":null`},
		{"negative duration", `"duration_ns":0`, `"duration_ns":-1`},
		{"missing register", `,"register":{"tag":{"counter":1,"writer_id":"n1"},"value":0}`, ``},
		{"null register", `{"tag":{"counter":1,"writer_id":"n1"},"value":0}`, `null`},
		{"missing value", `,"value":0`, ``},
		{"null value", `"value":0`, `"value":null`},
		{"missing tag", `"tag":{"counter":1,"writer_id":"n1"},`, ``},
		{"null tag", `"tag":{"counter":1,"writer_id":"n1"}`, `"tag":null`},
		{"missing counter", `"counter":1,`, ``},
		{"null counter", `"counter":1`, `"counter":null`},
		{"missing writer", `,"writer_id":"n1"`, ``},
		{"null writer", `"writer_id":"n1"`, `"writer_id":null`},
		{"empty positive writer", `"writer_id":"n1"`, `"writer_id":" "`},
		{"initial nonempty writer", `"counter":1`, `"counter":0`},
		{"initial nonzero value", `{"counter":1,"writer_id":"n1"},"value":0`, `{"counter":0,"writer_id":""},"value":11`},
		{"value overflow", `"value":0`, `"value":9223372036854775808`},
		{"unknown field", `"duration_ns":0`, `"duration_ns":0,"extra":1`},
		{"nested unknown field", `"sequence":1`, `"sequence":1,"extra":1`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := strings.Replace(valid, tc.old, tc.replacement, 1)
			if body == valid {
				t.Fatal("negative test did not alter the response")
			}
			rejectABDResponse(t, "read", http.StatusOK, body)
		})
	}
	for name, body := range map[string]string{
		"malformed": `{"status":`, "null": `null`, "trailing": valid + `{}`,
		"oversized": valid + strings.Repeat(" ", 4097),
	} {
		t.Run(name, func(t *testing.T) { rejectABDResponse(t, "read", http.StatusOK, body) })
	}
	write := strings.Replace(valid, `"operation":"read"`, `"operation":"write"`, 1)
	for name, body := range map[string]string{
		"wrong write value":  strings.Replace(write, `"value":0`, `"value":11`, 1),
		"wrong write writer": strings.Replace(write, `"writer_id":"n1"`, `"writer_id":"n2"`, 1),
		"initial write":      strings.Replace(write, `{"counter":1,"writer_id":"n1"}`, `{"counter":0,"writer_id":""}`, 1),
	} {
		t.Run(name, func(t *testing.T) { rejectABDResponse(t, "write", http.StatusOK, body) })
	}
}

func TestRunABDErrorOutcomesAndNoRedirects(t *testing.T) {
	for _, tc := range []struct {
		name, operation string
		status          int
		body            string
	}{
		{"not applied", "write", 503, `{"status":"error","error":"disk unavailable","outcome":"not_applied"}`},
		{"indeterminate", "write", 504, `{"status":"error","error":"quorum deadline","outcome":"indeterminate","request":{"owner":"n1","sequence":1}}`},
		{"failed read", "read", 429, `{"status":"error","error":"busy","outcome":"failed"}`},
		{"wrong write outcome", "write", 503, `{"status":"error","error":"disk unavailable","outcome":"failed"}`},
		{"wrong read outcome", "read", 503, `{"status":"error","error":"disk unavailable","outcome":"not_applied"}`},
		{"error on success status", "write", 200, `{"status":"error","error":"disk unavailable","outcome":"not_applied"}`},
		{"wrong error status", "write", 503, `{"status":"completed","error":"disk unavailable","outcome":"not_applied"}`},
		{"missing error", "write", 503, `{"status":"error","outcome":"not_applied"}`},
		{"null outcome", "write", 503, `{"status":"error","error":"disk unavailable","outcome":null}`},
		{"invalid request", "write", 503, `{"status":"error","error":"disk unavailable","outcome":"not_applied","request":{"owner":"n1","sequence":0}}`},
		{"unknown error field", "write", 503, `{"status":"error","error":"disk unavailable","outcome":"not_applied","extra":1}`},
		{"trailing error", "write", 503, `{"status":"error","error":"disk unavailable","outcome":"not_applied"} {}`},
		{"malformed error", "write", 503, `<html>unavailable</html>`},
		{"oversized error", "write", 503, strings.Repeat(" ", 4097)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rejectABDResponse(t, tc.operation, tc.status, tc.body)
		})
	}
	for _, status := range []int{301, 302, 303, 307, 308} {
		rejectABDResponse(t, "write", status, `{"status":"error","error":"moved","outcome":"not_applied"}`)
	}
}

func TestRunABDRejectsInvalidFlags(t *testing.T) {
	for _, args := range [][]string{
		{"-action", "write"}, {"-action", "read", "-value", "0"},
		{"-action", "health", "-value", "1"}, {"-action", "write", "-value", "null"},
		{"-action", "write", "-value", "9223372036854775808"},
		{"-action", "read", "-wait-ms", "0"}, {"-action", "write", "-value", "0", "-wait-ms", "3001"},
	} {
		var output bytes.Buffer
		if err := run(args, &output); err == nil || output.Len() != 0 {
			t.Fatalf("invalid flags %v: error=%v, output=%q", args, err, output.String())
		}
	}
}

func TestRunABDTimeoutLeavesOutcomeUnknown(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		_, _ = io.Copy(io.Discard, r.Body)
		<-r.Context().Done()
	}))
	defer server.Close()
	var output bytes.Buffer
	err := run([]string{"-action", "write", "-value", "11", "-url", server.URL, "-timeout", "50ms"}, &output)
	var timeout interface{ Timeout() bool }
	if !errors.As(err, &timeout) || !timeout.Timeout() ||
		output.Len() != 0 || calls.Load() != 1 {
		t.Fatalf("timeout must not report completion or retry: error=%v, output=%q, calls=%d", err, output.String(), calls.Load())
	}
}
