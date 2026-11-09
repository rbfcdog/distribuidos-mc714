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

func rejectIncrementResponse(t *testing.T, status int, body string) {
	t.Helper()
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Location", "/increment")
		w.WriteHeader(status)
		_, _ = io.WriteString(w, body)
	}))
	defer server.Close()
	var output bytes.Buffer
	err := run([]string{"-action", "increment", "-url", server.URL, "-wait-ms", "123"}, &output)
	if err == nil || output.Len() != 0 || calls.Load() != 1 {
		t.Fatalf("invalid increment accepted or reissued: error=%v, output=%q, calls=%d", err, output.String(), calls.Load())
	}
}

func TestRunIncrementRejectsInvalidCompletion(t *testing.T) {
	valid := `{"status":"completed","node_id":"n1","run_id":"r1","cluster_id":"` + strings.Repeat("a", 64) + `","request":{"owner":"n1","sequence":1},"before":0,"register":{"tag":{"counter":1,"writer_id":"n1"},"value":1},"duration_ns":0}`
	for _, tc := range []struct {
		name, old, replacement string
	}{
		{"not completed", `"completed"`, `"entered"`},
		{"missing node", `,"node_id":"n1"`, ``},
		{"null node", `"node_id":"n1"`, `"node_id":null`},
		{"missing run", `,"run_id":"r1"`, ``},
		{"invalid cluster", strings.Repeat("a", 64), strings.Repeat("g", 64)},
		{"missing request", `,"request":{"owner":"n1","sequence":1}`, ``},
		{"null request", `"request":{"owner":"n1","sequence":1}`, `"request":null`},
		{"wrong owner", `"owner":"n1"`, `"owner":"n2"`},
		{"zero sequence", `"sequence":1`, `"sequence":0`},
		{"missing before", `,"before":0`, ``},
		{"null before", `"before":0`, `"before":null`},
		{"fractional before", `"before":0`, `"before":0.5`},
		{"before overflow", `"before":0`, `"before":9223372036854775808`},
		{"before underflow", `"before":0`, `"before":-9223372036854775809`},
		{"missing duration", `,"duration_ns":0`, ``},
		{"null duration", `"duration_ns":0`, `"duration_ns":null`},
		{"negative duration", `"duration_ns":0`, `"duration_ns":-1`},
		{"missing register", `,"register":{"tag":{"counter":1,"writer_id":"n1"},"value":1}`, ``},
		{"null register", `{"tag":{"counter":1,"writer_id":"n1"},"value":1}`, `null`},
		{"missing after value", `,"value":1`, ``},
		{"null after value", `"value":1`, `"value":null`},
		{"after overflow", `"value":1`, `"value":9223372036854775808`},
		{"unchanged value", `"value":1`, `"value":0`},
		{"wrong delta", `"value":1`, `"value":2`},
		{"missing tag", `"tag":{"counter":1,"writer_id":"n1"},`, ``},
		{"null tag", `"tag":{"counter":1,"writer_id":"n1"}`, `"tag":null`},
		{"missing counter", `"counter":1,`, ``},
		{"null counter", `"counter":1`, `"counter":null`},
		{"initial tag", `"counter":1`, `"counter":0`},
		{"missing writer", `,"writer_id":"n1"`, ``},
		{"null writer", `"writer_id":"n1"`, `"writer_id":null`},
		{"wrong writer", `"writer_id":"n1"`, `"writer_id":"n2"`},
		{"unknown field", `"duration_ns":0`, `"duration_ns":0,"extra":1`},
		{"unknown request field", `"sequence":1`, `"sequence":1,"extra":1`},
		{"unknown register field", `"value":1`, `"value":1,"extra":1`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := strings.Replace(valid, tc.old, tc.replacement, 1)
			if body == valid {
				t.Fatal("negative test did not alter the response")
			}
			rejectIncrementResponse(t, http.StatusOK, body)
		})
	}
	for name, body := range map[string]string{
		"empty": ``, "malformed": `{"status":`, "null": `null`, "trailing": valid + `{}`,
		"oversized":         valid + strings.Repeat(" ", 4097),
		"wrapped overflow":  strings.Replace(strings.Replace(valid, `"before":0`, `"before":9223372036854775807`, 1), `"value":1`, `"value":-9223372036854775808`, 1),
		"lossy float delta": strings.Replace(strings.Replace(valid, `"before":0`, `"before":9007199254740992`, 1), `"value":1`, `"value":9007199254740994`, 1),
	} {
		t.Run(name, func(t *testing.T) { rejectIncrementResponse(t, http.StatusOK, body) })
	}
}

func TestRunIncrementErrorsAndRedirectsDoNotReissue(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status int
		body   string
	}{
		{"not applied", 403, `{"status":"error","error":"mode forbids increment","outcome":"not_applied"}`},
		{"indeterminate", 504, `{"status":"error","error":"deadline","outcome":"indeterminate","request":{"owner":"n1","sequence":1}}`},
		{"failed outcome", 503, `{"status":"error","error":"unavailable","outcome":"failed"}`},
		{"missing outcome", 503, `{"status":"error","error":"unavailable"}`},
		{"null outcome", 503, `{"status":"error","error":"unavailable","outcome":null}`},
		{"missing error", 503, `{"status":"error","outcome":"not_applied"}`},
		{"wrong status", 503, `{"status":"completed","error":"unavailable","outcome":"not_applied"}`},
		{"error on success", 200, `{"status":"error","error":"unavailable","outcome":"not_applied"}`},
		{"invalid request", 503, `{"status":"error","error":"unavailable","outcome":"not_applied","request":{"owner":"n1","sequence":0}}`},
		{"error with before", 503, `{"status":"error","error":"unavailable","outcome":"indeterminate","before":0}`},
		{"error with value", 503, `{"status":"error","error":"unavailable","outcome":"indeterminate","value":1}`},
		{"trailing error", 503, `{"status":"error","error":"unavailable","outcome":"not_applied"} {}`},
		{"malformed error", 503, `<html>unavailable</html>`},
		{"oversized error", 503, strings.Repeat(" ", 4097)},
	} {
		t.Run(tc.name, func(t *testing.T) { rejectIncrementResponse(t, tc.status, tc.body) })
	}
	for _, status := range []int{301, 302, 303, 307, 308} {
		rejectIncrementResponse(t, status, `{"status":"error","error":"moved","outcome":"not_applied"}`)
	}
}

func TestRunIncrementTimeoutDoesNotReportOrReissue(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		_, _ = io.Copy(io.Discard, r.Body)
		<-r.Context().Done()
	}))
	defer server.Close()
	var output bytes.Buffer
	err := run([]string{"-action", "increment", "-url", server.URL, "-timeout", "50ms"}, &output)
	var timeout interface{ Timeout() bool }
	if !errors.As(err, &timeout) || !timeout.Timeout() || output.Len() != 0 || calls.Load() != 1 {
		t.Fatalf("timeout reported completion or reissued: error=%v, output=%q, calls=%d", err, output.String(), calls.Load())
	}
}

func TestRunIncrementRejectsInvalidFlags(t *testing.T) {
	for _, args := range [][]string{
		{"-action", "increment", "-value", "0"},
		{"-action", "increment", "-wait-ms", "0"},
		{"-action", "increment", "-wait-ms", "3001"},
	} {
		var output bytes.Buffer
		if err := run(args, &output); err == nil || output.Len() != 0 {
			t.Fatalf("invalid increment flags %v: error=%v, output=%q", args, err, output.String())
		}
	}
}
