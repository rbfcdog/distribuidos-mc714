package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"mc714-t2/internal/config"
)

func TestArrivalStreamIndependentOfTargetSelection(t *testing.T) {
	schedule := func(nodes int, seed uint64) []int64 {
		var output bytes.Buffer
		w := &workload{
			o: options{rate: 1000}, targets: make([]string, nodes),
			jobs: make(chan job), records: &recorder{encoder: json.NewEncoder(&output)},
			arrivals: rand.New(rand.NewPCG(seed, 0)), choices: rand.New(rand.NewPCG(seed, 1)),
		}
		// No workers: every scheduled arrival is recorded as a local queue rejection.
		if err := w.phase("measurement", 5*time.Millisecond, nil); err != nil {
			t.Fatal(err)
		}
		decoder := json.NewDecoder(&output)
		var result []int64
		for {
			var row struct {
				Type        string `json:"type"`
				ScheduledNS int64  `json:"scheduled_ns"`
			}
			if err := decoder.Decode(&row); errors.Is(err, io.EOF) {
				break
			} else if err != nil {
				t.Fatal(err)
			}
			if row.Type == "operation" {
				result = append(result, row.ScheduledNS)
			}
		}
		return result
	}
	three, seven, other := schedule(3, 42), schedule(7, 42), schedule(3, 43)
	if fmt.Sprint(three) != fmt.Sprint(seven) {
		t.Fatalf("cluster size changed planned arrivals: %v vs %v", three, seven)
	}
	if fmt.Sprint(three) == fmt.Sprint(other) {
		t.Fatal("different seed did not alter the offered schedule")
	}
}

func TestQueueRejectionDoesNotRetimingOrLoseArrival(t *testing.T) {
	var output bytes.Buffer
	w := &workload{jobs: make(chan job, 1), records: &recorder{encoder: json.NewEncoder(&output)}}
	origin := time.Now().Add(-time.Millisecond)
	for id := int64(1); id <= 2; id++ {
		value := id
		w.offer(job{origin: origin, row: operation{Type: "operation", Phase: "measurement", ID: id, NodeID: "node1", Operation: "write", ScheduledNS: id, Value: &value, GeneratorLagNS: 50}})
	}
	if len(w.jobs) != 1 {
		t.Fatal("queue exceeded bound or lost accepted arrival")
	}
	accepted := <-w.jobs
	if accepted.row.ID != 1 {
		t.Fatal("wrong arrival accepted")
	}
	w.pending.Done()
	w.pending.Wait()
	var rejected operation
	if err := json.Unmarshal(output.Bytes(), &rejected); err != nil {
		t.Fatal(err)
	}
	if rejected.ID != 2 || rejected.Outcome != "queue_full" || rejected.StartNS != nil || rejected.ScheduledNS != 2 || rejected.WriteOutcome != "not_applied" || rejected.EndNS < rejected.ScheduledNS {
		t.Fatalf("invalid rejected arrival: %+v", rejected)
	}
}

func TestSuccessfulResponseValidation(t *testing.T) {
	w := &workload{o: options{runID: "run"}, cluster: strings.Repeat("a", 64), targets: []string{"a", "b", "c"}}
	start := int64(1)
	ra := fmt.Sprintf(`{"status":"completed","node_id":"node1","run_id":"run","cluster_id":%q,"request":{"owner":"node1","sequence":1},"wait_ns":2,"held_ns":3}`, w.cluster)
	abd := fmt.Sprintf(`{"status":"completed","operation":"write","node_id":"node1","run_id":"run","cluster_id":%q,"request":{"owner":"node1","sequence":1},"register":{"tag":{"counter":1,"writer_id":"node1"},"value":17},"duration_ns":4}`, w.cluster)
	for _, tc := range []struct {
		name, kind, body string
		valid            bool
	}{
		{"valid RA", "execute", ra, true},
		{"missing timing", "execute", strings.Replace(ra, `,"wait_ns":2`, "", 1), false},
		{"negative timing", "execute", strings.Replace(ra, `"held_ns":3`, `"held_ns":-1`, 1), false},
		{"impossible timing", "execute", strings.Replace(ra, `"held_ns":3`, `"held_ns":100`, 1), false},
		{"wrong node", "execute", strings.Replace(ra, `"node_id":"node1"`, `"node_id":"node2"`, 1), false},
		{"zero request", "execute", strings.Replace(ra, `"sequence":1`, `"sequence":0`, 1), false},
		{"wrong run", "execute", strings.Replace(ra, `"run_id":"run"`, `"run_id":"other"`, 1), false},
		{"valid ABD", "write", abd, true},
		{"missing duration", "write", strings.Replace(abd, `,"duration_ns":4`, "", 1), false},
		{"missing register value", "write", strings.Replace(abd, `,"value":17`, "", 1), false},
		{"wrong value", "write", strings.Replace(abd, `"value":17`, `"value":18`, 1), false},
		{"wrong writer", "write", strings.Replace(abd, `"writer_id":"node1"`, `"writer_id":"node2"`, 1), false},
		{"trailing JSON", "write", abd + ` {}`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			value := int64(17)
			row := operation{Operation: tc.kind, NodeID: "node1", StartNS: &start, EndNS: 10, Value: &value}
			err := w.completion(&row, []byte(tc.body))
			if (err == nil) != tc.valid {
				t.Fatalf("valid=%v, error=%v", tc.valid, err)
			}
			if !tc.valid && (row.WaitNS != nil || row.HeldNS != nil || row.DurationNS != nil) {
				t.Fatal("invalid response manufactured timing")
			}
		})
	}
}

func TestPhasesDrainBeforeSnapshotsAndLifecycle(t *testing.T) {
	var mu sync.Mutex
	active := 0
	badSnapshot := false
	var targets []string
	var members []config.Member
	hash := strings.Repeat("b", 64)
	for i := range 3 {
		node := fmt.Sprintf("node%d", i+1)
		members = append(members, config.Member{ID: node, URL: "http://" + node + ":8080"})
		server := httptest.NewServer(http.HandlerFunc(func(out http.ResponseWriter, request *http.Request) {
			switch request.URL.Path {
			case "/cluster":
				_ = json.NewEncoder(out).Encode(config.Cluster{NodeID: node, RunID: "run", Mode: config.Standalone, ClusterID: hash, Members: members, Quorum: 2})
			case "/ra/execute":
				mu.Lock()
				active++
				mu.Unlock()
				time.Sleep(2 * time.Millisecond)
				mu.Lock()
				active--
				mu.Unlock()
				_, _ = fmt.Fprintf(out, `{"status":"completed","node_id":%q,"run_id":"run","cluster_id":%q,"request":{"owner":%q,"sequence":1},"wait_ns":0,"held_ns":0}`, node, hash, node)
			case "/ra/state":
				mu.Lock()
				badSnapshot = badSnapshot || active != 0
				mu.Unlock()
				_, _ = io.WriteString(out, `{"logical_messages":0,"transport_attempts":0}`)
			default:
				http.NotFound(out, request)
			}
		}))
		t.Cleanup(server.Close)
		targets = append(targets, server.URL)
	}
	var lifecycle bytes.Buffer
	path := filepath.Join(t.TempDir(), "raw.jsonl")
	args := []string{"-algorithm", "ra", "-run-id", "run", "-targets", strings.Join(targets, ","), "-rate", "500", "-warmup", "30ms", "-duration", "30ms", "-workers", "1", "-queue", "2", "-output", path}
	if err := run(args, &lifecycle); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	invalid := badSnapshot || active != 0
	mu.Unlock()
	if invalid {
		t.Fatal("snapshot/lifecycle preceded drain")
	}
	var start struct {
		Event  string `json:"event"`
		UnixNS int64  `json:"unix_ns"`
	}
	decoder := json.NewDecoder(&lifecycle)
	if err := decoder.Decode(&start); err != nil || start.Event != "measurement_start" || start.UnixNS <= 0 {
		t.Fatalf("bad lifecycle: %+v %v", start, err)
	}
	var finish struct {
		Event string `json:"event"`
	}
	if err := decoder.Decode(&finish); err != nil || finish.Event != "finished" {
		t.Fatalf("bad finished marker: %+v %v", finish, err)
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		t.Fatal("extra lifecycle output")
	}
	raw, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	rows := json.NewDecoder(raw)
	var order []string
	var operations []operation
	phaseEnds := map[string]int64{}
	for {
		var record json.RawMessage
		if err := rows.Decode(&record); errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			t.Fatal(err)
		}
		var header struct {
			Type, Phase, Event, Stage string
			ElapsedNS                 int64 `json:"elapsed_ns"`
		}
		if err := json.Unmarshal(record, &header); err != nil {
			t.Fatal(err)
		}
		switch header.Type {
		case "phase":
			order = append(order, header.Phase+":"+header.Event)
			if header.Event == "end" {
				phaseEnds[header.Phase] = header.ElapsedNS
			}
		case "snapshot":
			order = append(order, header.Stage)
		case "operation":
			var op operation
			if err := json.Unmarshal(record, &op); err != nil {
				t.Fatal(err)
			}
			operations = append(operations, op)
		}
	}
	if strings.Join(order, ",") != "warmup:start,warmup:end,baseline,measurement:start,measurement:end,final" {
		t.Fatalf("phase order: %v", order)
	}
	if len(operations) == 0 {
		t.Fatal("seeded workload offered no operations")
	}
	for _, op := range operations {
		if op.ScheduledNS < 0 || op.GeneratorLagNS < 0 || op.EndNS < op.ScheduledNS || op.EndNS > phaseEnds[op.Phase] {
			t.Fatalf("invalid phase timing: %+v", op)
		}
		if op.StartNS != nil && (*op.StartNS < op.ScheduledNS+op.GeneratorLagNS || op.EndNS < *op.StartNS) {
			t.Fatalf("invalid request timing: %+v", op)
		}
		if (op.StartNS == nil) != (op.Outcome == "queue_full") {
			t.Fatalf("missing invocation evidence: %+v", op)
		}
	}
}

func TestWriteFailureClassification(t *testing.T) {
	for _, malformed := range []bool{false, true} {
		t.Run(fmt.Sprint(malformed), func(t *testing.T) {
			var output bytes.Buffer
			server := httptest.NewServer(http.HandlerFunc(func(out http.ResponseWriter, _ *http.Request) {
				out.WriteHeader(http.StatusGatewayTimeout)
				if malformed {
					_, _ = io.WriteString(out, `{}`)
				} else {
					_, _ = io.WriteString(out, `{"status":"error","error":"quorum deadline","outcome":"not_applied"}`)
				}
			}))
			defer server.Close()
			value := int64(7)
			w := &workload{o: options{waitMS: 1}, targets: []string{server.URL}, client: server.Client(), records: &recorder{encoder: json.NewEncoder(&output)}}
			w.perform(job{origin: time.Now(), row: operation{Type: "operation", NodeID: "node1", Operation: "write", Value: &value}})
			var row operation
			if err := json.Unmarshal(output.Bytes(), &row); err != nil {
				t.Fatal(err)
			}
			expected := "not_applied"
			if malformed {
				expected = "indeterminate"
			}
			if row.Outcome != "deadline" || row.WriteOutcome != expected || row.HTTPStatus != 504 {
				t.Fatalf("lost failure evidence: %+v", row)
			}
		})
	}
}

func TestMissingFinalSnapshotIsPreserved(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(out http.ResponseWriter, _ *http.Request) {
		http.Error(out, "node unavailable", http.StatusServiceUnavailable)
	}))
	defer server.Close()
	var output bytes.Buffer
	w := &workload{o: options{algorithm: "ra"}, targets: []string{server.URL}, client: server.Client(), records: &recorder{encoder: json.NewEncoder(&output)}}
	if err := w.snapshots("final"); err != nil {
		t.Fatal(err)
	}
	var snapshot struct {
		Nodes []snapshotNode `json:"nodes"`
	}
	if err := json.Unmarshal(output.Bytes(), &snapshot); err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Nodes) != 1 || snapshot.Nodes[0].Error == "" || len(snapshot.Nodes[0].State) != 0 {
		t.Fatal("missing node became fabricated counters")
	}
	if err := w.snapshots("baseline"); err == nil {
		t.Fatal("missing baseline must abort before measurement")
	}
}
