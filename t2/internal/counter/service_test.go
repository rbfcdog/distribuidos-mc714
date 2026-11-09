package counter

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"mc714-t2/internal/abd"
	"mc714-t2/internal/config"
	"mc714-t2/internal/ra"
	"mc714-t2/internal/wire"
)

type networkFilter struct {
	serve func(http.ResponseWriter, *http.Request, http.Handler)
}

type counterNode struct {
	server   *httptest.Server
	cluster  config.Cluster
	guard    *ra.Service
	register *abd.Service
	filter   atomic.Pointer[networkFilter]
}

type counterNetwork struct {
	nodes  []*counterNode
	client *http.Client
}

// Every protocol response uses the real RA engine and fsynced ABD stores.
// Filters only delay or discard network traffic; they never fabricate replies.
func newCounterNetwork(t *testing.T, mode, observerURL string) *counterNetwork {
	t.Helper()
	n := &counterNetwork{client: &http.Client{Timeout: 5 * time.Second}}
	members := make([]config.Member, 3)
	for i := range members {
		node := &counterNode{server: httptest.NewUnstartedServer(nil)}
		n.nodes = append(n.nodes, node)
		members[i] = config.Member{ID: fmt.Sprintf("node%d", i), URL: "http://" + node.server.Listener.Addr().String()}
	}
	membership, err := json.Marshal(struct {
		Mode    string          `json:"mode"`
		Members []config.Member `json:"members"`
	}{mode, members})
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "members.json")
	if err := os.WriteFile(path, membership, 0600); err != nil {
		t.Fatal(err)
	}
	for i, node := range n.nodes {
		node.cluster, err = config.Load(path, members[i].ID, "counter-test")
		if err != nil {
			t.Fatal(err)
		}
		node.guard, err = ra.NewService(node.cluster, observerURL)
		if err != nil {
			t.Fatal(err)
		}
		node.register, err = abd.NewService(node.cluster, t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		mux := http.NewServeMux()
		node.guard.Register(mux)
		node.register.Register(mux)
		NewService(node.cluster, node.guard, node.register).Register(mux)
		node.server.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if filter := node.filter.Load(); filter != nil {
				filter.serve(w, r, mux)
				return
			}
			mux.ServeHTTP(w, r)
		})
		node.server.Start()
		t.Cleanup(func() {
			node.server.Close()
			node.guard.Close()
			if err := node.register.Close(); err != nil {
				t.Error(err)
			}
		})
	}
	t.Cleanup(n.client.CloseIdleConnections)
	return n
}

type incrementReply struct {
	status  int
	result  wire.IncrementResult
	failure wire.ABDError
	raw     map[string]json.RawMessage
	err     error
}

func (n *counterNetwork) post(ctx context.Context, node int, path, payload string) incrementReply {
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, n.nodes[node].server.URL+path, strings.NewReader(payload))
	if err != nil {
		return incrementReply{err: err}
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := n.client.Do(request)
	if err != nil {
		return incrementReply{err: err}
	}
	defer response.Body.Close()
	reply := incrementReply{status: response.StatusCode}
	body, err := io.ReadAll(io.LimitReader(response.Body, 4096))
	if err != nil {
		reply.err = err
		return reply
	}
	reply.err = json.Unmarshal(body, &reply.raw)
	if reply.err == nil {
		if reply.status == http.StatusOK {
			reply.err = json.Unmarshal(body, &reply.result)
		} else {
			reply.err = json.Unmarshal(body, &reply.failure)
		}
	}
	return reply
}

func (n *counterNetwork) begin(ctx context.Context, node int, timeout int) <-chan incrementReply {
	done := make(chan incrementReply, 1)
	go func() {
		done <- n.post(ctx, node, "/increment", fmt.Sprintf(`{"timeout_ms":%d}`, timeout))
	}()
	return done
}

func awaitReply(t *testing.T, done <-chan incrementReply) incrementReply {
	t.Helper()
	select {
	case reply := <-done:
		return reply
	case <-time.After(5 * time.Second):
		t.Fatal("increment did not terminate")
		return incrementReply{}
	}
}

func (n *counterNetwork) increment(t *testing.T, node, timeout int) incrementReply {
	t.Helper()
	return awaitReply(t, n.begin(context.Background(), node, timeout))
}

func requireCompleted(t *testing.T, n *counterNetwork, node int, reply incrementReply) int64 {
	t.Helper()
	result := reply.result
	cluster := n.nodes[node].cluster
	if reply.err != nil || reply.status != http.StatusOK || result.Status != "completed" || result.NodeID != cluster.NodeID || result.RunID != cluster.RunID || result.ClusterID != cluster.ClusterID || result.Request.Owner != cluster.NodeID || result.Request.Sequence == 0 || result.Before == nil || result.Register == nil || result.DurationNS < 0 {
		t.Fatalf("invalid increment completion: %+v", reply)
	}
	if _, ok := reply.raw["duration_ns"]; !ok {
		t.Fatal("missing duration_ns")
	}
	if *result.Before == math.MaxInt64 || result.Register.Value != *result.Before+1 || result.Register.Tag.Counter == 0 || result.Register.Tag.WriterID != cluster.NodeID {
		t.Fatalf("invalid before/after pair: %+v", result)
	}
	return *result.Before
}

func requireFailure(t *testing.T, reply incrementReply, status int, outcome string) {
	t.Helper()
	if reply.err != nil || reply.status != status || reply.failure.Status != "error" || reply.failure.Outcome != outcome || reply.failure.Error == "" {
		t.Fatalf("want HTTP %d/%s, got %+v", status, outcome, reply)
	}
	for _, key := range []string{"before", "value", "register"} {
		if _, ok := reply.raw[key]; ok {
			t.Fatalf("failure leaked success field %s", key)
		}
	}
}

func (n *counterNetwork) state(t *testing.T, node int, wanted string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	var state ra.Snapshot
	for time.Now().Before(deadline) {
		response, err := n.client.Get(n.nodes[node].server.URL + "/ra/state")
		if err != nil {
			t.Fatal(err)
		}
		err = json.NewDecoder(response.Body).Decode(&state)
		response.Body.Close()
		if err != nil {
			t.Fatal(err)
		}
		if state.State == wanted {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("RA state = %+v, want %s", state, wanted)
}

func (n *counterNetwork) value(t *testing.T, node int) int64 {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	result, err := n.nodes[node].register.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return result.Register.Value
}

func (n *counterNetwork) seed(t *testing.T, value int64) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, err := n.nodes[0].register.Write(ctx, value); err != nil {
		t.Fatal(err)
	}
}

func TestConcurrentIncrementsHaveExactSequentialValues(t *testing.T) {
	n := newCounterNetwork(t, config.Integrated, "")
	const perNode = 4
	type nodeReply struct {
		node  int
		reply incrementReply
	}
	replies := make(chan nodeReply, len(n.nodes)*perNode)
	start := make(chan struct{})
	var workers sync.WaitGroup
	for node := range n.nodes {
		workers.Add(1)
		go func() {
			defer workers.Done()
			<-start
			for range perNode {
				replies <- nodeReply{node, n.post(context.Background(), node, "/increment", `{"timeout_ms":3000}`)}
			}
		}()
	}
	close(start)
	workers.Wait()
	close(replies)
	var before []int64
	for item := range replies {
		before = append(before, requireCompleted(t, n, item.node, item.reply))
	}
	sort.Slice(before, func(i, j int) bool { return before[i] < before[j] })
	for i, value := range before {
		if value != int64(i) {
			t.Fatalf("lost or duplicate increment: before values %v", before)
		}
	}
	if got := n.value(t, 2); got != int64(len(n.nodes)*perNode) {
		t.Fatalf("final register = %d", got)
	}
	for node := range n.nodes {
		n.state(t, node, "RELEASED")
	}
}

func TestModeGuardsDoNotMutate(t *testing.T) {
	for _, mode := range []string{config.Standalone, config.Integrated} {
		t.Run(mode, func(t *testing.T) {
			n := newCounterNetwork(t, mode, "")
			path, body := "/increment", `{"timeout_ms":1000}`
			if mode == config.Integrated {
				path, body = "/abd/write", `{"timeout_ms":1000,"value":99}`
			}
			requireFailure(t, n.post(context.Background(), 0, path, body), http.StatusForbidden, "not_applied")
			if got := n.value(t, 1); got != 0 {
				t.Fatalf("forbidden operation changed register to %d", got)
			}
			n.state(t, 0, "RELEASED")
		})
	}
}

func TestOverflowReleasesWithoutChangingRegister(t *testing.T) {
	n := newCounterNetwork(t, config.Integrated, "")
	n.seed(t, math.MaxInt64)
	reply := n.increment(t, 0, 1000)
	requireFailure(t, reply, http.StatusConflict, "not_applied")
	if reply.failure.Request == nil || reply.failure.Request.Owner != n.nodes[0].cluster.NodeID {
		t.Fatal("overflow did not identify the RA request")
	}
	n.state(t, 0, "RELEASED")
	if got := n.value(t, 1); got != math.MaxInt64 {
		t.Fatalf("overflow changed register to %d", got)
	}
	// A different node can acquire too; no deferred RA permission was stranded.
	requireFailure(t, n.increment(t, 1, 1000), http.StatusConflict, "not_applied")
	n.state(t, 1, "RELEASED")
}

func TestFailuresBeforeMutationReleaseAndPermitNextIncrement(t *testing.T) {
	for _, phase := range []string{"acquire", "read", "write_query"} {
		t.Run(phase, func(t *testing.T) {
			n := newCounterNetwork(t, config.Integrated, "")
			for _, node := range n.nodes[1:] {
				node.filter.Store(&networkFilter{serve: func(w http.ResponseWriter, r *http.Request, next http.Handler) {
					blocked := phase == "acquire" && r.URL.Path == "/ra/messages"
					if r.URL.Path == "/abd/messages" && phase != "acquire" {
						message := readFrame(t, r)
						blocked = phase == "read" || (message.Kind == wire.ABDQuery && message.Request.Sequence == 2)
					}
					if blocked {
						http.Error(w, "partition", http.StatusServiceUnavailable)
						return
					}
					next.ServeHTTP(w, r)
				}})
			}
			reply := n.increment(t, 0, 180)
			requireFailure(t, reply, http.StatusGatewayTimeout, "not_applied")
			if reply.failure.Request == nil || reply.failure.Request.Sequence == 0 {
				t.Fatal("failure omitted the RA acquisition identity")
			}
			n.state(t, 0, "RELEASED")
			for _, node := range n.nodes {
				node.filter.Store(nil)
			}
			if got := n.value(t, 2); got != 0 {
				t.Fatalf("failed %s mutated register to %d", phase, got)
			}
			if before := requireCompleted(t, n, 1, n.increment(t, 1, 2000)); before != 0 {
				t.Fatalf("next increment started from %d", before)
			}
		})
	}
}

func readFrame(t *testing.T, r *http.Request) wire.Message {
	t.Helper()
	body, err := io.ReadAll(r.Body)
	if err != nil {
		t.Error(err)
		return wire.Message{}
	}
	r.Body.Close()
	r.Body = io.NopCloser(bytes.NewReader(body))
	var message wire.Message
	if err := json.Unmarshal(body, &message); err != nil {
		t.Error(err)
	}
	return message
}

func TestAppliedWriteWithoutAcknowledgmentsIsIndeterminate(t *testing.T) {
	n := newCounterNetwork(t, config.Integrated, "")
	var applied atomic.Int64
	for _, node := range n.nodes[1:] {
		node.filter.Store(&networkFilter{serve: func(w http.ResponseWriter, r *http.Request, next http.Handler) {
			if r.URL.Path == "/abd/messages" {
				message := readFrame(t, r)
				if message.Kind == wire.ABDUpdate && message.Register.Value == 1 {
					response := httptest.NewRecorder()
					next.ServeHTTP(response, r)
					if response.Code == http.StatusOK {
						applied.Add(1)
					}
					http.Error(w, "acknowledgment lost", http.StatusServiceUnavailable)
					return
				}
			}
			next.ServeHTTP(w, r)
		}})
	}
	requireFailure(t, n.increment(t, 0, 250), http.StatusGatewayTimeout, "indeterminate")
	if applied.Load() == 0 {
		t.Fatal("test did not reach a real remote durable update")
	}
	n.state(t, 0, "RELEASED")
	for _, node := range n.nodes {
		node.filter.Store(nil)
	}
	if got := n.value(t, 2); got != 1 {
		t.Fatalf("indeterminate write not visible: %d", got)
	}
	if before := requireCompleted(t, n, 1, n.increment(t, 1, 2000)); before != 1 {
		t.Fatalf("next increment started from %d", before)
	}
}

func TestObserverFailuresReleaseAndPreserveMutationUncertainty(t *testing.T) {
	for _, failPhase := range []string{"enter", "exit"} {
		t.Run(failPhase, func(t *testing.T) {
			var failing atomic.Bool
			failing.Store(true)
			observer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var event wire.CriticalEvent
				if err := json.NewDecoder(r.Body).Decode(&event); err != nil {
					t.Error(err)
				}
				if failing.Load() && event.Phase == failPhase {
					http.Error(w, "observer unavailable", http.StatusServiceUnavailable)
					return
				}
				w.WriteHeader(http.StatusNoContent)
			}))
			defer observer.Close()
			n := newCounterNetwork(t, config.Integrated, observer.URL)
			outcome, want := "not_applied", int64(0)
			if failPhase == "exit" {
				outcome, want = "indeterminate", 1
			}
			requireFailure(t, n.increment(t, 0, 2000), http.StatusServiceUnavailable, outcome)
			n.state(t, 0, "RELEASED")
			if got := n.value(t, 2); got != want {
				t.Fatalf("after %s failure: register = %d, want %d", failPhase, got, want)
			}
			failing.Store(false)
			if before := requireCompleted(t, n, 1, n.increment(t, 1, 2000)); before != want {
				t.Fatalf("next increment started from %d, want %d", before, want)
			}
		})
	}
}

func TestClientDisconnectAfterEntryStillCompletesAndReleases(t *testing.T) {
	entered, resume := make(chan struct{}), make(chan struct{})
	var once sync.Once
	n := newCounterNetwork(t, config.Integrated, "")
	for _, node := range n.nodes[1:] {
		node.filter.Store(&networkFilter{serve: func(w http.ResponseWriter, r *http.Request, next http.Handler) {
			if r.URL.Path == "/abd/messages" {
				message := readFrame(t, r)
				if message.Kind == wire.ABDUpdate && message.Register.Value == 0 {
					once.Do(func() { close(entered) })
					select {
					case <-resume:
					case <-r.Context().Done():
						return
					}
				}
			}
			next.ServeHTTP(w, r)
		}})
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := n.begin(ctx, 0, 2000)
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("increment never entered RA critical section")
	}
	n.state(t, 0, "HELD")
	// Local overlap is rejected, not queued behind the disconnected request.
	requireFailure(t, n.increment(t, 0, 1000), http.StatusTooManyRequests, "not_applied")
	cancel()
	if reply := awaitReply(t, done); reply.err == nil {
		t.Fatalf("disconnected client unexpectedly got a response: %+v", reply)
	}
	close(resume)
	n.state(t, 0, "RELEASED")
	if got := n.value(t, 2); got != 1 {
		t.Fatalf("disconnect aborted entered work: register = %d", got)
	}
	if before := requireCompleted(t, n, 1, n.increment(t, 1, 2000)); before != 1 {
		t.Fatalf("next increment started from %d", before)
	}
}

func TestDisconnectDuringAcquisitionDoesNotMutate(t *testing.T) {
	n := newCounterNetwork(t, config.Integrated, "")
	n.nodes[2].filter.Store(&networkFilter{serve: func(w http.ResponseWriter, r *http.Request, next http.Handler) {
		if r.URL.Path == "/ra/messages" {
			http.Error(w, "partition", http.StatusServiceUnavailable)
			return
		}
		next.ServeHTTP(w, r)
	}})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := n.begin(ctx, 0, 2000)
	n.state(t, 0, "WANTED")
	cancel()
	if reply := awaitReply(t, done); reply.err == nil {
		t.Fatalf("disconnected client unexpectedly got a response: %+v", reply)
	}
	n.state(t, 0, "RELEASED")
	n.nodes[2].filter.Store(nil)
	if got := n.value(t, 1); got != 0 {
		t.Fatalf("canceled acquisition changed register to %d", got)
	}
	requireCompleted(t, n, 1, n.increment(t, 1, 2000))
}

func TestDisconnectedWorkStillHonorsOriginalDeadline(t *testing.T) {
	n := newCounterNetwork(t, config.Integrated, "")
	entered := make(chan struct{})
	var once sync.Once
	for _, node := range n.nodes[1:] {
		node.filter.Store(&networkFilter{serve: func(w http.ResponseWriter, r *http.Request, next http.Handler) {
			if r.URL.Path == "/abd/messages" {
				// Consume the body so net/http can observe the peer closing the connection.
				if _, err := io.Copy(io.Discard, r.Body); err != nil {
					return
				}
				once.Do(func() { close(entered) })
				<-r.Context().Done()
				return
			}
			next.ServeHTTP(w, r)
		}})
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := n.begin(ctx, 0, 180)
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("entered increment never started ABD read")
	}
	cancel()
	if reply := awaitReply(t, done); reply.err == nil {
		t.Fatalf("disconnected client unexpectedly got a response: %+v", reply)
	}
	n.state(t, 0, "RELEASED")
	for _, node := range n.nodes {
		node.filter.Store(nil)
	}
	if got := n.value(t, 2); got != 0 {
		t.Fatalf("expired read changed register to %d", got)
	}
	requireCompleted(t, n, 1, n.increment(t, 1, 2000))
}

func TestIncrementStrictBoundedJSON(t *testing.T) {
	n := newCounterNetwork(t, config.Integrated, "")
	for _, body := range []string{
		``, `null`, `[]`, `{}`, `{"timeout_ms":null}`, `{"timeout_ms":1.5}`,
		`{"timeout_ms":1e3}`, `{"timeout_ms":"1000"}`, `{"timeout_ms":true}`,
		`{"timeout_ms":0}`, `{"timeout_ms":-1}`, `{"timeout_ms":3001}`,
		`{"timeout_ms":1000,"timeout_ms":1000}`, `{"TIMEOUT_MS":1000}`,
		`{"timeout_ms":1000,"Timeout_MS":1000}`, `{"timeout_ms":1000,"value":1}`,
		`{"timeout_ms":1000} {}`, `{"timeout_ms":1000}x`,
		`{"timeout_ms":1000}` + strings.Repeat(" ", 1025),
	} {
		t.Run(body, func(t *testing.T) {
			requireFailure(t, n.post(context.Background(), 0, "/increment", body), http.StatusBadRequest, "not_applied")
		})
	}
	if got := n.value(t, 2); got != 0 {
		t.Fatalf("invalid input changed register to %d", got)
	}
	body := `{"timeout_ms":2000}`
	body += strings.Repeat(" ", 1024-len(body))
	requireCompleted(t, n, 0, n.post(context.Background(), 0, "/increment", body))
}
