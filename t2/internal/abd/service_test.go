package abd

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"mc714-t2/internal/config"
	"mc714-t2/internal/wire"
)

type replicaFilter struct {
	serve func(http.ResponseWriter, *http.Request, http.Handler)
}

type serviceNode struct {
	server  *httptest.Server
	service *Service
	dir     string
	filter  atomic.Pointer[replicaFilter]
}

type serviceNetwork struct {
	nodes  []*serviceNode
	client *http.Client
}

// Every protocol reply comes from a real Service and a real fsynced temp Store.
// Filters inject partitions, delayed delivery or corrupt replies, never echoes.
func newServiceNetwork(t *testing.T) *serviceNetwork {
	t.Helper()
	network := &serviceNetwork{client: &http.Client{Timeout: 5 * time.Second}}
	members := make([]config.Member, 3)
	for i := range members {
		node := &serviceNode{server: httptest.NewUnstartedServer(nil), dir: t.TempDir()}
		network.nodes = append(network.nodes, node)
		members[i] = config.Member{ID: fmt.Sprintf("node%d", i), URL: "http://" + node.server.Listener.Addr().String()}
	}
	membership, err := json.Marshal(struct {
		Mode    string          `json:"mode"`
		Members []config.Member `json:"members"`
	}{Mode: config.Standalone, Members: members})
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "members.json")
	if err := os.WriteFile(path, membership, 0600); err != nil {
		t.Fatal(err)
	}
	for i, node := range network.nodes {
		cluster, err := config.Load(path, members[i].ID, "service-test")
		if err != nil {
			t.Fatal(err)
		}
		node.service, err = NewService(cluster, node.dir)
		if err != nil {
			t.Fatal(err)
		}
		mux := http.NewServeMux()
		node.service.Register(mux)
		node.server.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if filter := node.filter.Load(); filter != nil && r.URL.Path == "/abd/messages" {
				filter.serve(w, r, mux)
				return
			}
			mux.ServeHTTP(w, r)
		})
		node.server.Start()
		t.Cleanup(func() {
			node.server.Close()
			if err := node.service.Close(); err != nil {
				t.Error(err)
			}
		})
	}
	t.Cleanup(network.client.CloseIdleConnections)
	return network
}

func (n *serviceNode) partition() {
	n.filter.Store(&replicaFilter{serve: func(w http.ResponseWriter, _ *http.Request, _ http.Handler) {
		http.Error(w, "partition", http.StatusServiceUnavailable)
	}})
}

type operationReply struct {
	status  int
	result  wire.ABDResult
	failure wire.ABDError
	err     error
}

func (n *serviceNetwork) begin(ctx context.Context, node int, operation string, timeout int64, value *int64) <-chan operationReply {
	done := make(chan operationReply, 1)
	go func() {
		payload, err := json.Marshal(wire.ABDRequest{TimeoutMS: timeout, Value: value})
		if err != nil {
			done <- operationReply{err: err}
			return
		}
		request, err := http.NewRequestWithContext(ctx, http.MethodPost, n.nodes[node].server.URL+"/abd/"+operation, bytes.NewReader(payload))
		if err != nil {
			done <- operationReply{err: err}
			return
		}
		response, err := n.client.Do(request)
		if err != nil {
			done <- operationReply{err: err}
			return
		}
		defer response.Body.Close()
		reply := operationReply{status: response.StatusCode}
		if response.StatusCode == http.StatusOK {
			reply.err = json.NewDecoder(response.Body).Decode(&reply.result)
		} else {
			reply.err = json.NewDecoder(response.Body).Decode(&reply.failure)
		}
		done <- reply
	}()
	return done
}

func awaitOperation(t *testing.T, done <-chan operationReply) operationReply {
	t.Helper()
	select {
	case reply := <-done:
		if reply.err != nil {
			t.Fatal(reply.err)
		}
		return reply
	case <-time.After(5 * time.Second):
		t.Fatal("operation did not terminate")
		return operationReply{}
	}
}

func (n *serviceNetwork) call(t *testing.T, node int, operation string, timeout int64, value *int64) operationReply {
	t.Helper()
	return awaitOperation(t, n.begin(context.Background(), node, operation, timeout, value))
}

func completed(t *testing.T, reply operationReply, operation string) wire.Register {
	t.Helper()
	if reply.status != http.StatusOK || reply.result.Status != "completed" || reply.result.Operation != operation || reply.result.Register == nil || reply.result.Request.Sequence == 0 || reply.result.Request.Owner != reply.result.NodeID || reply.result.DurationNS < 0 {
		t.Fatalf("not a completed %s: %+v", operation, reply)
	}
	return *reply.result.Register
}

func failed(t *testing.T, reply operationReply, status int, outcome string) {
	t.Helper()
	if reply.status != status || reply.failure.Status != "error" || reply.failure.Outcome != outcome || reply.failure.Error == "" {
		t.Fatalf("want HTTP %d/%s, got %+v", status, outcome, reply)
	}
}

func (n *serviceNetwork) rpc(t *testing.T, from, to int, kind wire.Kind, register *wire.Register) (int, wire.Message) {
	t.Helper()
	cluster := n.nodes[from].service.cluster
	message := wire.Message{Version: wire.ProtocolVersion, RunID: cluster.RunID, ClusterID: cluster.ClusterID, From: cluster.NodeID, To: n.nodes[to].service.cluster.NodeID, MessageID: 1, Kind: kind, Request: wire.RequestID{Owner: cluster.NodeID, Sequence: 1}, Register: register}
	payload, err := json.Marshal(message)
	if err != nil {
		t.Fatal(err)
	}
	response, err := n.client.Post(n.nodes[to].server.URL+"/abd/messages", "application/json", bytes.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return response.StatusCode, wire.Message{}
	}
	reply, err := wire.DecodeMessage(response.Body, cluster)
	if err != nil {
		t.Fatal(err)
	}
	if reply.From != message.To || reply.To != message.From || reply.Request != message.Request || reply.MessageID == 0 {
		t.Fatalf("uncorrelated replica reply: %+v", reply)
	}
	return response.StatusCode, reply
}

func replicaMessage(r *http.Request) (wire.Message, error) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return wire.Message{}, err
	}
	r.Body.Close()
	r.Body = io.NopCloser(bytes.NewReader(body))
	var message wire.Message
	err = json.Unmarshal(body, &message)
	return message, err
}

func TestServiceConcurrentWriters(t *testing.T) {
	n := newServiceNetwork(t)
	initial := int64(-7)
	completed(t, n.call(t, 0, "write", 2000, &initial), "write")
	writes := []int64{11, 22, 33}
	pending := make([]<-chan operationReply, len(writes))
	for i := range writes {
		pending[i] = n.begin(context.Background(), i, "write", 3000, &writes[i])
	}
	seen := make(map[wire.Tag]bool)
	var greatest wire.Register
	for i, done := range pending {
		reply := awaitOperation(t, done)
		register := completed(t, reply, "write")
		if register.Value != writes[i] || register.Tag.WriterID != n.nodes[i].service.cluster.NodeID || seen[register.Tag] {
			t.Fatalf("write tag/value not unique: %+v", register)
		}
		seen[register.Tag] = true
		if register.Tag.Compare(greatest.Tag) > 0 {
			greatest = register
		}
	}
	for i := range n.nodes {
		register := completed(t, n.call(t, i, "read", 2000, nil), "read")
		if register != greatest {
			t.Fatalf("read after completed writes: got %+v, want %+v", register, greatest)
		}
	}
	// A read overlapping a new write may see either value, never an invented one.
	value := int64(44)
	write := n.begin(context.Background(), 0, "write", 2000, &value)
	read := n.begin(context.Background(), 1, "read", 2000, nil)
	written := completed(t, awaitOperation(t, write), "write")
	observed := completed(t, awaitOperation(t, read), "read")
	if observed != greatest && observed != written {
		t.Fatalf("overlapping read returned uncommitted value: %+v", observed)
	}
	if got := completed(t, n.call(t, 2, "read", 2000, nil), "read"); got != written {
		t.Fatalf("real-time successor read: got %+v, want %+v", got, written)
	}
}

func TestServiceReadWritebackAndCrashedMinority(t *testing.T) {
	n := newServiceNetwork(t)
	n.nodes[2].partition()
	value := int64(73)
	written := completed(t, n.call(t, 0, "write", 2000, &value), "write")
	stale, err := n.nodes[2].service.store.Read()
	if err != nil || stale != (wire.Register{}) {
		t.Fatalf("partitioned replica was updated: %+v, %v", stale, err)
	}
	// The stale coordinator can only intersect the previous majority through node1.
	n.nodes[0].server.Close()
	n.nodes[2].filter.Store(nil)
	got := completed(t, n.call(t, 2, "read", 2000, nil), "read")
	if got != written {
		t.Fatalf("stale coordinator read %+v, want %+v", got, written)
	}
	local, err := n.nodes[2].service.store.Read()
	if err != nil || local != written {
		t.Fatalf("read did not durably write back: %+v, %v", local, err)
	}
	value = 74
	completed(t, n.call(t, 1, "write", 2000, &value), "write")
	if got := completed(t, n.call(t, 2, "read", 2000, nil), "read"); got.Value != value {
		t.Fatalf("crashed minority prevented visible write: %+v", got)
	}
}

func TestServiceInsufficientMajorityNeverCompletes(t *testing.T) {
	n := newServiceNetwork(t)
	n.nodes[1].server.Close()
	n.nodes[2].server.Close()
	failed(t, n.call(t, 0, "read", 120, nil), http.StatusGatewayTimeout, "failed")
	value := int64(8)
	failed(t, n.call(t, 0, "write", 120, &value), http.StatusGatewayTimeout, "not_applied")
	local, err := n.nodes[0].service.store.Read()
	if err != nil || local != (wire.Register{}) {
		t.Fatalf("query-only failed write changed local register: %+v, %v", local, err)
	}
}

func TestServicePartialWriteIsIndeterminateAndVisible(t *testing.T) {
	n := newServiceNetwork(t)
	for _, node := range n.nodes[1:] {
		node.filter.Store(&replicaFilter{serve: func(w http.ResponseWriter, r *http.Request, next http.Handler) {
			message, err := replicaMessage(r)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			if message.Kind == wire.ABDUpdate {
				// Commit through the actual replica handler, then lose its ACK.
				next.ServeHTTP(httptest.NewRecorder(), r)
				http.Error(w, "ACK lost after durable update", http.StatusServiceUnavailable)
				return
			}
			next.ServeHTTP(w, r)
		}})
	}
	value := int64(91)
	failed(t, n.call(t, 0, "write", 250, &value), http.StatusGatewayTimeout, "indeterminate")
	local, err := n.nodes[0].service.store.Read()
	if err != nil || local.Value != value || local.Tag.Counter != 1 {
		t.Fatalf("ambiguous write not locally durable: %+v, %v", local, err)
	}
	for _, node := range n.nodes {
		node.filter.Store(nil)
	}
	if got := completed(t, n.call(t, 1, "read", 2000, nil), "read"); got != local {
		t.Fatalf("indeterminate write was not visible: %+v, want %+v", got, local)
	}
	next := int64(92)
	written := completed(t, n.call(t, 0, "write", 2000, &next), "write")
	if written.Tag.Counter <= local.Tag.Counter {
		t.Fatalf("canceled write tag reused: %+v after %+v", written, local)
	}
}

func TestServiceGateCancellationAndReplicaProgress(t *testing.T) {
	n := newServiceNetwork(t)
	entered := make(chan struct{}, 2)
	for _, node := range n.nodes[1:] {
		node.filter.Store(&replicaFilter{serve: func(_ http.ResponseWriter, r *http.Request, _ http.Handler) {
			_, _ = io.Copy(io.Discard, r.Body)
			select {
			case entered <- struct{}{}:
			default:
			}
			<-r.Context().Done()
		}})
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	value := int64(1)
	pending := n.begin(ctx, 0, "write", 3000, &value)
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("coordinator never contacted replicas")
	}
	failed(t, n.call(t, 0, "read", 1000, nil), http.StatusTooManyRequests, "failed")
	status, reply := n.rpc(t, 1, 0, wire.ABDQuery, nil)
	if status != http.StatusOK || reply.Kind != wire.ABDState {
		t.Fatalf("busy coordinator blocked independent replica handler: %d %+v", status, reply)
	}
	cancel()
	select {
	case reply := <-pending:
		if !errors.Is(reply.err, context.Canceled) {
			t.Fatalf("client cancellation ignored: %+v", reply)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("canceled client did not return")
	}
	// Acquiring the internal gate proves the canceled handler joined all workers.
	select {
	case n.nodes[0].service.gate <- struct{}{}:
		<-n.nodes[0].service.gate
	case <-time.After(time.Second):
		t.Fatal("canceled coordinator retained gate")
	}
	for _, node := range n.nodes[1:] {
		node.filter.Store(nil)
	}
	if got := completed(t, n.call(t, 0, "read", 2000, nil), "read"); got != (wire.Register{}) {
		t.Fatalf("canceled query propagated a write: %+v", got)
	}
	written := completed(t, n.call(t, 0, "write", 2000, &value), "write")
	if written.Tag.Counter != 1 {
		t.Fatalf("fresh operation inherited canceled write tag: %+v", written)
	}
}

func TestServiceCancelsSlowMinorityAfterMajority(t *testing.T) {
	n := newServiceNetwork(t)
	entered := make(chan struct{})
	canceled := make(chan struct{}, 4)
	var once sync.Once
	n.nodes[2].filter.Store(&replicaFilter{serve: func(_ http.ResponseWriter, r *http.Request, _ http.Handler) {
		_, _ = io.Copy(io.Discard, r.Body)
		once.Do(func() { close(entered) })
		<-r.Context().Done()
		select {
		case canceled <- struct{}{}:
		default:
		}
	}})
	n.nodes[1].filter.Store(&replicaFilter{serve: func(w http.ResponseWriter, r *http.Request, next http.Handler) {
		select {
		case <-entered:
			next.ServeHTTP(w, r)
		case <-r.Context().Done():
		}
	}})
	pending := n.begin(context.Background(), 0, "read", 3000, nil)
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("slow minority was never contacted")
	}
	select {
	case reply := <-pending:
		if reply.err != nil {
			t.Fatal(reply.err)
		}
		completed(t, reply, "read")
	case <-time.After(450 * time.Millisecond):
		t.Fatal("majority waited for the slow minority attempt timeout")
	}
	select {
	case <-canceled:
	case <-time.After(time.Second):
		t.Fatal("slow minority request was not canceled")
	}
}

func TestServiceDurableReplicaUpdatesAndRestartRejection(t *testing.T) {
	n := newServiceNetwork(t)
	register := wire.Register{Tag: wire.Tag{Counter: 7, WriterID: "node1"}, Value: -12}
	status, ack := n.rpc(t, 1, 0, wire.ABDUpdate, &register)
	if status != http.StatusOK || ack.Kind != wire.ABDAck || ack.Register == nil || *ack.Register != register {
		t.Fatalf("update not acknowledged: %d %+v", status, ack)
	}
	// Inspect the real snapshot immediately after the ACK, before closing anything.
	data, err := os.ReadFile(filepath.Join(n.nodes[0].dir, "register.json"))
	if err != nil {
		t.Fatal(err)
	}
	var snapshot struct {
		Register wire.Register `json:"register"`
	}
	if err := json.Unmarshal(data, &snapshot); err != nil || snapshot.Register != register {
		t.Fatalf("ACK preceded durable snapshot: %+v %v", snapshot, err)
	}
	stale := wire.Register{Tag: wire.Tag{Counter: 6, WriterID: "node1"}, Value: 99}
	status, ack = n.rpc(t, 1, 0, wire.ABDUpdate, &stale)
	if status != http.StatusOK || ack.Register == nil || *ack.Register != register {
		t.Fatalf("stale update changed register: %d %+v", status, ack)
	}
	status, _ = n.rpc(t, 1, 0, wire.ABDUpdate, &register)
	if status != http.StatusOK {
		t.Fatalf("idempotent retry failed: %d", status)
	}
	conflict := register
	conflict.Value++
	status, _ = n.rpc(t, 1, 0, wire.ABDUpdate, &conflict)
	if status != http.StatusConflict {
		t.Fatalf("same-tag conflict was accepted: %d", status)
	}
	node := n.nodes[0]
	node.server.Close()
	if err := node.service.Close(); err != nil {
		t.Fatal(err)
	}
	inspection, err := OpenStore(node.dir, node.service.cluster)
	if err != nil {
		t.Fatal(err)
	}
	got, err := inspection.Read()
	if err != nil || got != register {
		t.Fatalf("reopened snapshot lost register: %+v %v", got, err)
	}
	if err := inspection.Close(); err != nil {
		t.Fatal(err)
	}
	if restarted, err := NewService(node.service.cluster, node.dir); !errors.Is(err, ErrAlreadyStarted) {
		if restarted != nil {
			restarted.Close()
		}
		t.Fatalf("same-run writer restart not rejected: %v", err)
	}
}

func TestServiceStrictOperationRequests(t *testing.T) {
	n := newServiceNetwork(t)
	cases := []struct{ operation, payload string }{
		{"read", `{}`},
		{"read", `null`},
		{"read", `[]`},
		{"read", `{"timeout_ms":null}`},
		{"read", `{"timeout_ms":0}`},
		{"read", `{"timeout_ms":3001}`},
		{"read", `{"timeout_ms":1.5}`},
		{"read", `{"Timeout_ms":100}`},
		{"read", `{"timeout_ms":100,"timeout_ms":200}`},
		{"read", `{"timeout_ms":100,"value":null}`},
		{"read", `{"timeout_ms":100,"value":0}`},
		{"read", `{"timeout_ms":100,"extra":1}`},
		{"read", `{"timeout_ms":100} {}`},
		{"read", `{"timeout_ms":100}` + strings.Repeat(" ", 1024)},
		{"write", `{"timeout_ms":100}`},
		{"write", `{"timeout_ms":100,"value":null}`},
		{"write", `{"timeout_ms":100,"value":1,"value":2}`},
		{"write", `{"timeout_ms":100,"value":9223372036854775808}`},
	}
	for _, test := range cases {
		t.Run(test.operation+"/"+test.payload[:min(len(test.payload), 65)], func(t *testing.T) {
			response, err := n.client.Post(n.nodes[0].server.URL+"/abd/"+test.operation, "application/json", strings.NewReader(test.payload))
			if err != nil {
				t.Fatal(err)
			}
			defer response.Body.Close()
			var failure wire.ABDError
			if err := json.NewDecoder(response.Body).Decode(&failure); err != nil {
				t.Fatal(err)
			}
			outcome := "failed"
			if test.operation == "write" {
				outcome = "not_applied"
			}
			failed(t, operationReply{status: response.StatusCode, failure: failure}, http.StatusBadRequest, outcome)
		})
	}
	zero := int64(0)
	register := completed(t, n.call(t, 0, "write", 2000, &zero), "write")
	if register.Value != 0 || register.Tag.Counter != 1 {
		t.Fatalf("explicit zero was not written: %+v", register)
	}
}

// Corrupt real replica responses, after their actual Store.Read/Apply completed.
func corruptReplicaResponses(node *serviceNode, kind wire.Kind, mutate func(*wire.Message)) {
	node.filter.Store(&replicaFilter{serve: func(w http.ResponseWriter, r *http.Request, next http.Handler) {
		recorder := httptest.NewRecorder()
		next.ServeHTTP(recorder, r)
		if recorder.Code != http.StatusOK {
			w.WriteHeader(recorder.Code)
			_, _ = w.Write(recorder.Body.Bytes())
			return
		}
		var message wire.Message
		if err := json.Unmarshal(recorder.Body.Bytes(), &message); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if message.Kind == kind {
			mutate(&message)
		}
		writeJSON(w, http.StatusOK, message)
	}})
}

func TestServiceRejectsUncorrelatedResponses(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*wire.Message)
	}{
		{"run", func(m *wire.Message) { m.RunID = "other-run" }},
		{"cluster", func(m *wire.Message) { m.ClusterID = strings.Repeat("f", 64) }},
		{"kind", func(m *wire.Message) { m.Kind = wire.ABDAck }},
		{"request", func(m *wire.Message) { m.Request.Sequence++ }},
		{"peer", func(m *wire.Message) { m.From = "node0" }},
		{"recipient", func(m *wire.Message) { m.To = "node1" }},
		{"identifier", func(m *wire.Message) { m.MessageID = 0 }},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			n := newServiceNetwork(t)
			for _, node := range n.nodes[1:] {
				corruptReplicaResponses(node, wire.ABDState, test.mutate)
			}
			failed(t, n.call(t, 0, "read", 150, nil), http.StatusGatewayTimeout, "failed")
		})
	}
}

func TestServiceACKTagValidation(t *testing.T) {
	for _, conflict := range []bool{false, true} {
		t.Run(fmt.Sprintf("conflict=%v", conflict), func(t *testing.T) {
			n := newServiceNetwork(t)
			for _, node := range n.nodes[1:] {
				corruptReplicaResponses(node, wire.ABDAck, func(m *wire.Message) {
					if conflict {
						m.Register.Value++
					} else {
						m.Register = &wire.Register{}
					}
				})
			}
			value := int64(5)
			status := http.StatusGatewayTimeout
			if conflict {
				status = http.StatusConflict
			}
			failed(t, n.call(t, 0, "write", 180, &value), status, "indeterminate")
		})
	}
}

func TestServiceHigherACKDoesNotChangeWriteResult(t *testing.T) {
	n := newServiceNetwork(t)
	n.nodes[2].partition()
	var higher wire.Register
	n.nodes[1].filter.Store(&replicaFilter{serve: func(w http.ResponseWriter, r *http.Request, next http.Handler) {
		message, err := replicaMessage(r)
		if err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		if message.Kind == wire.ABDUpdate && message.Register.Tag.Counter == 1 {
			register := wire.Register{Tag: wire.Tag{Counter: 2, WriterID: "node2"}, Value: 99}
			if _, err := n.nodes[1].service.store.Apply(register); err != nil {
				http.Error(w, err.Error(), 503)
				return
			}
		}
		next.ServeHTTP(w, r)
	}})
	value := int64(40)
	written := completed(t, n.call(t, 0, "write", 2000, &value), "write")
	if written.Value != value || written.Tag != (wire.Tag{Counter: 1, WriterID: "node0"}) {
		t.Fatalf("write returned concurrent higher ACK instead of its proposal: %+v", written)
	}
	higher = wire.Register{Tag: wire.Tag{Counter: 2, WriterID: "node2"}, Value: 99}
	if got := completed(t, n.call(t, 0, "read", 2000, nil), "read"); got != higher {
		t.Fatalf("read ignored concurrent higher register: %+v", got)
	}
}

func TestServiceObservedTagConflict(t *testing.T) {
	n := newServiceNetwork(t)
	n.nodes[2].partition()
	register := wire.Register{Tag: wire.Tag{Counter: 3, WriterID: "node1"}, Value: 1}
	if _, err := n.nodes[0].service.store.Apply(register); err != nil {
		t.Fatal(err)
	}
	register.Value = 2
	if _, err := n.nodes[1].service.store.Apply(register); err != nil {
		t.Fatal(err)
	}
	failed(t, n.call(t, 0, "read", 1000, nil), http.StatusConflict, "failed")
}

func TestServiceCounterExhaustion(t *testing.T) {
	t.Run("register", func(t *testing.T) {
		n := newServiceNetwork(t)
		max := wire.Register{Tag: wire.Tag{Counter: math.MaxUint64, WriterID: "node1"}, Value: 9}
		if _, err := n.nodes[0].service.store.Apply(max); err != nil {
			t.Fatal(err)
		}
		value := int64(10)
		failed(t, n.call(t, 0, "write", 1000, &value), http.StatusServiceUnavailable, "not_applied")
		local, err := n.nodes[0].service.store.Read()
		if err != nil || local != max {
			t.Fatalf("counter wrapped: %+v %v", local, err)
		}
	})
	t.Run("frames", func(t *testing.T) {
		n := newServiceNetwork(t)
		n.nodes[0].service.messages.Store(math.MaxUint64)
		status, _ := n.rpc(t, 1, 0, wire.ABDQuery, nil)
		if status != http.StatusServiceUnavailable {
			t.Fatalf("exhausted frame ID accepted: %d", status)
		}
		value := int64(10)
		failed(t, n.call(t, 0, "write", 1000, &value), http.StatusServiceUnavailable, "not_applied")
		if got := n.nodes[0].service.messages.Load(); got != math.MaxUint64 {
			t.Fatalf("frame counter wrapped: %d", got)
		}
	})
	t.Run("operations", func(t *testing.T) {
		n := newServiceNetwork(t)
		n.nodes[0].service.sequence = math.MaxUint64
		value := int64(10)
		failed(t, n.call(t, 0, "write", 1000, &value), http.StatusServiceUnavailable, "not_applied")
	})
}

func TestServiceStateAndMessageValidation(t *testing.T) {
	n := newServiceNetwork(t)
	status, initial := n.rpc(t, 1, 0, wire.ABDQuery, nil)
	if status != http.StatusOK || initial.Register == nil || *initial.Register != (wire.Register{}) {
		t.Fatalf("initial replica query: %d %+v", status, initial)
	}
	value := int64(-81)
	written := completed(t, n.call(t, 0, "write", 2000, &value), "write")
	status, latest := n.rpc(t, 1, 0, wire.ABDQuery, nil)
	if status != http.StatusOK || latest.Register == nil || *latest.Register != written || latest.MessageID <= initial.MessageID {
		t.Fatalf("retried query returned cached state/ID: %d %+v", status, latest)
	}
	response, err := n.client.Get(n.nodes[0].server.URL + "/abd/state")
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var state struct {
		NodeID    string        `json:"node_id"`
		RunID     string        `json:"run_id"`
		ClusterID string        `json:"cluster_id"`
		Register  wire.Register `json:"register"`
	}
	if err := json.NewDecoder(response.Body).Decode(&state); err != nil {
		t.Fatal(err)
	}
	cluster := n.nodes[0].service.cluster
	if response.StatusCode != http.StatusOK || state.NodeID != cluster.NodeID || state.RunID != cluster.RunID || state.ClusterID != cluster.ClusterID || state.Register != written {
		t.Fatalf("state is not the local durable register: %d %+v", response.StatusCode, state)
	}
	base := wire.Message{Version: wire.ProtocolVersion, RunID: cluster.RunID, ClusterID: cluster.ClusterID, From: "node1", To: "node0", MessageID: 1, Kind: wire.ABDQuery, Request: wire.RequestID{Owner: "node1", Sequence: 1}}
	cases := []struct {
		name   string
		status int
		mutate func(*wire.Message)
	}{
		{"run", 400, func(m *wire.Message) { m.RunID = "other-run" }},
		{"recipient", 400, func(m *wire.Message) { m.To = "node2" }},
		{"query-payload", 400, func(m *wire.Message) { m.Register = &written }},
		{"missing-update", 400, func(m *wire.Message) { m.Kind = wire.ABDUpdate }},
		{"reply-kind", 422, func(m *wire.Message) {
			m.Kind = wire.ABDState
			m.Request.Owner = "node0"
			m.Register = &written
		}},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			message := base
			test.mutate(&message)
			payload, err := json.Marshal(message)
			if err != nil {
				t.Fatal(err)
			}
			response, err := n.client.Post(n.nodes[0].server.URL+"/abd/messages", "application/json", bytes.NewReader(payload))
			if err != nil {
				t.Fatal(err)
			}
			defer response.Body.Close()
			if response.StatusCode != test.status {
				t.Fatalf("got HTTP %d, want %d", response.StatusCode, test.status)
			}
		})
	}
}

func TestServiceRPCReattemptKeepsIdentityAndReadsFreshState(t *testing.T) {
	n := newServiceNetwork(t)
	n.nodes[2].partition()
	register := wire.Register{Tag: wire.Tag{Counter: 4, WriterID: "node2"}, Value: 17}
	var mu sync.Mutex
	var first wire.Message
	var attempts int
	n.nodes[1].filter.Store(&replicaFilter{serve: func(w http.ResponseWriter, r *http.Request, next http.Handler) {
		message, err := replicaMessage(r)
		if err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		if message.Kind == wire.ABDQuery {
			mu.Lock()
			attempts++
			count := attempts
			if count == 1 {
				first = message
			}
			match := message.MessageID == first.MessageID && message.Request == first.Request
			mu.Unlock()
			if !match {
				http.Error(w, "RPC retry changed identity", 400)
				return
			}
			if count == 1 {
				if _, err := n.nodes[1].service.store.Apply(register); err != nil {
					http.Error(w, err.Error(), 503)
					return
				}
				http.Error(w, "transient delivery failure", 503)
				return
			}
		}
		next.ServeHTTP(w, r)
	}})
	reply := n.call(t, 0, "read", 2000, nil)
	if got := completed(t, reply, "read"); got != register {
		t.Fatalf("retried query failed to observe newer durable state: %+v", got)
	}
	mu.Lock()
	defer mu.Unlock()
	if attempts < 2 || reply.result.Request.Sequence != 1 {
		t.Fatalf("expected RPC retry, not logical operation retry: attempts=%d request=%+v", attempts, reply.result.Request)
	}
}

func TestServiceLocalOnlyPartialWrite(t *testing.T) {
	n := newServiceNetwork(t)
	for _, node := range n.nodes[1:] {
		node.filter.Store(&replicaFilter{serve: func(w http.ResponseWriter, r *http.Request, next http.Handler) {
			message, err := replicaMessage(r)
			if err != nil {
				http.Error(w, err.Error(), 400)
				return
			}
			if message.Kind == wire.ABDUpdate {
				http.Error(w, "update partition", 503)
				return
			}
			next.ServeHTTP(w, r)
		}})
	}
	value := int64(27)
	failed(t, n.call(t, 0, "write", 200, &value), http.StatusGatewayTimeout, "indeterminate")
	for _, node := range n.nodes[1:] {
		register, err := node.service.store.Read()
		if err != nil || register != (wire.Register{}) {
			t.Fatalf("partitioned update reached a remote replica: %+v %v", register, err)
		}
		node.filter.Store(nil)
	}
	// A subsequent read can make the previously local-only write visible.
	if got := completed(t, n.call(t, 0, "read", 2000, nil), "read"); got.Value != value {
		t.Fatalf("partial write was incorrectly reported as not applied: %+v", got)
	}
}

func TestServiceUnavailableLocalReplicaCannotCoordinate(t *testing.T) {
	n := newServiceNetwork(t)
	if err := n.nodes[0].service.store.Close(); err != nil {
		t.Fatal(err)
	}
	value := int64(5)
	failed(t, n.call(t, 0, "write", 1000, &value), http.StatusServiceUnavailable, "not_applied")
	failed(t, n.call(t, 0, "read", 1000, nil), http.StatusServiceUnavailable, "failed")
	status, _ := n.rpc(t, 1, 0, wire.ABDQuery, nil)
	if status != http.StatusServiceUnavailable {
		t.Fatalf("closed local replica voted: %d", status)
	}
	response, err := n.client.Get(n.nodes[0].server.URL + "/abd/state")
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("closed store served cached diagnostic state: %d", response.StatusCode)
	}
}
