package ra

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"mc714-t2/internal/config"
	"mc714-t2/internal/wire"
)

type networkNode struct {
	server  *httptest.Server
	blocked atomic.Bool
}

func startNetwork(t *testing.T, observerURL string) []*networkNode {
	t.Helper()
	nodes := make([]*networkNode, 3)
	muxes := make([]*http.ServeMux, len(nodes))
	members := make([]config.Member, len(nodes))
	for i := range nodes {
		node := &networkNode{}
		mux := http.NewServeMux()
		muxes[i] = mux
		node.server = httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/ra/messages" && node.blocked.Load() {
				http.Error(w, "peer unavailable", http.StatusServiceUnavailable)
				return
			}
			mux.ServeHTTP(w, r)
		}))
		nodes[i] = node
		members[i] = config.Member{ID: fmt.Sprintf("n%d", i), URL: "http://" + node.server.Listener.Addr().String()}
		t.Cleanup(node.server.Close)
	}
	for i, node := range nodes {
		cluster := config.Cluster{NodeID: members[i].ID, RunID: "network-test", ClusterID: strings.Repeat("a", 64), Members: members, Quorum: 2}
		service, err := NewService(cluster, observerURL)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(service.Close)
		service.Register(muxes[i])
		node.server.Start()
	}
	return nodes
}

func executeHTTP(ctx context.Context, target string, hold, wait int64) (int, wire.ExecuteResult, error) {
	body, err := json.Marshal(wire.ExecuteRequest{HoldMS: hold, WaitMS: wait})
	if err != nil {
		return 0, wire.ExecuteResult{}, err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, target+"/ra/execute", bytes.NewReader(body))
	if err != nil {
		return 0, wire.ExecuteResult{}, err
	}
	client := &http.Client{Timeout: 5 * time.Second}
	response, err := client.Do(request)
	if err != nil {
		return 0, wire.ExecuteResult{}, err
	}
	defer response.Body.Close()
	var result wire.ExecuteResult
	if response.StatusCode == http.StatusOK {
		err = json.NewDecoder(io.LimitReader(response.Body, 4096)).Decode(&result)
	}
	return response.StatusCode, result, err
}

func awaitState(t *testing.T, target, state string, deferred ...int) Snapshot {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	client := &http.Client{Timeout: 300 * time.Millisecond}
	var last Snapshot
	for time.Now().Before(deadline) {
		response, err := client.Get(target + "/ra/state")
		if err != nil {
			t.Fatal(err)
		}
		err = json.NewDecoder(io.LimitReader(response.Body, 4096)).Decode(&last)
		response.Body.Close()
		if err != nil {
			t.Fatal(err)
		}
		if last.State == state && (len(deferred) == 0 || last.Deferred == deferred[0]) {
			return last
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("state = %+v, want %s", last, state)
	return last
}

func TestNetworkCancellationAndPeerFailureNeverGrant(t *testing.T) {
	nodes := startNetwork(t, "")
	nodes[2].blocked.Store(true)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	finished := make(chan error, 1)
	go func() {
		_, _, err := executeHTTP(ctx, nodes[0].server.URL, 0, 3000)
		finished <- err
	}()
	wanted := awaitState(t, nodes[0].server.URL, "WANTED")
	peerFinished := make(chan error, 1)
	go func() {
		status, result, err := executeHTTP(context.Background(), nodes[1].server.URL, 1, 3000)
		if err == nil && (status != http.StatusOK || result.Status != "completed") {
			err = fmt.Errorf("deferred peer result: status=%d result=%+v", status, result)
		}
		peerFinished <- err
	}()
	awaitState(t, nodes[0].server.URL, "WANTED", 1)
	// Force the cancellation's flushed REPLY to fail, then recover this peer.
	// Delivery must survive the canceled acquisition without a fresh REQUEST.
	nodes[1].blocked.Store(true)
	cancel()
	select {
	case err := <-finished:
		if err == nil {
			t.Fatal("canceled HTTP acquisition unexpectedly succeeded")
		}
	case <-time.After(time.Second):
		t.Fatal("canceled HTTP acquisition did not finish")
	}
	awaitState(t, nodes[0].server.URL, "RELEASED")
	for range 3 {
		status, _, err := executeHTTP(context.Background(), nodes[0].server.URL, 0, 60)
		if err != nil || status != http.StatusGatewayTimeout {
			t.Fatalf("missing peer granted permission or wrong error: status=%d err=%v", status, err)
		}
		snapshot := awaitState(t, nodes[0].server.URL, "RELEASED")
		if snapshot.Deferred != 0 {
			t.Fatalf("canceled acquisition retained deferred replies: %+v", snapshot)
		}
	}
	nodes[2].blocked.Store(false)
	nodes[1].blocked.Store(false)
	select {
	case err := <-peerFinished:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("deferred REPLY did not survive cancellation and peer recovery")
	}
	status, result, err := executeHTTP(context.Background(), nodes[0].server.URL, 1, 3000)
	if err != nil || status != http.StatusOK || result.Status != "completed" || result.Request.Sequence <= wanted.Request.Sequence {
		t.Fatalf("acquisition after cancellation: status=%d result=%+v err=%v", status, result, err)
	}
	awaitState(t, nodes[0].server.URL, "RELEASED")
}

func TestNetworkContentionExcludesAndReleasesDespiteDisconnect(t *testing.T) {
	var mu sync.Mutex
	active := make(map[wire.RequestID]bool)
	entries, exits, overlaps := 0, 0, 0
	entered := make(chan wire.RequestID, 32)
	observer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var event wire.CriticalEvent
		if err := json.NewDecoder(r.Body).Decode(&event); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		mu.Lock()
		if event.Phase == "enter" {
			if len(active) != 0 {
				overlaps++
			}
			active[event.Request] = true
			entries++
			entered <- event.Request
		} else {
			delete(active, event.Request)
			exits++
		}
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(observer.Close)
	nodes := startNetwork(t, observer.URL)
	for round := range 2 {
		var workers sync.WaitGroup
		start := make(chan struct{})
		errors := make(chan error, len(nodes))
		for _, node := range nodes {
			workers.Add(1)
			go func() {
				defer workers.Done()
				<-start
				status, result, err := executeHTTP(context.Background(), node.server.URL, 15, 3000)
				if err != nil || status != http.StatusOK || result.Status != "completed" || result.HeldNS < int64(15*time.Millisecond) || result.Request.Owner != result.NodeID {
					errors <- fmt.Errorf("status=%d result=%+v err=%v", status, result, err)
				}
			}()
		}
		close(start)
		workers.Wait()
		close(errors)
		for err := range errors {
			t.Errorf("round %d: %v", round, err)
		}
	}
	// Clear completed entry notifications before the disconnect case.
	for range 6 {
		select {
		case <-entered:
		default:
			t.Fatal("completed operation did not notify observer")
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	finished := make(chan error, 1)
	go func() {
		_, _, err := executeHTTP(ctx, nodes[0].server.URL, 150, 3000)
		finished <- err
	}()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("critical section did not enter")
	}
	cancel()
	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("disconnected client did not finish")
	}
	awaitState(t, nodes[0].server.URL, "HELD")
	awaitState(t, nodes[0].server.URL, "RELEASED")
	mu.Lock()
	defer mu.Unlock()
	if entries != 7 || exits != entries || overlaps != 0 || len(active) != 0 {
		t.Fatalf("observer entries=%d exits=%d overlaps=%d active=%v", entries, exits, overlaps, active)
	}
}

func TestExecuteRejectsInvalidBoundedJSON(t *testing.T) {
	nodes := startNetwork(t, "")
	for _, body := range []string{
		`null`, `{"wait_ms":1,"hold_ms":0,"extra":true}`,
		`{"wait_ms":1,"wait_ms":2}`, `{"wait_ms":null}`,
		`{"wait_ms":0}`, `{"wait_ms":3001}`, `{"wait_ms":1,"hold_ms":501}`,
		`{"wait_ms":1} {}`, `{"wait_ms":1}` + strings.Repeat(" ", 1024),
	} {
		response, err := http.Post(nodes[0].server.URL+"/ra/execute", "application/json", strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		response.Body.Close()
		if response.StatusCode != http.StatusBadRequest {
			t.Errorf("body %q: status=%d", body, response.StatusCode)
		}
	}
}

func TestObserverFailureAttemptsExitAndReleases(t *testing.T) {
	phases := make(chan string, 2)
	observer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var event wire.CriticalEvent
		if err := json.NewDecoder(r.Body).Decode(&event); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		phases <- event.Phase
		if event.Phase == "enter" {
			http.Error(w, "recorded enter but acknowledgement failed", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(observer.Close)
	nodes := startNetwork(t, observer.URL)
	status, _, err := executeHTTP(context.Background(), nodes[0].server.URL, 0, 3000)
	if err != nil || status != http.StatusServiceUnavailable {
		t.Fatalf("observer failure falsely completed: status=%d err=%v", status, err)
	}
	awaitState(t, nodes[0].server.URL, "RELEASED")
	for _, want := range []string{"enter", "exit"} {
		select {
		case got := <-phases:
			if got != want {
				t.Fatalf("observer phase=%q want=%q", got, want)
			}
		default:
			t.Fatalf("missing observer %s after failed enter acknowledgement", want)
		}
	}
}
