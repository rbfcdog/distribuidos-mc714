package fd

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"mc714-t2/internal/config"
	"mc714-t2/internal/faults"
)

func testCluster(t *testing.T, node string) config.Cluster {
	t.Helper()
	return config.Cluster{
		NodeID: node, RunID: "fd-run", Mode: config.Standalone, ClusterID: "fd-cluster",
		Members: []config.Member{
			{ID: "node1", URL: "http://127.0.0.1:18081"},
			{ID: "node2", URL: "http://127.0.0.1:18082"},
			{ID: "node3", URL: "http://127.0.0.1:18083"},
		},
		Quorum: 2,
	}
}

func TestHeartbeatClearsSuspicion(t *testing.T) {
	cluster := testCluster(t, "node1")
	svc, err := NewService(cluster, faults.Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer svc.Close()
	svc.mu.Lock()
	svc.lastSeen["node2"] = time.Now().Add(-2 * time.Second)
	svc.mu.Unlock()
	if !svc.Suspected("node2") {
		t.Fatal("expected suspicion after silence")
	}
	mux := http.NewServeMux()
	svc.Register(mux)
	server := httptest.NewServer(mux)
	defer server.Close()
	body := `{"from":"node2","run_id":"fd-run","cluster_id":"fd-cluster","sequence":1,"sent_unix_ns":1}`
	resp, err := http.Post(server.URL+"/fd/heartbeat", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("status=%d", resp.StatusCode)
	}
	if svc.Suspected("node2") {
		t.Fatal("heartbeat should clear suspicion")
	}
	state := svc.Snapshot()
	if len(state.Peers) != 2 {
		t.Fatalf("peers=%d", len(state.Peers))
	}
}
