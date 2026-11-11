package abd_test

import (
	"os"
	"path/filepath"
	"testing"

	"mc714-t2/internal/abd"
	"mc714-t2/internal/config"
	"mc714-t2/internal/wire"
)

func TestRejoinClearsStaleLockAndAcceptsStartedMarker(t *testing.T) {
	dir := t.TempDir()
	cluster := config.Cluster{
		NodeID: "n1", RunID: "run-rejoin", ClusterID: "cluster-rejoin", Quorum: 2,
		Members: []config.Member{{ID: "n1"}, {ID: "n2"}, {ID: "n3"}},
	}
	first, err := abd.OpenStore(dir, cluster)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := first.Apply(wire.Register{Tag: wire.Tag{Counter: 1, WriterID: "n1"}, Value: 7}); err != nil {
		t.Fatal(err)
	}
	if err := first.MarkStarted(); err != nil {
		t.Fatal(err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, ".lock"), []byte("stale"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := abd.OpenStore(dir, cluster); err == nil {
		t.Fatal("expected default open to reject leftover lock")
	}
	second, err := abd.OpenStoreOpts(dir, cluster, abd.OpenOptions{AllowRejoin: true})
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	rejoined, err := second.MarkStartedOrRejoin(true)
	if err != nil {
		t.Fatal(err)
	}
	if !rejoined {
		t.Fatal("expected rejoined=true for existing started marker")
	}
	reg, err := second.Read()
	if err != nil {
		t.Fatal(err)
	}
	if reg.Value != 7 {
		t.Fatalf("value=%d", reg.Value)
	}
}
