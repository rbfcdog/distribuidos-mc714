package abd_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"

	"mc714-t2/internal/abd"
	"mc714-t2/internal/config"
	"mc714-t2/internal/wire"
)

func storeTestCluster() config.Cluster {
	return config.Cluster{
		NodeID: "n1", RunID: "store-run", ClusterID: "store-cluster", Quorum: 2,
		Members: []config.Member{{ID: "n1"}, {ID: "n2"}, {ID: "n3"}},
	}
}

func storeTestOpen(t *testing.T, dir string, cluster config.Cluster) *abd.Store {
	t.Helper()
	store, err := abd.OpenStore(dir, cluster)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	return store
}

func storeTestRead(t *testing.T, store *abd.Store, want wire.Register) {
	t.Helper()
	got, err := store.Read()
	if err != nil || got != want {
		t.Fatalf("Read() = %+v, %v; want %+v", got, err, want)
	}
}

func TestStoreReopenRetainsHighestDurableRegister(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "run", "n1")
	cluster := storeTestCluster()
	store := storeTestOpen(t, dir, cluster)
	storeTestRead(t, store, wire.Register{})
	updates := []wire.Register{
		{Tag: wire.Tag{Counter: 1, WriterID: "n1"}, Value: 11},
		{Tag: wire.Tag{Counter: 1, WriterID: "n3"}, Value: 13},
		{Tag: wire.Tag{Counter: 2, WriterID: "n2"}, Value: -7},
	}
	for _, update := range updates {
		if got, err := store.Apply(update); err != nil || got != update {
			t.Fatalf("Apply(%+v) = %+v, %v", update, got, err)
		}
	}
	want := updates[len(updates)-1]
	if got, err := store.Apply(updates[0]); err != nil || got != want {
		t.Fatalf("stale Apply = %+v, %v; want %+v", got, err, want)
	}
	// Inspect the public on-disk schema as well as the subsequently reopened API.
	data, err := os.ReadFile(filepath.Join(dir, "register.json"))
	if err != nil {
		t.Fatal(err)
	}
	var snapshot struct {
		Version   int           `json:"version"`
		NodeID    string        `json:"node_id"`
		RunID     string        `json:"run_id"`
		ClusterID string        `json:"cluster_id"`
		Register  wire.Register `json:"register"`
	}
	if err := json.Unmarshal(data, &snapshot); err != nil {
		t.Fatal(err)
	}
	if snapshot.Version != 1 || snapshot.NodeID != cluster.NodeID || snapshot.RunID != cluster.RunID || snapshot.ClusterID != cluster.ClusterID || snapshot.Register != want {
		t.Fatalf("unexpected durable snapshot: %+v", snapshot)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened := storeTestOpen(t, dir, cluster)
	storeTestRead(t, reopened, want)
}

func TestStoreStaleDuplicateConflictAndValidation(t *testing.T) {
	dir := t.TempDir()
	store := storeTestOpen(t, dir, storeTestCluster())
	current := wire.Register{Tag: wire.Tag{Counter: 4, WriterID: "n2"}, Value: 0}
	if _, err := store.Apply(current); err != nil {
		t.Fatal(err)
	}
	before, err := os.Stat(filepath.Join(dir, "register.json"))
	if err != nil {
		t.Fatal(err)
	}
	for _, stale := range []wire.Register{
		{},
		{Tag: wire.Tag{Counter: 3, WriterID: "n3"}, Value: 100},
		{Tag: wire.Tag{Counter: 4, WriterID: "n1"}, Value: -100},
		current,
	} {
		if got, err := store.Apply(stale); err != nil || got != current {
			t.Fatalf("stale/duplicate Apply(%+v) = %+v, %v", stale, got, err)
		}
	}
	conflict := current
	conflict.Value = 1
	if _, err := store.Apply(conflict); !errors.Is(err, abd.ErrConflict) {
		t.Fatalf("conflicting tag error = %v", err)
	}
	for _, invalid := range []wire.Register{
		{Value: 1},
		{Tag: wire.Tag{WriterID: "n1"}},
		{Tag: wire.Tag{Counter: 5, WriterID: "outsider"}},
	} {
		if _, err := store.Apply(invalid); err == nil || errors.Is(err, abd.ErrUnavailable) {
			t.Fatalf("invalid Apply(%+v) error = %v", invalid, err)
		}
	}
	after, err := os.Stat(filepath.Join(dir, "register.json"))
	if err != nil {
		t.Fatal(err)
	}
	if !os.SameFile(before, after) {
		t.Fatal("stale, duplicate, conflicting or invalid update rewrote the snapshot")
	}
	storeTestRead(t, store, current)
	next := wire.Register{Tag: wire.Tag{Counter: 4, WriterID: "n3"}, Value: -3}
	if got, err := store.Apply(next); err != nil || got != next {
		t.Fatalf("valid update after validation/conflict error = %+v, %v", got, err)
	}
}

func TestStoreIndependentReplicaDirectories(t *testing.T) {
	root := t.TempDir()
	cluster1 := storeTestCluster()
	cluster2 := storeTestCluster()
	cluster2.NodeID = "n2"
	first := storeTestOpen(t, filepath.Join(root, "n1"), cluster1)
	second := storeTestOpen(t, filepath.Join(root, "n2"), cluster2)
	update := wire.Register{Tag: wire.Tag{Counter: 1, WriterID: "n1"}, Value: 22}
	if _, err := first.Apply(update); err != nil {
		t.Fatal(err)
	}
	storeTestRead(t, second, wire.Register{})
	if _, err := second.Apply(update); err != nil {
		t.Fatal(err)
	}
	newer := wire.Register{Tag: wire.Tag{Counter: 2, WriterID: "n2"}, Value: 33}
	if _, err := second.Apply(newer); err != nil {
		t.Fatal(err)
	}
	storeTestRead(t, first, update)
	storeTestRead(t, second, newer)
}

func TestStoreRejectsCorruptAndMismatchedSnapshotsWithoutReset(t *testing.T) {
	valid := `{"version":1,"node_id":"n1","run_id":"store-run","cluster_id":"store-cluster","register":{"tag":{"counter":1,"writer_id":"n2"},"value":17}}`
	cases := map[string]string{
		"truncated":              `{`,
		"array":                  `[]`,
		"null":                   `null`,
		"missing version":        strings.Replace(valid, `"version":1,`, "", 1),
		"unknown version":        strings.Replace(valid, `"version":1`, `"version":2`, 1),
		"null version":           strings.Replace(valid, `"version":1`, `"version":null`, 1),
		"missing node":           strings.Replace(valid, `"node_id":"n1",`, "", 1),
		"empty node":             strings.Replace(valid, `"node_id":"n1"`, `"node_id":""`, 1),
		"wrong node":             strings.Replace(valid, `"node_id":"n1"`, `"node_id":"n3"`, 1),
		"null node":              strings.Replace(valid, `"node_id":"n1"`, `"node_id":null`, 1),
		"missing run":            strings.Replace(valid, `"run_id":"store-run",`, "", 1),
		"wrong run":              strings.Replace(valid, `"run_id":"store-run"`, `"run_id":"other"`, 1),
		"missing cluster":        strings.Replace(valid, `"cluster_id":"store-cluster",`, "", 1),
		"wrong cluster":          strings.Replace(valid, `"cluster_id":"store-cluster"`, `"cluster_id":"other"`, 1),
		"missing register":       `{"version":1,"node_id":"n1","run_id":"store-run","cluster_id":"store-cluster"}`,
		"null register":          `{"version":1,"node_id":"n1","run_id":"store-run","cluster_id":"store-cluster","register":null}`,
		"unknown field":          strings.Replace(valid, `{`, `{"extra":0,`, 1),
		"duplicate field":        strings.Replace(valid, `{`, `{"node_id":"n1",`, 1),
		"case alias":             strings.Replace(valid, `"node_id"`, `"Node_ID"`, 1),
		"trailing object":        valid + `{}`,
		"oversized":              valid + strings.Repeat(" ", (16<<10)-len(valid)+1),
		"missing tag":            strings.Replace(valid, `"tag":{"counter":1,"writer_id":"n2"},`, "", 1),
		"missing counter":        strings.Replace(valid, `"counter":1,`, "", 1),
		"missing writer":         strings.Replace(valid, `,"writer_id":"n2"`, "", 1),
		"null tag":               strings.Replace(valid, `{"counter":1,"writer_id":"n2"}`, `null`, 1),
		"null value":             strings.Replace(valid, `"value":17`, `"value":null`, 1),
		"missing value":          strings.Replace(valid, `,"value":17`, "", 1),
		"unknown writer":         strings.Replace(valid, `"writer_id":"n2"`, `"writer_id":"outsider"`, 1),
		"invalid initial tag":    strings.Replace(valid, `"counter":1`, `"counter":0`, 1),
		"unknown register field": strings.Replace(valid, `"value":17`, `"value":17,"extra":0`, 1),
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "register.json")
			if err := os.WriteFile(path, []byte(body), 0600); err != nil {
				t.Fatal(err)
			}
			for range 2 {
				if store, err := abd.OpenStore(dir, storeTestCluster()); err == nil {
					store.Close()
					t.Fatal("corrupt or mismatched state was accepted")
				}
				if _, err := os.Lstat(filepath.Join(dir, ".lock")); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("failed open left its lock: %v", err)
				}
			}
			got, err := os.ReadFile(path)
			if err != nil || !bytes.Equal(got, []byte(body)) {
				t.Fatalf("failed open altered persisted state: %q, %v", got, err)
			}
		})
	}
}

func TestStoreRenameFailurePermanentlyPoisonsReadsAndWrites(t *testing.T) {
	dir := t.TempDir()
	store := storeTestOpen(t, dir, storeTestCluster())
	old := wire.Register{Tag: wire.Tag{Counter: 1, WriterID: "n1"}, Value: 8}
	if _, err := store.Apply(old); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "register.json")
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	// Replacing a directory with a regular file fails even for root; chmod does not.
	if err := os.Mkdir(path, 0700); err != nil {
		t.Fatal(err)
	}
	newer := wire.Register{Tag: wire.Tag{Counter: 2, WriterID: "n2"}, Value: 9}
	if got, err := store.Apply(newer); !errors.Is(err, abd.ErrUnavailable) || got == newer {
		t.Fatalf("failed rename acknowledged update: %+v, %v", got, err)
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	// Repairing the external obstacle must not resurrect a poisoned instance.
	if _, err := store.Read(); !errors.Is(err, abd.ErrUnavailable) {
		t.Fatalf("Read after failed rename = %v", err)
	}
	for _, update := range []wire.Register{old, newer, {}} {
		if _, err := store.Apply(update); !errors.Is(err, abd.ErrUnavailable) {
			t.Fatalf("Apply after failed rename = %v", err)
		}
	}
	if err := store.MarkStarted(); !errors.Is(err, abd.ErrUnavailable) {
		t.Fatalf("MarkStarted after failed rename = %v", err)
	}
	if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("poisoned store recreated snapshot: %v", err)
	}
	temps, err := filepath.Glob(filepath.Join(dir, ".register-*"))
	if err != nil || len(temps) != 0 {
		t.Fatalf("failed replace left temporary snapshots: %v, %v", temps, err)
	}
}

func TestStoreExclusiveLockAndPersistentStartedMarker(t *testing.T) {
	dir := t.TempDir()
	cluster := storeTestCluster()
	store := storeTestOpen(t, dir, cluster)
	lockPath := filepath.Join(dir, ".lock")
	before, err := os.Stat(lockPath)
	if err != nil {
		t.Fatal(err)
	}
	if duplicate, err := abd.OpenStore(dir, cluster); !errors.Is(err, abd.ErrUnavailable) {
		if duplicate != nil {
			duplicate.Close()
		}
		t.Fatalf("second owner acquired store: %v", err)
	}
	after, err := os.Stat(lockPath)
	if err != nil || !os.SameFile(before, after) {
		t.Fatalf("failed competing open changed owner's lock: %v", err)
	}
	if err := store.MarkStarted(); err != nil {
		t.Fatal(err)
	}
	if err := store.MarkStarted(); !errors.Is(err, abd.ErrAlreadyStarted) {
		t.Fatalf("same-instance restart accepted: %v", err)
	}
	storeTestRead(t, store, wire.Register{})
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Read(); !errors.Is(err, abd.ErrUnavailable) {
		t.Fatalf("Read after Close = %v", err)
	}
	if _, err := store.Apply(wire.Register{}); !errors.Is(err, abd.ErrUnavailable) {
		t.Fatalf("Apply after Close = %v", err)
	}
	reopened := storeTestOpen(t, dir, cluster)
	storeTestRead(t, reopened, wire.Register{})
	if err := reopened.MarkStarted(); !errors.Is(err, abd.ErrAlreadyStarted) {
		t.Fatalf("same-run service restart accepted: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("idempotent Close = %v", err)
	}
	// A second Close on the old instance must not remove the new owner's lock.
	if duplicate, err := abd.OpenStore(dir, cluster); err == nil {
		duplicate.Close()
		t.Fatal("old Close removed reopened owner's lock")
	}
}

func TestStoreCloseDoesNotRemoveReplacementLock(t *testing.T) {
	dir := t.TempDir()
	store, err := abd.OpenStore(dir, storeTestCluster())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	path := filepath.Join(dir, ".lock")
	if err := os.Remove(path); err != nil {
		if runtime.GOOS == "windows" {
			t.Skip("windows cannot replace an open lock file inode")
		}
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("another owner"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); !errors.Is(err, abd.ErrUnavailable) {
		t.Fatalf("changed lock ownership was not reported: %v", err)
	}
	data, err := os.ReadFile(path)
	if err != nil || string(data) != "another owner" {
		t.Fatalf("Close deleted/replaced another owner's lock: %q, %v", data, err)
	}
}

func TestStoreDoesNotRecoverCrashLockOrMissingStartedSnapshot(t *testing.T) {
	t.Run("crash lock", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, ".lock")
		if err := os.WriteFile(path, []byte("crashed owner"), 0600); err != nil {
			t.Fatal(err)
		}
		if store, err := abd.OpenStore(dir, storeTestCluster()); err == nil {
			store.Close()
			t.Fatal("crash lock was silently recovered")
		}
		data, err := os.ReadFile(path)
		if err != nil || string(data) != "crashed owner" {
			t.Fatalf("existing lock changed: %q, %v", data, err)
		}
		if _, err := os.Stat(filepath.Join(dir, "register.json")); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("competing open initialized state: %v", err)
		}
	})
	t.Run("missing started snapshot", func(t *testing.T) {
		dir := t.TempDir()
		store := storeTestOpen(t, dir, storeTestCluster())
		if err := store.MarkStarted(); err != nil {
			t.Fatal(err)
		}
		if err := store.Close(); err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(dir, "register.json")
		if err := os.Remove(path); err != nil {
			t.Fatal(err)
		}
		if reopened, err := abd.OpenStore(dir, storeTestCluster()); err == nil {
			reopened.Close()
			t.Fatal("missing previously started state reset to zero")
		}
		if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("failed open reset missing state: %v", err)
		}
	})
}

func TestStoreConcurrentUpdatesRetainMaximum(t *testing.T) {
	store := storeTestOpen(t, t.TempDir(), storeTestCluster())
	const count = 24
	var workers sync.WaitGroup
	failures := make(chan error, count)
	for counter := range uint64(count) {
		workers.Add(1)
		go func() {
			defer workers.Done()
			update := wire.Register{Tag: wire.Tag{Counter: counter + 1, WriterID: "n2"}, Value: int64(counter + 1)}
			got, err := store.Apply(update)
			if err != nil || got.Tag.Compare(update.Tag) < 0 {
				failures <- fmt.Errorf("Apply(%+v) = %+v, %v", update, got, err)
			}
		}()
	}
	workers.Wait()
	close(failures)
	for err := range failures {
		t.Error(err)
	}
	storeTestRead(t, store, wire.Register{Tag: wire.Tag{Counter: count, WriterID: "n2"}, Value: count})
}
