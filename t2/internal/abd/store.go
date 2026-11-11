// Package abd implements a durable multiwriter atomic register.
package abd

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sync"

	"mc714-t2/internal/config"
	"mc714-t2/internal/wire"
)

var (
	ErrConflict       = errors.New("register tag has conflicting values")
	ErrUnavailable    = errors.New("register store unavailable")
	ErrAlreadyStarted = errors.New("register run already started")
)

// Store owns one replica directory until Close. A failed durable write makes
// this instance unusable even when its previous in-memory value is intact.
type Store struct {
	mu       sync.Mutex
	dir      string
	cluster  config.Cluster
	register wire.Register
	lock     *os.File
	lockInfo os.FileInfo
	closed   bool
	failure  error
}

type storedRegister struct {
	Version   int           `json:"version"`
	NodeID    string        `json:"node_id"`
	RunID     string        `json:"run_id"`
	ClusterID string        `json:"cluster_id"`
	Register  wire.Register `json:"register"`
}

// OpenOptions controls crash rejoin behavior for a replica directory.
type OpenOptions struct {
	// AllowRejoin removes a leftover .lock from a dead process and permits
	// opening a previously started identity. The operator must ensure no other
	// process still serves that directory.
	AllowRejoin bool
}

// OpenStore exclusively opens a replica, initializing only an absent snapshot.
// A leftover lock deliberately prevents crash recovery within the same run.
func OpenStore(dir string, cluster config.Cluster) (*Store, error) {
	return OpenStoreOpts(dir, cluster, OpenOptions{})
}

// OpenStoreOpts opens a replica with optional rejoin after crash-stop.
func OpenStoreOpts(dir string, cluster config.Cluster, opts OpenOptions) (_ *Store, err error) {
	if cluster.NodeID == "" || cluster.RunID == "" || cluster.ClusterID == "" || !cluster.HasMember(cluster.NodeID) {
		return nil, errors.New("store requires a nonempty cluster identity and member node")
	}
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, fmt.Errorf("%w: create store directory: %w", ErrUnavailable, err)
	}
	lockPath := filepath.Join(dir, ".lock")
	if opts.AllowRejoin {
		if removeErr := os.Remove(lockPath); removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
			return nil, fmt.Errorf("%w: clear stale store lock: %w", ErrUnavailable, removeErr)
		}
	}
	lock, err := os.OpenFile(lockPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return nil, fmt.Errorf("%w: acquire store lock: %w", ErrUnavailable, err)
	}
	s := &Store{dir: dir, cluster: cluster, lock: lock}
	defer func() {
		if err != nil {
			err = errors.Join(err, s.Close())
		}
	}()
	s.lockInfo, err = lock.Stat()
	if err != nil {
		return nil, fmt.Errorf("%w: stat store lock: %w", ErrUnavailable, err)
	}
	if err = lock.Sync(); err != nil {
		return nil, fmt.Errorf("%w: sync store lock: %w", ErrUnavailable, err)
	}
	if err = s.syncDirectory(); err != nil {
		return nil, fmt.Errorf("%w: persist store lock: %w", ErrUnavailable, err)
	}
	snapshot, err := os.Open(filepath.Join(dir, "register.json"))
	var data []byte
	if err == nil {
		data, err = io.ReadAll(io.LimitReader(snapshot, (16<<10)+1))
		err = errors.Join(err, snapshot.Close())
		if err == nil && len(data) > 16<<10 {
			err = errors.New("snapshot exceeds 16 KiB")
		}
	}
	if errors.Is(err, os.ErrNotExist) {
		if _, statErr := os.Lstat(filepath.Join(dir, "register.json")); !errors.Is(statErr, os.ErrNotExist) {
			return nil, fmt.Errorf("%w: unreadable register path: %v", ErrUnavailable, statErr)
		}
		if _, markerErr := os.Lstat(filepath.Join(dir, "started")); !errors.Is(markerErr, os.ErrNotExist) {
			return nil, fmt.Errorf("%w: missing snapshot in a previously started or unreadable store: %v", ErrUnavailable, markerErr)
		}
		if err = s.persist(wire.Register{}); err != nil {
			return nil, fmt.Errorf("%w: initialize register: %w", ErrUnavailable, err)
		}
		return s, nil
	}
	if err != nil {
		return nil, fmt.Errorf("%w: read register: %w", ErrUnavailable, err)
	}
	s.register, err = decodeStoredRegister(data, cluster)
	if err != nil {
		return nil, fmt.Errorf("decode persisted register: %w", err)
	}
	return s, nil
}

func (s *Store) Read() (wire.Register, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.unavailable(); err != nil {
		return wire.Register{}, err
	}
	return s.register, nil
}

// Apply never publishes an advancing tag before both the snapshot and its
// directory entry are synced. Stale and identical updates do not touch disk.
func (s *Store) Apply(register wire.Register) (wire.Register, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.unavailable(); err != nil {
		return wire.Register{}, err
	}
	if err := wire.ValidateRegister(register, s.cluster); err != nil {
		return wire.Register{}, err
	}
	switch register.Tag.Compare(s.register.Tag) {
	case -1:
		return s.register, nil
	case 0:
		if register.Value != s.register.Value {
			return wire.Register{}, ErrConflict
		}
		return s.register, nil
	}
	if err := s.persist(register); err != nil {
		return wire.Register{}, s.poison(err)
	}
	s.register = register
	return s.register, nil
}

// MarkStarted permanently claims the protocol identity, independently of the
// initialization lock. Close never removes this marker.
func (s *Store) MarkStarted() error {
	_, err := s.markStarted(false)
	return err
}

// MarkStartedOrRejoin claims the identity, or accepts an existing marker when
// rejoining after crash-stop under AllowRejoin.
func (s *Store) MarkStartedOrRejoin(allowRejoin bool) (bool, error) {
	return s.markStarted(allowRejoin)
}

func (s *Store) markStarted(allowRejoin bool) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.unavailable(); err != nil {
		return false, err
	}
	file, err := os.OpenFile(filepath.Join(s.dir, "started"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if errors.Is(err, os.ErrExist) {
		if allowRejoin {
			return true, nil
		}
		return false, ErrAlreadyStarted
	}
	if err != nil {
		return false, s.poison(fmt.Errorf("create started marker: %w", err))
	}
	err = errors.Join(file.Sync(), file.Close())
	if err != nil {
		return false, s.poison(fmt.Errorf("sync started marker: %w", err))
	}
	if err := s.syncDirectory(); err != nil {
		return false, s.poison(fmt.Errorf("persist started marker: %w", err))
	}
	return false, nil
}

// Close permits inspection/reopen but never permits serving a started run again
// unless a later OpenStoreOpts uses AllowRejoin. Only the inode acquired by this
// instance may be unlinked as its lock.
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	lockPath := filepath.Join(s.dir, ".lock")
	info, err := os.Lstat(lockPath)
	owned := err == nil && s.lockInfo != nil && os.SameFile(s.lockInfo, info)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		closeErr := s.lock.Close()
		return fmt.Errorf("%w: close store: %w", ErrUnavailable, errors.Join(err, closeErr))
	}
	if err == nil && !owned {
		closeErr := s.lock.Close()
		return fmt.Errorf("%w: close store: %w", ErrUnavailable, errors.Join(errors.New("store lock ownership changed"), closeErr))
	}
	// Close before unlink: Windows cannot remove an open file.
	closeErr := s.lock.Close()
	s.lock = nil
	var removeErr error
	if owned {
		removeErr = os.Remove(lockPath)
		if removeErr == nil {
			removeErr = s.syncDirectory()
		}
	}
	if err := errors.Join(closeErr, removeErr); err != nil {
		return fmt.Errorf("%w: close store: %w", ErrUnavailable, err)
	}
	return nil
}

func (s *Store) unavailable() error {
	if s.closed {
		return fmt.Errorf("%w: store closed", ErrUnavailable)
	}
	return s.failure
}

func (s *Store) poison(err error) error {
	s.failure = fmt.Errorf("%w: %w", ErrUnavailable, err)
	return s.failure
}

func (s *Store) persist(register wire.Register) (err error) {
	file, err := os.CreateTemp(s.dir, ".register-*")
	if err != nil {
		return fmt.Errorf("create register snapshot: %w", err)
	}
	fileName := file.Name()
	defer func() {
		if file != nil {
			err = errors.Join(err, file.Close())
		}
		if removeErr := os.Remove(fileName); removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
			err = errors.Join(err, removeErr)
		}
	}()
	snapshot := storedRegister{Version: 1, NodeID: s.cluster.NodeID, RunID: s.cluster.RunID, ClusterID: s.cluster.ClusterID, Register: register}
	if err := json.NewEncoder(file).Encode(snapshot); err != nil {
		return fmt.Errorf("encode register snapshot: %w", err)
	}
	if err := file.Sync(); err != nil {
		return fmt.Errorf("sync register snapshot: %w", err)
	}
	closeErr := file.Close()
	file = nil
	if closeErr != nil {
		return fmt.Errorf("close register snapshot: %w", closeErr)
	}
	if err := os.Rename(fileName, filepath.Join(s.dir, "register.json")); err != nil {
		return fmt.Errorf("replace register snapshot: %w", err)
	}
	if err := s.syncDirectory(); err != nil {
		return fmt.Errorf("sync register directory: %w", err)
	}
	return nil
}

func (s *Store) syncDirectory() error {
	dir, err := os.Open(s.dir)
	if err != nil {
		return err
	}
	syncErr := dir.Sync()
	closeErr := dir.Close()
	// Windows returns ERROR_INVALID_FUNCTION / access denied for directory
	// Sync; rename durability still depends on the file Sync above.
	if syncErr != nil && runtime.GOOS == "windows" {
		syncErr = nil
	}
	return errors.Join(syncErr, closeErr)
}

func decodeStoredRegister(data []byte, cluster config.Cluster) (wire.Register, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	token, err := decoder.Token()
	if err != nil || token != json.Delim('{') {
		return wire.Register{}, errors.New("snapshot must be an object")
	}
	var snapshot storedRegister
	seen := make(map[string]bool, 5)
	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return wire.Register{}, err
		}
		key, ok := token.(string)
		if !ok || seen[key] {
			return wire.Register{}, fmt.Errorf("invalid or duplicate snapshot field %v", token)
		}
		seen[key] = true
		var raw json.RawMessage
		if err := decoder.Decode(&raw); err != nil {
			return wire.Register{}, err
		}
		if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
			return wire.Register{}, fmt.Errorf("null snapshot field %q", key)
		}
		switch key {
		case "version":
			err = json.Unmarshal(raw, &snapshot.Version)
		case "node_id":
			err = json.Unmarshal(raw, &snapshot.NodeID)
		case "run_id":
			err = json.Unmarshal(raw, &snapshot.RunID)
		case "cluster_id":
			err = json.Unmarshal(raw, &snapshot.ClusterID)
		case "register":
			err = snapshot.Register.UnmarshalJSON(raw)
		default:
			return wire.Register{}, fmt.Errorf("unknown snapshot field %q", key)
		}
		if err != nil {
			return wire.Register{}, err
		}
	}
	if token, err = decoder.Token(); err != nil || token != json.Delim('}') {
		return wire.Register{}, errors.New("unterminated register snapshot")
	}
	if _, err := decoder.Token(); err != io.EOF {
		return wire.Register{}, errors.New("trailing register snapshot data")
	}
	if len(seen) != 5 || snapshot.Version != 1 || snapshot.NodeID != cluster.NodeID || snapshot.RunID != cluster.RunID || snapshot.ClusterID != cluster.ClusterID {
		return wire.Register{}, errors.New("missing, unsupported or mismatched snapshot provenance")
	}
	if err := wire.ValidateRegister(snapshot.Register, cluster); err != nil {
		return wire.Register{}, err
	}
	return snapshot.Register, nil
}
