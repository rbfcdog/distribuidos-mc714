package ra

import (
	"errors"
	"math"
	"sync"
	"testing"

	"mc714-t2/internal/config"
	"mc714-t2/internal/wire"
)

func engineCluster(node string) config.Cluster {
	return config.Cluster{
		NodeID: node, RunID: "run-1", ClusterID: "cluster-1", Quorum: 2,
		Members: []config.Member{{ID: "node1"}, {ID: "node2"}, {ID: "node3"}},
	}
}

func engineMessage(from, to string, kind wire.Kind, sequence, id, clock uint64) wire.Message {
	owner := from
	if kind == wire.RAReply {
		owner = to
	}
	return wire.Message{
		Version: wire.ProtocolVersion, RunID: "run-1", ClusterID: "cluster-1",
		From: from, To: to, Kind: kind, MessageID: id, Clock: clock,
		Request: wire.RequestID{Owner: owner, Sequence: sequence},
	}
}

func beginEngine(t *testing.T, e *Engine) Attempt {
	t.Helper()
	attempt, err := e.Begin()
	if err != nil {
		t.Fatal(err)
	}
	return attempt
}

func handleEngine(t *testing.T, e *Engine, message wire.Message) []wire.Message {
	t.Helper()
	messages, err := e.Handle(message)
	if err != nil {
		t.Fatal(err)
	}
	return messages
}

func assertEngineReady(t *testing.T, attempt Attempt, ready bool) {
	t.Helper()
	select {
	case <-attempt.Ready:
		if !ready {
			t.Fatal("Ready closed without all peer permissions")
		}
	default:
		if ready {
			t.Fatal("Ready did not close after all peer permissions")
		}
	}
}

// All three priorities are equal; only node ID can break the tie. The driver
// advances messages explicitly, so neither acquisition nor exclusion uses time.
func TestEngineEqualClockMutualExclusion(t *testing.T) {
	ids := []string{"node1", "node2", "node3"}
	engines := make(map[string]*Engine)
	attempts := make(map[string]Attempt)
	var pending []wire.Message
	for _, id := range ids {
		engines[id] = NewEngine(engineCluster(id))
		attempts[id] = beginEngine(t, engines[id])
		messages := attempts[id].Messages
		if len(messages) != 2 || messages[0].Clock != messages[1].Clock || messages[0].MessageID == messages[1].MessageID {
			t.Fatalf("invalid multicast frames: %+v", messages)
		}
		pending = append(pending, messages...)
	}
	drain := func() {
		for len(pending) != 0 {
			message := pending[0]
			pending = pending[1:]
			pending = append(pending, handleEngine(t, engines[message.To], message)...)
		}
	}
	drain()
	for index, id := range ids {
		for _, other := range ids[index:] {
			assertEngineReady(t, attempts[other], other == id)
			if other != id && engines[other].Enter(attempts[other].Request) == nil {
				t.Fatal("peer entered without all permissions")
			}
		}
		if err := engines[id].Enter(attempts[id].Request); err != nil {
			t.Fatal(err)
		}
		heldCount := 0
		for _, engine := range engines {
			if engine.Snapshot().State == held {
				heldCount++
			}
		}
		if heldCount != 1 {
			t.Fatalf("critical section overlap: %d holders", heldCount)
		}
		if _, err := engines[id].Cancel(attempts[id].Request); !errors.Is(err, ErrBusy) {
			t.Fatalf("Cancel accepted HELD state: %v", err)
		}
		messages, err := engines[id].Release(attempts[id].Request)
		if err != nil {
			t.Fatal(err)
		}
		pending = append(pending, messages...)
		drain()
	}
	for _, engine := range engines {
		if snapshot := engine.Snapshot(); snapshot.State != released || snapshot.Deferred != 0 || snapshot.Permissions != 0 {
			t.Fatalf("unfinished state: %+v", snapshot)
		}
	}
}

func TestEngineCancellationFlushAndImmutableRetry(t *testing.T) {
	e := NewEngine(engineCluster("node1"))
	attempt := beginEngine(t, e)
	originalRequest := attempt.Request
	attempt.Messages[0].Request.Sequence = 99
	if e.Snapshot().Request != originalRequest {
		t.Fatal("outbound request aliases active state")
	}
	request := engineMessage("node2", "node1", wire.RARequest, 1, 1, 1)
	for range 3 {
		if replies := handleEngine(t, e, request); len(replies) != 0 {
			t.Fatal("duplicate deferred request was granted")
		}
	}
	if snapshot := e.Snapshot(); snapshot.Deferred != 1 || snapshot.LogicalMessages != 2 {
		t.Fatalf("deferred duplicates changed counts: %+v", snapshot)
	}
	if _, err := e.Release(originalRequest); !errors.Is(err, ErrBusy) {
		t.Fatalf("Release accepted WANTED state: %v", err)
	}
	replies, err := e.Cancel(originalRequest)
	if err != nil || len(replies) != 1 || replies[0].Request != request.Request {
		t.Fatalf("Cancel did not flush peer request: %+v, %v", replies, err)
	}
	assertEngineReady(t, attempt, false)
	cached := replies[0]
	replies[0].Clock++
	replies[0].Request.Sequence++
	before := e.Snapshot()
	retried := handleEngine(t, e, request)
	if len(retried) != 1 || retried[0] != cached || e.Snapshot().LogicalMessages != before.LogicalMessages {
		t.Fatalf("retry did not retain immutable logical grant: %+v", retried)
	}
	if _, err := e.Cancel(originalRequest); !errors.Is(err, ErrStale) {
		t.Fatalf("canceled request remained active: %v", err)
	}
	next := beginEngine(t, e)
	if next.Request.Sequence != originalRequest.Sequence+1 {
		t.Fatal("cancellation reused request sequence")
	}
	if err := e.Enter(originalRequest); !errors.Is(err, ErrStale) {
		t.Fatalf("stale request entered: %v", err)
	}
	if snapshot := e.Snapshot(); snapshot.Deferred != 0 || snapshot.Permissions != 0 || snapshot.State != wanted {
		t.Fatalf("cancellation leaked local state: %+v", snapshot)
	}
}

func TestEngineDistinctPermissionsAndReorderedReply(t *testing.T) {
	e := NewEngine(engineCluster("node2"))
	old := beginEngine(t, e)
	oldReply := engineMessage("node1", "node2", wire.RAReply, old.Request.Sequence, 5, 2)
	handleEngine(t, e, oldReply)
	if _, err := e.Cancel(old.Request); err != nil {
		t.Fatal(err)
	}
	attempt := beginEngine(t, e)
	// This request was generated after the reply below, but arrived first.
	handleEngine(t, e, engineMessage("node1", "node2", wire.RARequest, 1, 100, 20))
	handleEngine(t, e, oldReply)
	if e.Snapshot().Permissions != 0 {
		t.Fatal("old reply granted permission to a new attempt")
	}
	reply := engineMessage("node1", "node2", wire.RAReply, attempt.Request.Sequence, 6, 5)
	for range 3 {
		handleEngine(t, e, reply)
		assertEngineReady(t, attempt, false)
	}
	if e.Snapshot().Permissions != 1 {
		t.Fatal("reordered reply lost or duplicate counted twice")
	}
	if e.Enter(attempt.Request) == nil {
		t.Fatal("entered with a quorum instead of all peers")
	}
	handleEngine(t, e, engineMessage("node3", "node2", wire.RAReply, attempt.Request.Sequence, 1, 2))
	assertEngineReady(t, attempt, true)
	if e.Snapshot().Permissions != 2 || e.Enter(attempt.Request) != nil {
		t.Fatal("all-peer permission did not permit entry")
	}
	handleEngine(t, e, reply)
	if e.Snapshot().Permissions != 2 {
		t.Fatal("duplicate HELD reply changed permission count")
	}
}

func TestEngineConflictingAndOlderRequests(t *testing.T) {
	e := NewEngine(engineCluster("node1"))
	request := engineMessage("node2", "node1", wire.RARequest, 1, 7, 2)
	grant := handleEngine(t, e, request)[0]
	for _, change := range []func(*wire.Message){
		func(m *wire.Message) { m.Clock++ },
		func(m *wire.Message) { m.MessageID++ },
	} {
		conflict := request
		change(&conflict)
		before := e.Snapshot()
		if _, err := e.Handle(conflict); err == nil {
			t.Fatal("conflicting request retry accepted")
		}
		if e.Snapshot() != before {
			t.Fatal("conflicting request partially changed state")
		}
	}
	if replies := handleEngine(t, e, request); len(replies) != 1 || replies[0] != grant {
		t.Fatal("exact duplicate lost cached grant")
	}
	attempt := beginEngine(t, e)
	newer := request
	newer.Request.Sequence++
	newer.MessageID++
	newer.Clock = 10
	if replies := handleEngine(t, e, newer); len(replies) != 0 {
		t.Fatal("newer lower-priority request was not deferred")
	}
	newest := newer
	newest.Request.Sequence++
	newest.MessageID++
	newest.Clock++
	handleEngine(t, e, newest)
	if replies := handleEngine(t, e, request); len(replies) != 0 {
		t.Fatal("older request was granted after replacement")
	}
	if e.Snapshot().Deferred != 1 {
		t.Fatal("more than one deferred request retained per peer")
	}
	replies, err := e.Cancel(attempt.Request)
	if err != nil || len(replies) != 1 || replies[0].Request != newest.Request {
		t.Fatalf("flush did not use latest request: %+v, %v", replies, err)
	}
}

func TestEngineCounterOverflowIsAtomic(t *testing.T) {
	for _, test := range []struct {
		name string
		set  func(*Engine)
	}{
		{"clock", func(e *Engine) { e.clock = math.MaxUint64 }},
		{"sequence", func(e *Engine) { e.sequence = math.MaxUint64 }},
		{"message IDs batch", func(e *Engine) { e.messageID = math.MaxUint64 - 1 }},
		{"logical messages batch", func(e *Engine) { e.logicalMessages = math.MaxUint64 - 1 }},
	} {
		t.Run("Begin/"+test.name, func(t *testing.T) {
			e := NewEngine(engineCluster("node1"))
			test.set(e)
			before := e.Snapshot()
			sequence, id := e.sequence, e.messageID
			if _, err := e.Begin(); !errors.Is(err, ErrClockExhausted) {
				t.Fatalf("expected counter exhaustion, got %v", err)
			}
			if e.Snapshot() != before || e.sequence != sequence || e.messageID != id {
				t.Fatal("failed Begin partially allocated an attempt")
			}
		})
	}
	for _, test := range []struct {
		name string
		set  func(*Engine, *wire.Message)
	}{
		{"receive", func(_ *Engine, m *wire.Message) { m.Clock = math.MaxUint64 }},
		{"send", func(_ *Engine, m *wire.Message) { m.Clock = math.MaxUint64 - 1 }},
		{"message ID", func(e *Engine, _ *wire.Message) { e.messageID = math.MaxUint64 }},
		{"logical messages", func(e *Engine, _ *wire.Message) { e.logicalMessages = math.MaxUint64 }},
	} {
		t.Run("Handle/"+test.name, func(t *testing.T) {
			e := NewEngine(engineCluster("node1"))
			request := engineMessage("node2", "node1", wire.RARequest, 1, 1, 1)
			test.set(e, &request)
			before := e.Snapshot()
			if _, err := e.Handle(request); !errors.Is(err, ErrClockExhausted) {
				t.Fatalf("expected counter exhaustion, got %v", err)
			}
			if e.Snapshot() != before || e.peers["node2"].request.Request.Sequence != 0 {
				t.Fatal("failed Handle partially recorded a request")
			}
		})
	}
	for _, state := range []string{wanted, held} {
		for _, counter := range []string{"clock", "message ID", "logical messages"} {
			t.Run("flush/"+state+"/"+counter, func(t *testing.T) {
				e := NewEngine(engineCluster("node1"))
				attempt := beginEngine(t, e)
				for _, peer := range []string{"node2", "node3"} {
					handleEngine(t, e, engineMessage(peer, "node1", wire.RARequest, 1, 1, 1))
					if state == held {
						handleEngine(t, e, engineMessage(peer, "node1", wire.RAReply, attempt.Request.Sequence, 2, 2))
					}
				}
				if state == held {
					if err := e.Enter(attempt.Request); err != nil {
						t.Fatal(err)
					}
				}
				switch counter {
				case "clock":
					e.clock = math.MaxUint64 - 1
				case "message ID":
					e.messageID = math.MaxUint64 - 1
				case "logical messages":
					e.logicalMessages = math.MaxUint64 - 1
				}
				before := e.Snapshot()
				var messages []wire.Message
				var err error
				if state == held {
					messages, err = e.Release(attempt.Request)
				} else {
					messages, err = e.Cancel(attempt.Request)
				}
				if !errors.Is(err, ErrClockExhausted) || len(messages) != 0 || e.Snapshot() != before {
					t.Fatalf("flush was not atomic and fail-closed: %+v, %v", e.Snapshot(), err)
				}
				for _, peer := range e.peers {
					if !peer.deferred || peer.reply.MessageID != 0 {
						t.Fatal("failed flush partially granted a deferred request")
					}
				}
			})
		}
	}
}

func TestEngineConcurrentLocalAndDuplicateOperations(t *testing.T) {
	e := NewEngine(engineCluster("node1"))
	const contenders = 24
	var workers sync.WaitGroup
	results := make(chan error, contenders)
	attempts := make(chan Attempt, contenders)
	for range contenders {
		workers.Add(1)
		go func() {
			defer workers.Done()
			attempt, err := e.Begin()
			results <- err
			if err == nil {
				attempts <- attempt
			}
		}()
	}
	workers.Wait()
	close(results)
	successes := 0
	for err := range results {
		if err == nil {
			successes++
		} else if !errors.Is(err, ErrBusy) {
			t.Fatal(err)
		}
	}
	if successes != 1 {
		t.Fatalf("concurrent Begin admitted %d operations", successes)
	}
	attempt := <-attempts
	for range contenders {
		workers.Add(1)
		go func() {
			defer workers.Done()
			_, err := e.Handle(engineMessage("node2", "node1", wire.RAReply, attempt.Request.Sequence, 1, 2))
			if err != nil {
				t.Error(err)
			}
			_ = e.Snapshot()
		}()
	}
	workers.Wait()
	if e.Snapshot().Permissions != 1 {
		t.Fatal("concurrent duplicates counted as multiple permissions")
	}
	assertEngineReady(t, attempt, false)
}
