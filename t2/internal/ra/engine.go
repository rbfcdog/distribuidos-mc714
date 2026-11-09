// Package ra implements Ricart–Agrawala mutual exclusion over static membership.
package ra

import (
	"errors"
	"fmt"
	"math"
	"sync"

	"mc714-t2/internal/config"
	"mc714-t2/internal/wire"
)

var (
	ErrBusy           = errors.New("a local request is already active")
	ErrStale          = errors.New("request is not the active local request")
	ErrClockExhausted = errors.New("protocol counter exhausted")
)

const (
	released = "RELEASED"
	wanted   = "WANTED"
	held     = "HELD"
)

type Attempt struct {
	Request  wire.RequestID
	Messages []wire.Message
	Ready    <-chan struct{}
}

type Snapshot struct {
	State           string         `json:"state"`
	Clock           uint64         `json:"clock"`
	Request         wire.RequestID `json:"request"`
	Permissions     int            `json:"permissions"`
	Deferred        int            `json:"deferred"`
	LogicalMessages uint64         `json:"logical_messages"`
}

// Each peer retains only its latest request and the immutable corresponding grant.
// Reply message IDs are not a receive watermark: unrelated frames can reorder.
type peerState struct {
	request    wire.Message
	reply      wire.Message
	deferred   bool
	permission bool
}

type Engine struct {
	mu              sync.Mutex
	nodeID          string
	runID           string
	clusterID       string
	peerIDs         []string
	peers           map[string]*peerState
	state           string
	clock           uint64
	sequence        uint64
	messageID       uint64
	logicalMessages uint64
	request         wire.RequestID
	requestClock    uint64
	permissions     int
	ready           chan struct{}
}

// NewEngine accepts validated, static cluster configuration and copies membership.
func NewEngine(cluster config.Cluster) *Engine {
	e := &Engine{
		nodeID:    cluster.NodeID,
		runID:     cluster.RunID,
		clusterID: cluster.ClusterID,
		state:     released,
		peers:     make(map[string]*peerState, len(cluster.Members)),
	}
	for _, member := range cluster.Members {
		if member.ID != cluster.NodeID {
			e.peerIDs = append(e.peerIDs, member.ID)
			e.peers[member.ID] = &peerState{}
		}
	}
	return e
}

func (e *Engine) Begin() (Attempt, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.state != released {
		return Attempt{}, ErrBusy
	}
	count := uint64(len(e.peerIDs))
	if e.sequence == math.MaxUint64 || e.clock == math.MaxUint64 || !e.canCreate(count) {
		return Attempt{}, ErrClockExhausted
	}
	priority := e.clock + 1
	e.clock = priority
	e.sequence++
	e.request = wire.RequestID{Owner: e.nodeID, Sequence: e.sequence}
	e.requestClock = priority
	e.state = wanted
	e.permissions = 0
	e.ready = make(chan struct{})
	messages := make([]wire.Message, 0, len(e.peerIDs))
	for _, id := range e.peerIDs {
		e.peers[id].permission = false
		messages = append(messages, e.frame(id, wire.RARequest, e.request, priority))
	}
	if len(e.peerIDs) == 0 {
		close(e.ready)
	}
	return Attempt{Request: e.request, Messages: messages, Ready: e.ready}, nil
}

func (e *Engine) Enter(request wire.RequestID) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.state == released || request != e.request {
		return ErrStale
	}
	if e.state != wanted {
		return ErrBusy
	}
	if e.permissions != len(e.peerIDs) {
		return errors.New("not all peers have granted permission")
	}
	e.state = held
	return nil
}

func (e *Engine) Cancel(request wire.RequestID) ([]wire.Message, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.finish(request, wanted)
}

func (e *Engine) Release(request wire.RequestID) ([]wire.Message, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.finish(request, held)
}

// finish reserves the entire flush before changing state or creating any frame.
func (e *Engine) finish(request wire.RequestID, expected string) ([]wire.Message, error) {
	if e.state == released || request != e.request {
		return nil, ErrStale
	}
	if e.state != expected {
		return nil, ErrBusy
	}
	count := uint64(0)
	for _, id := range e.peerIDs {
		if e.peers[id].deferred {
			count++
		}
	}
	if count > math.MaxUint64-e.clock || !e.canCreate(count) {
		return nil, ErrClockExhausted
	}
	messages := make([]wire.Message, 0, int(count))
	for _, id := range e.peerIDs {
		peer := e.peers[id]
		if peer.deferred {
			e.clock++
			peer.reply = e.frame(id, wire.RAReply, peer.request.Request, e.clock)
			peer.deferred = false
			messages = append(messages, peer.reply)
		}
		peer.permission = false
	}
	e.state = released
	e.request = wire.RequestID{}
	e.requestClock = 0
	e.permissions = 0
	e.ready = nil
	return messages, nil
}

// Handle consumes wire-validated RA frames. Returned values own no engine storage;
// callers can retain them for retries without another clock tick or message ID.
func (e *Engine) Handle(message wire.Message) ([]wire.Message, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	peer, known := e.peers[message.From]
	if !known || message.To != e.nodeID || message.RunID != e.runID || message.ClusterID != e.clusterID || message.Version != wire.ProtocolVersion {
		return nil, errors.New("message does not match engine membership or context")
	}
	if message.MessageID == 0 || message.Request.Sequence == 0 || message.Clock == 0 || message.Register != nil {
		return nil, errors.New("invalid RA frame")
	}
	if message.Kind != wire.RARequest && message.Kind != wire.RAReply {
		return nil, fmt.Errorf("unsupported RA message kind %q", message.Kind)
	}
	if (message.Kind == wire.RARequest && message.Request.Owner != message.From) || (message.Kind == wire.RAReply && message.Request.Owner != e.nodeID) {
		return nil, errors.New("message request owner does not match its kind")
	}
	duplicate := false
	older := false
	grant := false
	if message.Kind == wire.RARequest {
		older = message.Request.Sequence < peer.request.Request.Sequence
		duplicate = message.Request.Sequence == peer.request.Request.Sequence
		if duplicate && (message.Clock != peer.request.Clock || message.MessageID != peer.request.MessageID) {
			return nil, errors.New("conflicting retry of a peer request")
		}
		grant = !older && !duplicate && e.state != held && (e.state != wanted || message.Clock < e.requestClock || (message.Clock == e.requestClock && message.From < e.nodeID))
	}
	clock := max(e.clock, message.Clock)
	// Receiving needs one tick; a newly created grant needs a second send tick.
	ticks := uint64(1)
	if grant {
		ticks++
	}
	if ticks > math.MaxUint64-clock || (grant && !e.canCreate(1)) {
		return nil, ErrClockExhausted
	}
	e.clock = clock + 1
	if message.Kind == wire.RAReply {
		if e.state == wanted && message.Request == e.request && !peer.permission {
			peer.permission = true
			e.permissions++
			if e.permissions == len(e.peerIDs) {
				close(e.ready)
			}
		}
		return nil, nil
	}
	if older {
		return nil, nil
	}
	if duplicate {
		if peer.reply.MessageID != 0 {
			return []wire.Message{peer.reply}, nil
		}
		return nil, nil
	}
	peer.request = message
	peer.reply = wire.Message{}
	peer.deferred = !grant
	if !grant {
		return nil, nil
	}
	e.clock++
	peer.reply = e.frame(message.From, wire.RAReply, message.Request, e.clock)
	return []wire.Message{peer.reply}, nil
}

func (e *Engine) Snapshot() Snapshot {
	e.mu.Lock()
	defer e.mu.Unlock()
	deferred := 0
	for _, peer := range e.peers {
		if peer.deferred {
			deferred++
		}
	}
	return Snapshot{
		State: e.state, Clock: e.clock, Request: e.request,
		Permissions: e.permissions, Deferred: deferred,
		LogicalMessages: e.logicalMessages,
	}
}

func (e *Engine) canCreate(count uint64) bool {
	return count <= math.MaxUint64-e.messageID && count <= math.MaxUint64-e.logicalMessages
}

// frame is called only after the complete transition has reserved its counters.
func (e *Engine) frame(to string, kind wire.Kind, request wire.RequestID, clock uint64) wire.Message {
	e.messageID++
	e.logicalMessages++
	return wire.Message{
		Version: wire.ProtocolVersion, RunID: e.runID, ClusterID: e.clusterID,
		From: e.nodeID, To: to, MessageID: e.messageID,
		Kind: kind, Request: request, Clock: clock,
	}
}
