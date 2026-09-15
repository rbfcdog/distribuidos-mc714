// Package balancer selects a destination server for accepted requests.
package balancer

import (
	"fmt"
	"math"
	rand "math/rand/v2"
)

// Policy identifies a supported load-balancing policy.
type Policy string

const (
	Random        Policy = "random"
	RoundRobin    Policy = "round_robin"
	ShortestQueue Policy = "shortest_queue"
	LeastWork     Policy = "least_work"
)

// ServerState is the routing-visible state of one server. Backup servers are
// excluded from normal routing and are activated by the simulation engine only
// when a selected primary server cannot accept another request.
type ServerState struct {
	Active      int
	Queued      int
	Capacity    int
	ServiceTime float64
	Backup      bool
}

// Router owns the state needed by a single simulation run. It is deliberately
// not shared between runs, so independent trials cannot affect one another.
type Router struct {
	policy Policy
	rng    *rand.Rand
	next   int
}

// NewRouter creates a router for policy. rng is required only by Random.
func NewRouter(policy Policy, rng *rand.Rand) (*Router, error) {
	if !policy.Valid() {
		return nil, fmt.Errorf("unsupported balancing policy %q", policy)
	}
	if policy == Random && rng == nil {
		return nil, fmt.Errorf("random policy requires a random source")
	}
	return &Router{policy: policy, rng: rng}, nil
}

// Route returns the index of a primary server. ShortestQueue uses active plus
// queued requests. LeastWork additionally normalizes the next request's work by
// heterogeneous concurrency and service time.
func (r *Router) Route(states []ServerState) int {
	candidates := primaryIndexes(states)
	if len(candidates) == 0 {
		panic("route called with no primary servers")
	}

	switch r.policy {
	case Random:
		return candidates[r.rng.IntN(len(candidates))]
	case RoundRobin:
		selected := candidates[r.next%len(candidates)]
		r.next = (r.next + 1) % len(candidates)
		return selected
	case ShortestQueue:
		selected := candidates[0]
		for _, index := range candidates[1:] {
			if states[index].load() < states[selected].load() {
				selected = index
			}
		}
		return selected
	case LeastWork:
		selected := candidates[0]
		best := states[selected].normalizedWork()
		for _, index := range candidates[1:] {
			work := states[index].normalizedWork()
			if work < best {
				selected, best = index, work
			}
		}
		return selected
	default:
		panic("router constructed with invalid policy")
	}
}

// Valid reports whether policy is implemented.
func (p Policy) Valid() bool {
	switch p {
	case Random, RoundRobin, ShortestQueue, LeastWork:
		return true
	default:
		return false
	}
}

func primaryIndexes(states []ServerState) []int {
	indexes := make([]int, 0, len(states))
	for index := range states {
		if !states[index].Backup {
			indexes = append(indexes, index)
		}
	}
	return indexes
}

func (s ServerState) load() int {
	return s.Active + s.Queued
}

func (s ServerState) normalizedWork() float64 {
	if s.Capacity <= 0 || s.ServiceTime <= 0 {
		return math.Inf(1)
	}
	return float64(s.load()+1) * s.ServiceTime / float64(s.Capacity)
}
