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
	Random             Policy = "random"
	RoundRobin         Policy = "round_robin"
	WeightedRoundRobin Policy = "weighted_round_robin"
	ShortestQueue      Policy = "shortest_queue"
	LeastWork          Policy = "least_work"
	PowerOfTwo         Policy = "power_of_two"
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
	policy        Policy
	rng           *rand.Rand
	next          int
	currentWeight []float64
	candidates    []int
	topologySize  int
}

// NewRouter creates a router for policy. Stochastic policies require rng.
func NewRouter(policy Policy, rng *rand.Rand) (*Router, error) {
	if !policy.Valid() {
		return nil, fmt.Errorf("unsupported balancing policy %q", policy)
	}
	if (policy == Random || policy == PowerOfTwo) && rng == nil {
		return nil, fmt.Errorf("%s policy requires a random source", policy)
	}
	return &Router{policy: policy, rng: rng}, nil
}

// Route returns the index of a primary server. ShortestQueue uses active plus
// queued requests. Capacity-aware policies use each server's concurrency and
// service time. Backup servers never participate in normal routing.
func (r *Router) Route(states []ServerState) int {
	candidates := r.primaryIndexes(states)
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
	case WeightedRoundRobin:
		return r.routeWeighted(states, candidates)
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
	case PowerOfTwo:
		if len(candidates) == 1 {
			return candidates[0]
		}
		firstPosition := r.rng.IntN(len(candidates))
		secondPosition := r.rng.IntN(len(candidates) - 1)
		if secondPosition >= firstPosition {
			secondPosition++
		}
		first, second := candidates[firstPosition], candidates[secondPosition]
		if states[second].normalizedWork() < states[first].normalizedWork() {
			return second
		}
		return first
	default:
		panic("router constructed with invalid policy")
	}
}

// Valid reports whether policy is implemented.
func (p Policy) Valid() bool {
	switch p {
	case Random, RoundRobin, WeightedRoundRobin, ShortestQueue, LeastWork, PowerOfTwo:
		return true
	default:
		return false
	}
}

func (r *Router) routeWeighted(states []ServerState, candidates []int) int {
	if len(r.currentWeight) != len(states) {
		r.currentWeight = make([]float64, len(states))
	}
	totalWeight := 0.0
	selected := candidates[0]
	for _, index := range candidates {
		weight := states[index].serviceRate()
		totalWeight += weight
		r.currentWeight[index] += weight
		if r.currentWeight[index] > r.currentWeight[selected] {
			selected = index
		}
	}
	r.currentWeight[selected] -= totalWeight
	return selected
}

// primaryIndexes caches the immutable topology used throughout one trial.
func (r *Router) primaryIndexes(states []ServerState) []int {
	if r.topologySize == len(states) && r.candidates != nil {
		return r.candidates
	}
	r.topologySize = len(states)
	r.candidates = make([]int, 0, len(states))
	for index := range states {
		if !states[index].Backup {
			r.candidates = append(r.candidates, index)
		}
	}
	return r.candidates
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

func (s ServerState) serviceRate() float64 {
	if s.Capacity <= 0 || s.ServiceTime <= 0 {
		return 0
	}
	return float64(s.Capacity) / s.ServiceTime
}
