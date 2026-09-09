// Package balancer selects a destination server for accepted requests.
package balancer

import (
	"fmt"
	rand "math/rand/v2"
)

// Policy identifies a supported load-balancing policy.
type Policy string

const (
	Random        Policy = "random"
	RoundRobin    Policy = "round_robin"
	ShortestQueue Policy = "shortest_queue"
)

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

// Route returns a server index. loads must contain each server's active plus
// queued requests, ensuring ShortestQueue observes in-service work as well.
func (r *Router) Route(loads []int) int {
	if len(loads) == 0 {
		panic("route called with no servers")
	}

	switch r.policy {
	case Random:
		return r.rng.IntN(len(loads))
	case RoundRobin:
		selected := r.next
		r.next = (r.next + 1) % len(loads)
		return selected
	case ShortestQueue:
		selected := 0
		for index := 1; index < len(loads); index++ {
			if loads[index] < loads[selected] {
				selected = index
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
	case Random, RoundRobin, ShortestQueue:
		return true
	default:
		return false
	}
}
