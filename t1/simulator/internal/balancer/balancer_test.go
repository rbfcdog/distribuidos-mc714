package balancer

import (
	rand "math/rand/v2"
	"testing"
)

func TestRoundRobinCyclesWithoutSharedState(t *testing.T) {
	first, err := NewRouter(RoundRobin, rand.New(rand.NewPCG(1, 2)))
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewRouter(RoundRobin, rand.New(rand.NewPCG(3, 4)))
	if err != nil {
		t.Fatal(err)
	}

	for index, want := range []int{0, 1, 2, 0, 1} {
		if got := first.Route([]int{0, 0, 0}); got != want {
			t.Fatalf("route %d = %d, want %d", index, got, want)
		}
	}
	if got := second.Route([]int{0, 0, 0}); got != 0 {
		t.Fatalf("independent router started at %d, want 0", got)
	}
}

func TestShortestQueueUsesTotalLoadAndStableTieBreak(t *testing.T) {
	router, err := NewRouter(ShortestQueue, rand.New(rand.NewPCG(1, 2)))
	if err != nil {
		t.Fatal(err)
	}
	if got := router.Route([]int{3, 1, 2}); got != 1 {
		t.Fatalf("route = %d, want least-loaded server 1", got)
	}
	if got := router.Route([]int{3, 1, 1}); got != 1 {
		t.Fatalf("route = %d, want first least-loaded server 1", got)
	}
}

func TestInvalidPolicyIsRejected(t *testing.T) {
	if _, err := NewRouter(Policy("unknown"), rand.New(rand.NewPCG(1, 2))); err == nil {
		t.Fatal("NewRouter accepted an unsupported policy")
	}
}
