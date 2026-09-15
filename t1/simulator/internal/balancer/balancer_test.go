package balancer

import (
	rand "math/rand/v2"
	"testing"
)

func TestRoundRobinCyclesAcrossPrimaryServersOnly(t *testing.T) {
	router, err := NewRouter(RoundRobin, rand.New(rand.NewPCG(1, 2)))
	if err != nil {
		t.Fatal(err)
	}
	states := []ServerState{{Capacity: 15}, {Capacity: 15}, {Capacity: 15}, {Capacity: 15, Backup: true}}
	for index, want := range []int{0, 1, 2, 0, 1} {
		if got := router.Route(states); got != want {
			t.Fatalf("route %d = %d, want %d", index, got, want)
		}
	}
}

func TestShortestQueueUsesTotalLoadAndStableTieBreak(t *testing.T) {
	router, err := NewRouter(ShortestQueue, rand.New(rand.NewPCG(1, 2)))
	if err != nil {
		t.Fatal(err)
	}
	states := []ServerState{{Active: 2, Queued: 1, Capacity: 15}, {Active: 1, Capacity: 15}, {Active: 1, Queued: 1, Capacity: 15}}
	if got := router.Route(states); got != 1 {
		t.Fatalf("route = %d, want least-loaded server 1", got)
	}
}

func TestLeastWorkAccountsForHeterogeneousCapacityAndSpeed(t *testing.T) {
	router, err := NewRouter(LeastWork, rand.New(rand.NewPCG(1, 2)))
	if err != nil {
		t.Fatal(err)
	}
	states := []ServerState{
		{Active: 4, Capacity: 5, ServiceTime: 0.08},
		{Active: 6, Capacity: 15, ServiceTime: 0.04},
		{Active: 0, Capacity: 10, ServiceTime: 0.05, Backup: true},
	}
	if got := router.Route(states); got != 1 {
		t.Fatalf("route = %d, want faster higher-capacity primary server 1", got)
	}
}

func TestInvalidPolicyIsRejected(t *testing.T) {
	if _, err := NewRouter(Policy("unknown"), rand.New(rand.NewPCG(1, 2))); err == nil {
		t.Fatal("NewRouter accepted an unsupported policy")
	}
}
