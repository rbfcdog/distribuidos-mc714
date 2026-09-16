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

func TestWeightedRoundRobinFollowsHeterogeneousServiceRates(t *testing.T) {
	router, err := NewRouter(WeightedRoundRobin, rand.New(rand.NewPCG(1, 2)))
	if err != nil {
		t.Fatal(err)
	}
	states := []ServerState{
		{Capacity: 1, ServiceTime: 1},
		{Capacity: 2, ServiceTime: 1},
		{Capacity: 3, ServiceTime: 1},
		{Capacity: 100, ServiceTime: 0.01, Backup: true},
	}
	counts := make([]int, len(states))
	for range 12 {
		counts[router.Route(states)]++
	}
	if counts[0] != 2 || counts[1] != 4 || counts[2] != 6 || counts[3] != 0 {
		t.Fatalf("weighted assignments = %v, want [2 4 6 0]", counts)
	}
}

func TestPowerOfTwoChoosesLessLoadedOfTwoPrimaries(t *testing.T) {
	router, err := NewRouter(PowerOfTwo, rand.New(rand.NewPCG(1, 2)))
	if err != nil {
		t.Fatal(err)
	}
	states := []ServerState{
		{Active: 8, Queued: 2, Capacity: 10, ServiceTime: 0.05},
		{Active: 2, Capacity: 10, ServiceTime: 0.05},
		{Capacity: 100, ServiceTime: 0.01, Backup: true},
	}
	for range 20 {
		if got := router.Route(states); got != 1 {
			t.Fatalf("route = %d, want less-loaded primary server 1", got)
		}
	}
}

func TestHierarchicalLeastWorkChoosesPoolThenLocalServer(t *testing.T) {
	router, err := NewRouter(HierarchicalLeastWork, rand.New(rand.NewPCG(1, 2)))
	if err != nil {
		t.Fatal(err)
	}
	states := []ServerState{
		{Active: 10, Capacity: 10, ServiceTime: 0.1, Pool: 0},
		{Active: 10, Capacity: 10, ServiceTime: 0.1, Pool: 0},
		{Active: 5, Capacity: 10, ServiceTime: 0.05, Pool: 1},
		{Capacity: 10, ServiceTime: 0.05, Pool: 1},
		{Capacity: 100, ServiceTime: 0.01, Pool: 2, Backup: true},
	}
	if got := router.Route(states); got != 3 {
		t.Fatalf("route = %d, want least-work server 3 from least-loaded pool 1", got)
	}
}

func TestPowerOfTwoRequiresRandomSource(t *testing.T) {
	if _, err := NewRouter(PowerOfTwo, nil); err == nil {
		t.Fatal("NewRouter accepted power-of-two policy without random source")
	}
}

func TestInvalidPolicyIsRejected(t *testing.T) {
	if _, err := NewRouter(Policy("unknown"), rand.New(rand.NewPCG(1, 2))); err == nil {
		t.Fatal("NewRouter accepted an unsupported policy")
	}
}
