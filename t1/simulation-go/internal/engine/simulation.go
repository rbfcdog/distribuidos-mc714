// Package engine implements the discrete-event load-balancing simulation.
package engine

import (
	"container/heap"
	"fmt"
	"math"
	rand "math/rand/v2"

	"mc714-t1/internal/balancer"
	"mc714-t1/internal/domain"
	"mc714-t1/pkg/mathutil"
)

const (
	DefaultServerCount    = 3
	DefaultServerCapacity = 15
	DefaultServiceTime    = 0.05
	DefaultHorizon        = 200.0
)

// Config contains all inputs for one independent simulation trial.
type Config struct {
	Policy         balancer.Policy
	RequestCount   int
	Horizon        float64
	ServiceTime    float64
	ServerCount    int
	ServerCapacity int
	InterArrival   mathutil.BoundedPareto
}

// DefaultConfig returns the assignment's required server parameters and a
// bounded-Pareto arrival process with Hurst parameter 0.8 (alpha 1.4).
func DefaultConfig(policy balancer.Policy, requestCount int) Config {
	alpha, err := mathutil.AlphaForHurst(0.8)
	if err != nil {
		panic(err)
	}
	arrival, err := mathutil.NewBoundedPareto(0.0004, 0.04, alpha)
	if err != nil {
		panic(err)
	}
	return Config{
		Policy:         policy,
		RequestCount:   requestCount,
		Horizon:        DefaultHorizon,
		ServiceTime:    DefaultServiceTime,
		ServerCount:    DefaultServerCount,
		ServerCapacity: DefaultServerCapacity,
		InterArrival:   arrival,
	}
}

// ServerResult summarizes a server at the end of a trial.
type ServerResult struct {
	ID        int
	Assigned  int
	Completed int
}

// Result contains the measurable outcome and complete state trace of a trial.
type Result struct {
	Requested           int
	Accepted            int
	DiscardedAtHorizon  int
	Completed           int
	Unfinished          int
	Duration            float64
	Throughput          float64
	AverageResponseTime float64
	Servers             []ServerResult
	Samples             []domain.ServerSample
}

// Run executes one isolated trial. Separate traffic and routing sources keep
// each policy's arrival trace identical for a given trial seed.
func Run(cfg Config, arrivalRNG, routingRNG *rand.Rand) (Result, error) {
	if err := cfg.validate(); err != nil {
		return Result{}, err
	}
	if arrivalRNG == nil || routingRNG == nil {
		return Result{}, fmt.Errorf("simulation requires traffic and routing random sources")
	}

	router, err := balancer.NewRouter(cfg.Policy, routingRNG)
	if err != nil {
		return Result{}, err
	}

	servers := make([]server, cfg.ServerCount)
	for index := range servers {
		servers[index].id = index
	}

	var events eventQueue
	heap.Init(&events)
	sequence := uint64(0)
	arrivalTime := 0.0
	accepted := 0
	discarded := 0
	for id := range cfg.RequestCount {
		arrivalTime += cfg.InterArrival.Sample(arrivalRNG)
		if arrivalTime > cfg.Horizon {
			discarded = cfg.RequestCount - id
			break
		}
		heap.Push(&events, event{
			time:     arrivalTime,
			kind:     arrivalEvent,
			request:  domain.Request{ID: id, ArrivalTime: arrivalTime},
			sequence: sequence,
		})
		sequence++
	}

	result := Result{Requested: cfg.RequestCount, DiscardedAtHorizon: discarded}
	result.Samples = appendSamples(result.Samples, 0, servers)
	clock := 0.0
	var totalResponseTime float64

	for events.Len() > 0 {
		event := heap.Pop(&events).(event)
		if event.time > cfg.Horizon {
			clock = cfg.Horizon
			break
		}
		clock = event.time

		switch event.kind {
		case arrivalEvent:
			loads := make([]int, len(servers))
			for index := range servers {
				loads[index] = servers[index].load()
			}
			serverID := router.Route(loads)
			selected := &servers[serverID]
			selected.assigned++
			accepted++
			if selected.active < cfg.ServerCapacity {
				selected.active++
				sequence = scheduleDeparture(&events, sequence, clock+cfg.ServiceTime, event.request, serverID)
			} else {
				selected.queue = append(selected.queue, event.request)
			}

		case departureEvent:
			selected := &servers[event.serverID]
			if selected.active == 0 {
				return Result{}, fmt.Errorf("server %d departure without active request", selected.id)
			}
			selected.active--
			selected.completed++
			result.Completed++
			totalResponseTime += clock - event.request.ArrivalTime
			if len(selected.queue) > 0 {
				next := selected.queue[0]
				selected.queue[0] = domain.Request{}
				selected.queue = selected.queue[1:]
				selected.active++
				sequence = scheduleDeparture(&events, sequence, clock+cfg.ServiceTime, next, selected.id)
			}
		}
		result.Samples = appendSamples(result.Samples, clock, servers)
	}

	result.Accepted = accepted
	result.Unfinished = accepted - result.Completed
	result.Duration = clock
	if result.Completed > 0 {
		result.AverageResponseTime = totalResponseTime / float64(result.Completed)
	}
	if clock > 0 {
		result.Throughput = float64(result.Completed) / clock
	}
	result.Servers = make([]ServerResult, len(servers))
	for index := range servers {
		result.Servers[index] = ServerResult{
			ID:        servers[index].id,
			Assigned:  servers[index].assigned,
			Completed: servers[index].completed,
		}
	}
	return result, nil
}

func (cfg Config) validate() error {
	if cfg.RequestCount <= 0 {
		return fmt.Errorf("request count must be positive: %d", cfg.RequestCount)
	}
	if cfg.Horizon <= 0 {
		return fmt.Errorf("horizon must be positive: %g", cfg.Horizon)
	}
	if cfg.ServiceTime <= 0 {
		return fmt.Errorf("service time must be positive: %g", cfg.ServiceTime)
	}
	if cfg.ServerCount <= 0 {
		return fmt.Errorf("server count must be positive: %d", cfg.ServerCount)
	}
	if cfg.ServerCapacity <= 0 {
		return fmt.Errorf("server capacity must be positive: %d", cfg.ServerCapacity)
	}
	if cfg.InterArrival.Lower <= 0 || cfg.InterArrival.Upper < cfg.InterArrival.Lower || cfg.InterArrival.Alpha <= 0 {
		return fmt.Errorf("invalid bounded Pareto inter-arrival distribution")
	}
	return nil
}

type server struct {
	id        int
	active    int
	queue     []domain.Request
	assigned  int
	completed int
}

func (s server) load() int {
	return s.active + len(s.queue)
}

func appendSamples(samples []domain.ServerSample, time float64, servers []server) []domain.ServerSample {
	for index := range servers {
		samples = append(samples, domain.ServerSample{
			Time:        time,
			ServerID:    servers[index].id,
			Active:      servers[index].active,
			QueueLength: len(servers[index].queue),
			Completed:   servers[index].completed,
		})
	}
	return samples
}

type eventKind uint8

const (
	departureEvent eventKind = iota
	arrivalEvent
)

type event struct {
	time     float64
	kind     eventKind
	request  domain.Request
	serverID int
	sequence uint64
}

type eventQueue []event

func (q eventQueue) Len() int { return len(q) }
func (q eventQueue) Less(i, j int) bool {
	if math.Abs(q[i].time-q[j].time) > 1e-12 {
		return q[i].time < q[j].time
	}
	if q[i].kind != q[j].kind {
		return q[i].kind < q[j].kind
	}
	return q[i].sequence < q[j].sequence
}
func (q eventQueue) Swap(i, j int) { q[i], q[j] = q[j], q[i] }
func (q *eventQueue) Push(value any) {
	*q = append(*q, value.(event))
}
func (q *eventQueue) Pop() any {
	old := *q
	last := len(old) - 1
	item := old[last]
	*q = old[:last]
	return item
}

func scheduleDeparture(queue *eventQueue, sequence uint64, time float64, request domain.Request, serverID int) uint64 {
	heap.Push(queue, event{
		time:     time,
		kind:     departureEvent,
		request:  request,
		serverID: serverID,
		sequence: sequence,
	})
	return sequence + 1
}
