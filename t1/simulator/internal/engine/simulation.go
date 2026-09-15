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

// ServerConfig defines one server's concurrency, service speed, waiting buffer,
// and whether it is reserved for overflow traffic.
type ServerConfig struct {
	Capacity       int
	ServiceTime    float64
	BufferCapacity int
	Backup         bool
}

// Config contains all inputs for one independent simulation trial.
type Config struct {
	Policy       balancer.Policy
	RequestCount int
	Horizon      float64
	Servers      []ServerConfig
	InterArrival mathutil.BoundedPareto
}

// DefaultConfig returns the assignment's homogeneous server parameters and a
// bounded-Pareto arrival process with Hurst parameter 0.8 (alpha 1.4).
func DefaultConfig(policy balancer.Policy, requestCount int) Config {
	return Config{
		Policy:       policy,
		RequestCount: requestCount,
		Horizon:      DefaultHorizon,
		Servers:      homogeneousServers(DefaultServerCount, DefaultServerCapacity, DefaultServiceTime, requestCount),
		InterArrival: defaultArrivalProcess(),
	}
}

// HeterogeneousConfig demonstrates capacity-aware routing across primary
// servers with different concurrency and service speed.
func HeterogeneousConfig(policy balancer.Policy, requestCount int) Config {
	return Config{
		Policy:       policy,
		RequestCount: requestCount,
		Horizon:      DefaultHorizon,
		Servers: []ServerConfig{
			{Capacity: 10, ServiceTime: 0.06, BufferCapacity: requestCount},
			{Capacity: 15, ServiceTime: 0.05, BufferCapacity: requestCount},
			{Capacity: 20, ServiceTime: 0.04, BufferCapacity: requestCount},
		},
		InterArrival: defaultArrivalProcess(),
	}
}

// BoundedBufferConfig gives each primary a deliberately small waiting buffer.
// Add backup=true to provision an overflow server activated on saturation.
func BoundedBufferConfig(policy balancer.Policy, requestCount int, backup bool) Config {
	servers := homogeneousServers(DefaultServerCount, DefaultServerCapacity, DefaultServiceTime, 2)
	if backup {
		servers = append(servers, ServerConfig{
			Capacity: 15, ServiceTime: DefaultServiceTime, BufferCapacity: requestCount, Backup: true,
		})
	}
	return Config{
		Policy:       policy,
		RequestCount: requestCount,
		Horizon:      DefaultHorizon,
		Servers:      servers,
		InterArrival: defaultArrivalProcess(),
	}
}

// ServerResult summarizes a server at the end of a trial.
type ServerResult struct {
	ID          int
	Assigned    int
	Completed   int
	Capacity    int
	Backup      bool
	Utilization float64
}

// Result contains the measurable outcome and complete state trace of a trial.
type Result struct {
	Requested           int
	Accepted            int
	RejectedFull        int
	DiscardedAtHorizon  int
	Completed           int
	Unfinished          int
	BackupActivations   int
	Duration            float64
	Throughput          float64
	Utilization         float64
	AverageResponseTime float64
	Interarrivals       []float64
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

	servers := make([]server, len(cfg.Servers))
	for index, configuration := range cfg.Servers {
		servers[index] = server{id: index, config: configuration}
	}

	var events eventQueue
	heap.Init(&events)
	sequence := uint64(0)
	arrivalTime := 0.0
	discarded := 0
	interarrivals := make([]float64, 0, cfg.RequestCount)
	for id := range cfg.RequestCount {
		interval := cfg.InterArrival.Sample(arrivalRNG)
		interarrivals = append(interarrivals, interval)
		arrivalTime += interval
		if arrivalTime > cfg.Horizon {
			discarded = cfg.RequestCount - id
			break
		}
		heap.Push(&events, event{
			time: arrivalTime, kind: arrivalEvent,
			request: domain.Request{ID: id, ArrivalTime: arrivalTime}, sequence: sequence,
		})
		sequence++
	}

	result := Result{Requested: cfg.RequestCount, DiscardedAtHorizon: discarded, Interarrivals: interarrivals}
	result.Samples = appendSamples(result.Samples, 0, servers)
	clock := 0.0
	var totalResponseTime float64

	for events.Len() > 0 {
		next := heap.Pop(&events).(event)
		if next.time > cfg.Horizon {
			integrateBusyTime(servers, cfg.Horizon-clock)
			clock = cfg.Horizon
			break
		}
		integrateBusyTime(servers, next.time-clock)
		clock = next.time

		switch next.kind {
		case arrivalEvent:
			states := routingStates(servers)
			serverID := router.Route(states)
			if !servers[serverID].canAdmit() {
				serverID = selectBackup(servers)
				if serverID < 0 {
					result.RejectedFull++
					result.Samples = appendSamples(result.Samples, clock, servers)
					continue
				}
				result.BackupActivations++
			}

			selected := &servers[serverID]
			selected.assigned++
			result.Accepted++
			if selected.active < selected.config.Capacity {
				selected.active++
				sequence = scheduleDeparture(&events, sequence, clock+selected.config.ServiceTime, next.request, serverID)
			} else {
				selected.queue = append(selected.queue, next.request)
			}

		case departureEvent:
			selected := &servers[next.serverID]
			if selected.active == 0 {
				return Result{}, fmt.Errorf("server %d departure without active request", selected.id)
			}
			selected.active--
			selected.completed++
			result.Completed++
			totalResponseTime += clock - next.request.ArrivalTime
			if len(selected.queue) > 0 {
				queued := selected.queue[0]
				selected.queue[0] = domain.Request{}
				selected.queue = selected.queue[1:]
				selected.active++
				sequence = scheduleDeparture(&events, sequence, clock+selected.config.ServiceTime, queued, selected.id)
			}
		}
		result.Samples = appendSamples(result.Samples, clock, servers)
	}

	if clock < cfg.Horizon {
		integrateBusyTime(servers, cfg.Horizon-clock)
		clock = cfg.Horizon
	}
	result.Samples = appendSamples(result.Samples, cfg.Horizon, servers)
	result.Unfinished = result.Accepted - result.Completed
	result.Duration = cfg.Horizon
	result.Throughput = float64(result.Completed) / cfg.Horizon
	if result.Completed > 0 {
		result.AverageResponseTime = totalResponseTime / float64(result.Completed)
	}

	result.Servers = make([]ServerResult, len(servers))
	totalCapacity := 0
	totalBusySlotTime := 0.0
	for index := range servers {
		utilization := servers[index].busySlotTime / (float64(servers[index].config.Capacity) * cfg.Horizon)
		result.Servers[index] = ServerResult{
			ID: servers[index].id, Assigned: servers[index].assigned, Completed: servers[index].completed,
			Capacity: servers[index].config.Capacity, Backup: servers[index].config.Backup, Utilization: utilization,
		}
		totalCapacity += servers[index].config.Capacity
		totalBusySlotTime += servers[index].busySlotTime
	}
	result.Utilization = totalBusySlotTime / (float64(totalCapacity) * cfg.Horizon)
	return result, nil
}

func (cfg Config) validate() error {
	if cfg.RequestCount <= 0 {
		return fmt.Errorf("request count must be positive: %d", cfg.RequestCount)
	}
	if cfg.Horizon <= 0 {
		return fmt.Errorf("horizon must be positive: %g", cfg.Horizon)
	}
	if len(cfg.Servers) == 0 {
		return fmt.Errorf("at least one server is required")
	}
	primaryCount := 0
	for index, server := range cfg.Servers {
		if server.Capacity <= 0 || server.ServiceTime <= 0 {
			return fmt.Errorf("server %d capacity and service time must be positive", index)
		}
		if server.BufferCapacity < 0 {
			return fmt.Errorf("server %d buffer capacity cannot be negative", index)
		}
		if !server.Backup {
			primaryCount++
		}
	}
	if primaryCount == 0 {
		return fmt.Errorf("at least one primary server is required")
	}
	if cfg.InterArrival.Lower <= 0 || cfg.InterArrival.Upper < cfg.InterArrival.Lower || cfg.InterArrival.Alpha <= 0 {
		return fmt.Errorf("invalid bounded Pareto inter-arrival distribution")
	}
	return nil
}

func defaultArrivalProcess() mathutil.BoundedPareto {
	alpha, err := mathutil.AlphaForHurst(0.8)
	if err != nil {
		panic(err)
	}
	arrival, err := mathutil.NewBoundedPareto(0.0004, 0.04, alpha)
	if err != nil {
		panic(err)
	}
	return arrival
}

func homogeneousServers(count, capacity int, serviceTime float64, bufferCapacity int) []ServerConfig {
	servers := make([]ServerConfig, count)
	for index := range servers {
		servers[index] = ServerConfig{Capacity: capacity, ServiceTime: serviceTime, BufferCapacity: bufferCapacity}
	}
	return servers
}

type server struct {
	id           int
	config       ServerConfig
	active       int
	queue        []domain.Request
	assigned     int
	completed    int
	busySlotTime float64
}

func (s server) canAdmit() bool {
	return s.active < s.config.Capacity || len(s.queue) < s.config.BufferCapacity
}

func (s server) load() int {
	return s.active + len(s.queue)
}

func routingStates(servers []server) []balancer.ServerState {
	states := make([]balancer.ServerState, len(servers))
	for index := range servers {
		states[index] = balancer.ServerState{
			Active: servers[index].active, Queued: len(servers[index].queue),
			Capacity: servers[index].config.Capacity, ServiceTime: servers[index].config.ServiceTime,
			Backup: servers[index].config.Backup,
		}
	}
	return states
}

func selectBackup(servers []server) int {
	selected := -1
	bestWork := math.Inf(1)
	for index := range servers {
		if !servers[index].config.Backup || !servers[index].canAdmit() {
			continue
		}
		work := float64(servers[index].load()+1) * servers[index].config.ServiceTime / float64(servers[index].config.Capacity)
		if work < bestWork {
			selected, bestWork = index, work
		}
	}
	return selected
}

func integrateBusyTime(servers []server, elapsed float64) {
	if elapsed <= 0 {
		return
	}
	for index := range servers {
		servers[index].busySlotTime += float64(servers[index].active) * elapsed
	}
}

func appendSamples(samples []domain.ServerSample, time float64, servers []server) []domain.ServerSample {
	for index := range servers {
		samples = append(samples, domain.ServerSample{
			Time: time, ServerID: servers[index].id, Active: servers[index].active,
			QueueLength: len(servers[index].queue), Completed: servers[index].completed,
			Capacity: servers[index].config.Capacity, Backup: servers[index].config.Backup,
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
func (q eventQueue) Swap(i, j int)   { q[i], q[j] = q[j], q[i] }
func (q *eventQueue) Push(value any) { *q = append(*q, value.(event)) }
func (q *eventQueue) Pop() any {
	old := *q
	last := len(old) - 1
	item := old[last]
	old[last] = event{}
	*q = old[:last]
	return item
}

func scheduleDeparture(queue *eventQueue, sequence uint64, time float64, request domain.Request, serverID int) uint64 {
	heap.Push(queue, event{time: time, kind: departureEvent, request: request, serverID: serverID, sequence: sequence})
	return sequence + 1
}
