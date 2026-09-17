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

type ServerConfig struct {
	Capacity       int
	ServiceTime    float64
	BufferCapacity int
	Pool           int
	Backup         bool
}

type QueueArchitecture uint8

const (
	PrivateQueues QueueArchitecture = iota
	SharedQueue
)

type Config struct {
	Policy              balancer.Policy
	RequestCount        int
	Horizon             float64
	Servers             []ServerConfig
	InterArrival        mathutil.BoundedPareto
	QueueArchitecture   QueueArchitecture
	SharedQueueCapacity int
}

func DefaultConfig(policy balancer.Policy, requestCount int) Config {
	return Config{
		Policy:       policy,
		RequestCount: requestCount,
		Horizon:      DefaultHorizon,
		Servers:      homogeneousServers(DefaultServerCount, DefaultServerCapacity, DefaultServiceTime, requestCount),
		InterArrival: defaultArrivalProcess(),
	}
}

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

func MultiPoolConfig(policy balancer.Policy, requestCount int) Config {
	return Config{
		Policy:       policy,
		RequestCount: requestCount,
		Horizon:      DefaultHorizon,
		Servers: []ServerConfig{
			{Capacity: 10, ServiceTime: 0.06, BufferCapacity: requestCount, Pool: 0},
			{Capacity: 15, ServiceTime: 0.05, BufferCapacity: requestCount, Pool: 0},
			{Capacity: 20, ServiceTime: 0.04, BufferCapacity: requestCount, Pool: 1},
			{Capacity: 15, ServiceTime: 0.05, BufferCapacity: requestCount, Pool: 1},
		},
		InterArrival: defaultArrivalProcess(),
	}
}

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

func PrivateQueueStressConfig(policy balancer.Policy, requestCount int) Config {
	return Config{
		Policy:       policy,
		RequestCount: requestCount,
		Horizon:      DefaultHorizon,
		Servers:      homogeneousServers(DefaultServerCount, 1, DefaultServiceTime, requestCount),
		InterArrival: defaultArrivalProcess(),
	}
}

func SharedQueueConfig(requestCount int) Config {
	return Config{
		Policy:              balancer.LeastWork,
		RequestCount:        requestCount,
		Horizon:             DefaultHorizon,
		Servers:             homogeneousServers(DefaultServerCount, 1, DefaultServiceTime, 0),
		InterArrival:        defaultArrivalProcess(),
		QueueArchitecture:   SharedQueue,
		SharedQueueCapacity: requestCount,
	}
}

type ServerResult struct {
	ID          int
	Assigned    int
	Completed   int
	Capacity    int
	Backup      bool
	Utilization float64
}

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
	sampledIntervals := cfg.InterArrival.SampleSequence(arrivalRNG, cfg.RequestCount)
	for id, interval := range sampledIntervals {
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
	sharedQueue := make([]domain.Request, 0, cfg.SharedQueueCapacity)
	result.Samples = appendSamples(result.Samples, 0, servers, len(sharedQueue))
	clock := 0.0
	var totalResponseTime float64
	states := make([]balancer.ServerState, len(servers))

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
			if cfg.QueueArchitecture == SharedQueue {
				if len(sharedQueue) >= cfg.SharedQueueCapacity {
					result.RejectedFull++
					result.Samples = appendSamples(result.Samples, clock, servers, len(sharedQueue))
					continue
				}
				result.Accepted++
				sharedQueue = append(sharedQueue, next.request)
				sequence = dispatchSharedQueue(&events, sequence, clock, servers, &sharedQueue)
				break
			}

			updateRoutingStates(states, servers)
			serverID := router.Route(states)
			if !servers[serverID].canAdmit() {
				serverID = selectBackup(servers)
				if serverID < 0 {
					result.RejectedFull++
					result.Samples = appendSamples(result.Samples, clock, servers, len(sharedQueue))
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
			if cfg.QueueArchitecture == SharedQueue {
				sequence = dispatchSharedQueue(&events, sequence, clock, servers, &sharedQueue)
				break
			}
			if len(selected.queue) > 0 {
				queued := selected.queue[0]
				selected.queue[0] = domain.Request{}
				selected.queue = selected.queue[1:]
				selected.active++
				sequence = scheduleDeparture(&events, sequence, clock+selected.config.ServiceTime, queued, selected.id)
			}
		}
		result.Samples = appendSamples(result.Samples, clock, servers, len(sharedQueue))
	}

	if clock < cfg.Horizon {
		integrateBusyTime(servers, cfg.Horizon-clock)
		clock = cfg.Horizon
	}
	result.Samples = appendSamples(result.Samples, cfg.Horizon, servers, len(sharedQueue))
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
	if !positiveFinite(cfg.Horizon) {
		return fmt.Errorf("horizon must be finite and positive: %g", cfg.Horizon)
	}
	if len(cfg.Servers) == 0 {
		return fmt.Errorf("at least one server is required")
	}
	if cfg.QueueArchitecture != PrivateQueues && cfg.QueueArchitecture != SharedQueue {
		return fmt.Errorf("unsupported queue architecture %d", cfg.QueueArchitecture)
	}
	if cfg.SharedQueueCapacity < 0 {
		return fmt.Errorf("shared queue capacity cannot be negative: %d", cfg.SharedQueueCapacity)
	}
	primaryCount := 0
	for index, server := range cfg.Servers {
		if server.Capacity <= 0 || !positiveFinite(server.ServiceTime) {
			return fmt.Errorf("server %d capacity and service time must be finite and positive", index)
		}
		if server.BufferCapacity < 0 {
			return fmt.Errorf("server %d buffer capacity cannot be negative", index)
		}
		if server.Pool < 0 {
			return fmt.Errorf("server %d routing pool cannot be negative", index)
		}
		if !server.Backup {
			primaryCount++
		}
	}
	if primaryCount == 0 {
		return fmt.Errorf("at least one primary server is required")
	}
	if !positiveFinite(cfg.InterArrival.Lower) ||
		!positiveFinite(cfg.InterArrival.Upper) ||
		cfg.InterArrival.Upper < cfg.InterArrival.Lower ||
		!positiveFinite(cfg.InterArrival.Alpha) ||
		!positiveFinite(cfg.InterArrival.Hurst) ||
		cfg.InterArrival.Hurst < 0.5 ||
		cfg.InterArrival.Hurst >= 1 {
		return fmt.Errorf("invalid bounded Pareto inter-arrival distribution")
	}
	return nil
}

func positiveFinite(value float64) bool {
	return value > 0 && !math.IsNaN(value) && !math.IsInf(value, 0)
}

func defaultArrivalProcess() mathutil.BoundedPareto {
	arrival, err := mathutil.NewBoundedPareto(0.0004, 0.04, 1.4, 0.8)
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

func updateRoutingStates(states []balancer.ServerState, servers []server) {
	for index := range servers {
		states[index] = balancer.ServerState{
			Active: servers[index].active, Queued: len(servers[index].queue),
			Capacity: servers[index].config.Capacity, ServiceTime: servers[index].config.ServiceTime,
			Pool: servers[index].config.Pool, Backup: servers[index].config.Backup,
		}
	}
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

func dispatchSharedQueue(events *eventQueue, sequence uint64, time float64, servers []server, queue *[]domain.Request) uint64 {
	for len(*queue) > 0 {
		serverID := selectSharedWorker(servers)
		if serverID < 0 {
			return sequence
		}
		request := (*queue)[0]
		(*queue)[0] = domain.Request{}
		*queue = (*queue)[1:]
		selected := &servers[serverID]
		selected.active++
		selected.assigned++
		sequence = scheduleDeparture(events, sequence, time+selected.config.ServiceTime, request, serverID)
	}
	return sequence
}

func selectSharedWorker(servers []server) int {
	selected := -1
	bestWork := math.Inf(1)
	for index := range servers {
		if servers[index].config.Backup || servers[index].active >= servers[index].config.Capacity {
			continue
		}
		work := float64(servers[index].active+1) * servers[index].config.ServiceTime / float64(servers[index].config.Capacity)
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

func appendSamples(samples []domain.ServerSample, time float64, servers []server, sharedQueueLength int) []domain.ServerSample {
	for index := range servers {
		queueLength := len(servers[index].queue)
		if index == 0 {
			queueLength += sharedQueueLength
		}
		samples = append(samples, domain.ServerSample{
			Time: time, ServerID: servers[index].id, Active: servers[index].active,
			QueueLength: queueLength, Completed: servers[index].completed,
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
	if q[i].time != q[j].time {
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
