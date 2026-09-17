package stationary

import (
	"container/heap"
	"fmt"
	"math"
	rand "math/rand/v2"

	"mc714-t1/internal/balancer"
)

const (
	DefaultServers = 3
	DefaultMu      = 1.0
	StableHorizon  = 20000.0
	StableWarmup   = 2000.0
	FluidHorizon   = 200.0
)

type Config struct {
	Policy  balancer.Policy
	Lambda  float64
	Mu      float64
	Servers int
	Horizon float64
	Warmup  float64
}

type Model struct {
	Lambda            float64
	Mu                float64
	Servers           int
	Stable            bool
	Rho               float64
	P0                float64
	ExpectedJobs      float64
	ExpectedQueueWait float64
	ExpectedResponse  float64
	Throughput        float64
	Utilization       float64
	FluidBacklogRate  float64
}

type Trial struct {
	Throughput      float64
	Utilization     float64
	AverageJobs     float64
	AverageResponse float64
	LittleRight     float64
	FinalJobs       int
}

type Summary struct {
	Policy          balancer.Policy
	Lambda          float64
	Mu              float64
	Servers         int
	Horizon         float64
	Warmup          float64
	Trials          int
	Model           Model
	MeanThroughput  float64
	MeanUtilization float64
	MeanJobs        float64
	MeanResponse    float64
	MeanLittleRight float64
	MeanFinalJobs   float64
	Runs            []Trial
}

func StableConfig(policy balancer.Policy, lambda float64) Config {
	return Config{
		Policy: policy, Lambda: lambda, Mu: DefaultMu, Servers: DefaultServers,
		Horizon: StableHorizon, Warmup: StableWarmup,
	}
}

func UnstableConfig(policy balancer.Policy, lambda float64) Config {
	return Config{
		Policy: policy, Lambda: lambda, Mu: DefaultMu, Servers: DefaultServers,
		Horizon: FluidHorizon,
	}
}

func Analyze(lambda, mu float64, servers int) (Model, error) {
	if !finitePositive(lambda) || !finitePositive(mu) || servers <= 0 {
		return Model{}, fmt.Errorf("lambda, mu, and server count must be positive and finite")
	}
	rho := lambda / (float64(servers) * mu)
	model := Model{
		Lambda: lambda, Mu: mu, Servers: servers, Rho: rho,
		FluidBacklogRate: math.Max(0, lambda-float64(servers)*mu),
	}
	if rho >= 1 {
		return model, nil
	}
	perServerRate := lambda / float64(servers)
	response := 1 / (mu - perServerRate)
	model.Stable = true
	model.P0 = 1 - rho
	model.ExpectedJobs = rho / (1 - rho)
	model.ExpectedQueueWait = response - 1/mu
	model.ExpectedResponse = response
	model.Throughput = lambda
	model.Utilization = rho
	return model, nil
}

func Run(cfg Config, trials int, seed uint64) (Summary, error) {
	if err := cfg.validate(); err != nil {
		return Summary{}, err
	}
	if trials <= 0 {
		return Summary{}, fmt.Errorf("trial count must be positive: %d", trials)
	}
	model, err := Analyze(cfg.Lambda, cfg.Mu, cfg.Servers)
	if err != nil {
		return Summary{}, err
	}
	summary := Summary{
		Policy: cfg.Policy, Lambda: cfg.Lambda, Mu: cfg.Mu, Servers: cfg.Servers,
		Horizon: cfg.Horizon, Warmup: cfg.Warmup, Trials: trials, Model: model,
		Runs: make([]Trial, 0, trials),
	}
	for trial := range trials {
		result, err := run(cfg, trialSeed(seed, cfg.Lambda, cfg.Policy, trial))
		if err != nil {
			return Summary{}, err
		}
		summary.Runs = append(summary.Runs, result)
		summary.MeanThroughput += result.Throughput
		summary.MeanUtilization += result.Utilization
		summary.MeanJobs += result.AverageJobs
		summary.MeanResponse += result.AverageResponse
		summary.MeanLittleRight += result.LittleRight
		summary.MeanFinalJobs += float64(result.FinalJobs)
	}
	divisor := float64(trials)
	summary.MeanThroughput /= divisor
	summary.MeanUtilization /= divisor
	summary.MeanJobs /= divisor
	summary.MeanResponse /= divisor
	summary.MeanLittleRight /= divisor
	summary.MeanFinalJobs /= divisor
	return summary, nil
}

func run(cfg Config, seed uint64) (Trial, error) {
	arrivalRNG := rand.New(rand.NewPCG(seed, 0x243f6a8885a308d3))
	serviceRNG := rand.New(rand.NewPCG(seed, 0x13198a2e03707344))
	routingRNG := rand.New(rand.NewPCG(seed, policySeed(cfg.Policy)))
	router, err := balancer.NewRouter(cfg.Policy, routingRNG)
	if err != nil {
		return Trial{}, err
	}

	servers := make([]server, cfg.Servers)
	states := make([]balancer.ServerState, cfg.Servers)
	events := eventQueue{}
	heap.Init(&events)
	sequence := uint64(0)
	nextArrival := exponential(arrivalRNG, cfg.Lambda)
	heap.Push(&events, event{time: nextArrival, kind: arrivalEvent, sequence: sequence})
	sequence++

	clock := 0.0
	requestID := 0
	completed := 0
	responseTotal := 0.0
	jobsIntegral := 0.0
	busyIntegral := 0.0
	for events.Len() > 0 {
		next := heap.Pop(&events).(event)
		if next.time > cfg.Horizon {
			jobsIntegral, busyIntegral = integrate(servers, clock, cfg.Horizon, cfg.Warmup, jobsIntegral, busyIntegral)
			clock = cfg.Horizon
			break
		}
		jobsIntegral, busyIntegral = integrate(servers, clock, next.time, cfg.Warmup, jobsIntegral, busyIntegral)
		clock = next.time

		switch next.kind {
		case arrivalEvent:
			for index := range servers {
				states[index] = balancer.ServerState{Active: boolInt(servers[index].busy), Queued: len(servers[index].queue), Capacity: 1, ServiceTime: 1 / cfg.Mu}
			}
			serverID := router.Route(states)
			request := request{id: requestID, arrival: clock}
			requestID++
			if !servers[serverID].busy {
				servers[serverID].busy = true
				sequence = scheduleDeparture(&events, sequence, clock+exponential(serviceRNG, cfg.Mu), serverID, request)
			} else {
				servers[serverID].queue = append(servers[serverID].queue, request)
			}
			nextArrival = clock + exponential(arrivalRNG, cfg.Lambda)
			if nextArrival <= cfg.Horizon {
				heap.Push(&events, event{time: nextArrival, kind: arrivalEvent, sequence: sequence})
				sequence++
			}
		case departureEvent:
			server := &servers[next.serverID]
			if !server.busy {
				return Trial{}, fmt.Errorf("departure on idle server %d", next.serverID)
			}
			if next.request.arrival >= cfg.Warmup {
				completed++
				responseTotal += clock - next.request.arrival
			}
			if len(server.queue) == 0 {
				server.busy = false
			} else {
				queued := server.queue[0]
				server.queue[0] = request{}
				server.queue = server.queue[1:]
				sequence = scheduleDeparture(&events, sequence, clock+exponential(serviceRNG, cfg.Mu), next.serverID, queued)
			}
		}
	}
	if clock < cfg.Horizon {
		jobsIntegral, busyIntegral = integrate(servers, clock, cfg.Horizon, cfg.Warmup, jobsIntegral, busyIntegral)
	}

	measurement := cfg.Horizon - cfg.Warmup
	result := Trial{
		Throughput:  float64(completed) / measurement,
		Utilization: busyIntegral / (measurement * float64(cfg.Servers)),
		AverageJobs: jobsIntegral / measurement,
		FinalJobs:   totalJobs(servers),
	}
	if completed > 0 {
		result.AverageResponse = responseTotal / float64(completed)
		result.LittleRight = result.Throughput * result.AverageResponse
	}
	return result, nil
}

func (cfg Config) validate() error {
	if !cfg.Policy.Valid() || !finitePositive(cfg.Lambda) || !finitePositive(cfg.Mu) || cfg.Servers <= 0 || !finitePositive(cfg.Horizon) || cfg.Warmup < 0 || cfg.Warmup >= cfg.Horizon {
		return fmt.Errorf("invalid stationary experiment configuration")
	}
	return nil
}

func integrate(servers []server, start, end, warmup, jobsIntegral, busyIntegral float64) (float64, float64) {
	from := math.Max(start, warmup)
	if end <= from {
		return jobsIntegral, busyIntegral
	}
	elapsed := end - from
	jobs := 0
	busy := 0
	for _, server := range servers {
		jobs += len(server.queue)
		if server.busy {
			jobs++
			busy++
		}
	}
	return jobsIntegral + float64(jobs)*elapsed, busyIntegral + float64(busy)*elapsed
}

func totalJobs(servers []server) int {
	jobs := 0
	for _, server := range servers {
		jobs += len(server.queue)
		if server.busy {
			jobs++
		}
	}
	return jobs
}

func exponential(rng *rand.Rand, rate float64) float64 {
	return -math.Log1p(-rng.Float64()) / rate
}

func boolInt(value bool) int {
	if value {
		return 1
	}
	return 0
}

func finitePositive(value float64) bool {
	return value > 0 && !math.IsNaN(value) && !math.IsInf(value, 0)
}

func trialSeed(seed uint64, lambda float64, policy balancer.Policy, trial int) uint64 {
	return seed ^ math.Float64bits(lambda)*0x9e3779b97f4a7c15 ^ policySeed(policy) ^ uint64(trial)*0xbf58476d1ce4e5b9
}

func policySeed(policy balancer.Policy) uint64 {
	switch policy {
	case balancer.Random:
		return 0x94d049bb133111eb
	case balancer.RoundRobin:
		return 0xd1b54a32d192ed03
	case balancer.ShortestQueue:
		return 0x8538ecf4f10d2f71
	default:
		return 0
	}
}

type request struct {
	id      int
	arrival float64
}

type server struct {
	busy  bool
	queue []request
}

type eventKind uint8

const (
	departureEvent eventKind = iota
	arrivalEvent
)

type event struct {
	time     float64
	kind     eventKind
	serverID int
	request  request
	sequence uint64
}

type eventQueue []event

func (q eventQueue) Len() int { return len(q) }

func (q eventQueue) Less(left, right int) bool {
	if q[left].time != q[right].time {
		return q[left].time < q[right].time
	}
	if q[left].kind != q[right].kind {
		return q[left].kind < q[right].kind
	}
	return q[left].sequence < q[right].sequence
}

func (q eventQueue) Swap(left, right int) { q[left], q[right] = q[right], q[left] }

func (q *eventQueue) Push(value any) { *q = append(*q, value.(event)) }

func (q *eventQueue) Pop() any {
	items := *q
	last := len(items) - 1
	item := items[last]
	items[last] = event{}
	*q = items[:last]
	return item
}

func scheduleDeparture(events *eventQueue, sequence uint64, time float64, serverID int, request request) uint64 {
	heap.Push(events, event{time: time, kind: departureEvent, serverID: serverID, request: request, sequence: sequence})
	return sequence + 1
}
