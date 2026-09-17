package stationary

import (
	"container/heap"
	"fmt"
	"math"
	rand "math/rand/v2"

	"mc714-t1/internal/balancer"
)

// Assignment defaults define the mandatory stable and unstable experiments.
const (
	DefaultServers = 3
	DefaultMu      = 1.0
	DefaultHorizon = 5000.0
	DefaultWarmup  = 500.0
	UnstableLambda = 3.3
)

// Config specifies one policy, traffic rate, and measurement window. Mu applies
// to every server unless ServiceRates supplies one rate per server.
type Config struct {
	Policy       balancer.Policy
	Lambda       float64
	Mu           float64
	ServiceRates []float64
	Servers      int
	Horizon      float64
	Warmup       float64
	CaptureTrace bool
}

// Model contains the M/M/1 analytical values for random routing.
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

// Sample records the whole-system state immediately after one event.
type Sample struct {
	Time      float64
	Jobs      int
	Queues    []int
	Completed []int
}

// Trial contains metrics from one independent replica.
type Trial struct {
	Throughput      float64
	Utilization     float64
	UtilizationByID []float64
	AverageJobs     float64
	AverageResponse float64
	LittleRight     float64
	FinalJobs       int
	Assigned        []int
	Samples         []Sample
}

// Summary aggregates replicas and their 95 percent confidence intervals.
type Summary struct {
	Policy              balancer.Policy
	Lambda              float64
	Mu                  float64
	Servers             int
	Horizon             float64
	Warmup              float64
	Trials              int
	Model               Model
	MeanThroughput      float64
	MeanUtilization     float64
	MeanUtilizationByID []float64
	MeanJobs            float64
	MeanResponse        float64
	MeanLittleRight     float64
	MeanFinalJobs       float64
	CI95Throughput      float64
	CI95Utilization     float64
	CI95Jobs            float64
	CI95Response        float64
	CI95LittleRight     float64
	MeanAssigned        []float64
	Runs                []Trial
	Representative      Trial
}

// DefaultConfig creates one required stable experiment.
func DefaultConfig(policy balancer.Policy, lambda float64) Config {
	return Config{
		Policy: policy, Lambda: lambda, Mu: DefaultMu, Servers: DefaultServers,
		Horizon: DefaultHorizon, Warmup: DefaultWarmup,
	}
}

// UnstableConfig creates the required lambda 3.3 trace experiment.
func UnstableConfig(policy balancer.Policy) Config {
	return Config{
		Policy: policy, Lambda: UnstableLambda, Mu: DefaultMu, Servers: DefaultServers,
		Horizon: DefaultHorizon,
	}
}

// HeterogeneousConfig creates the optional experiment with one service rate per server.
func HeterogeneousConfig(policy balancer.Policy, lambda float64, serviceRates []float64) Config {
	rates := append([]float64(nil), serviceRates...)
	return Config{
		Policy: policy, Lambda: lambda, Mu: DefaultMu, ServiceRates: rates, Servers: len(rates),
		Horizon: DefaultHorizon, Warmup: DefaultWarmup,
	}
}

// Analyze derives the random-routing three-queue M/M/1 model.
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

// Run executes independent replicas and computes sample confidence intervals.
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
		MeanUtilizationByID: make([]float64, cfg.Servers), MeanAssigned: make([]float64, cfg.Servers),
		Runs: make([]Trial, 0, trials),
	}
	throughputs := make([]float64, 0, trials)
	utilizations := make([]float64, 0, trials)
	jobs := make([]float64, 0, trials)
	responses := make([]float64, 0, trials)
	littleRights := make([]float64, 0, trials)
	for trial := range trials {
		trialConfig := cfg
		trialConfig.CaptureTrace = cfg.CaptureTrace && trial == 0
		result, err := run(trialConfig, arrivalSeed(seed, cfg.Lambda, trial), routingSeed(seed, cfg.Policy, trial))
		if err != nil {
			return Summary{}, err
		}
		if trial == 0 {
			summary.Representative = result
		}
		summary.Runs = append(summary.Runs, result)
		throughputs = append(throughputs, result.Throughput)
		utilizations = append(utilizations, result.Utilization)
		jobs = append(jobs, result.AverageJobs)
		responses = append(responses, result.AverageResponse)
		littleRights = append(littleRights, result.LittleRight)
		summary.MeanThroughput += result.Throughput
		summary.MeanUtilization += result.Utilization
		summary.MeanJobs += result.AverageJobs
		summary.MeanResponse += result.AverageResponse
		summary.MeanLittleRight += result.LittleRight
		summary.MeanFinalJobs += float64(result.FinalJobs)
		for index := range result.UtilizationByID {
			summary.MeanUtilizationByID[index] += result.UtilizationByID[index]
			summary.MeanAssigned[index] += float64(result.Assigned[index])
		}
	}
	divisor := float64(trials)
	summary.MeanThroughput /= divisor
	summary.MeanUtilization /= divisor
	summary.MeanJobs /= divisor
	summary.MeanResponse /= divisor
	summary.MeanLittleRight /= divisor
	summary.MeanFinalJobs /= divisor
	for index := range summary.MeanUtilizationByID {
		summary.MeanUtilizationByID[index] /= divisor
		summary.MeanAssigned[index] /= divisor
	}
	summary.CI95Throughput = ci95(throughputs)
	summary.CI95Utilization = ci95(utilizations)
	summary.CI95Jobs = ci95(jobs)
	summary.CI95Response = ci95(responses)
	summary.CI95LittleRight = ci95(littleRights)
	return summary, nil
}

func run(cfg Config, arrivalSeed, routingSeed uint64) (Trial, error) {
	arrivalRNG := rand.New(rand.NewPCG(arrivalSeed, 0x243f6a8885a308d3))
	serviceRNG := rand.New(rand.NewPCG(arrivalSeed, 0x13198a2e03707344))
	routingRNG := rand.New(rand.NewPCG(routingSeed, 0x8538ecf4f10d2f71))
	router, err := balancer.NewRouter(cfg.Policy, routingRNG)
	if err != nil {
		return Trial{}, err
	}

	servers := make([]server, cfg.Servers)
	states := make([]balancer.ServerState, cfg.Servers)
	events := eventQueue{}
	heap.Init(&events)
	sequence := uint64(0)
	heap.Push(&events, event{time: exponential(arrivalRNG, cfg.Lambda), kind: arrivalEvent, sequence: sequence})
	sequence++

	clock := 0.0
	requestID := 0
	completed := 0
	responseTotal := 0.0
	jobsIntegral := 0.0
	busyIntegral := make([]float64, cfg.Servers)
	assigned := make([]int, cfg.Servers)
	samples := make([]Sample, 0)
	for events.Len() > 0 {
		next := heap.Pop(&events).(event)
		if next.time > cfg.Horizon {
			integrate(servers, clock, cfg.Horizon, cfg.Warmup, &jobsIntegral, busyIntegral)
			clock = cfg.Horizon
			break
		}
		integrate(servers, clock, next.time, cfg.Warmup, &jobsIntegral, busyIntegral)
		clock = next.time

		switch next.kind {
		case arrivalEvent:
			for index := range servers {
				states[index] = balancer.ServerState{Active: boolInt(servers[index].busy), Queued: len(servers[index].queue), Capacity: 1, ServiceTime: 1 / cfg.serviceRate(index)}
			}
			serverID := router.Route(states)
			assigned[serverID]++
			request := request{id: requestID, arrival: clock}
			requestID++
			if !servers[serverID].busy {
				servers[serverID].busy = true
				sequence = scheduleDeparture(&events, sequence, clock+exponential(serviceRNG, cfg.serviceRate(serverID)), serverID, request)
			} else {
				servers[serverID].queue = append(servers[serverID].queue, request)
			}
			nextArrival := clock + exponential(arrivalRNG, cfg.Lambda)
			if nextArrival <= cfg.Horizon {
				heap.Push(&events, event{time: nextArrival, kind: arrivalEvent, sequence: sequence})
				sequence++
			}
		case departureEvent:
			server := &servers[next.serverID]
			if !server.busy {
				return Trial{}, fmt.Errorf("departure on idle server %d", next.serverID)
			}
			server.completed++
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
				sequence = scheduleDeparture(&events, sequence, clock+exponential(serviceRNG, cfg.serviceRate(next.serverID)), next.serverID, queued)
			}
		}
		if cfg.CaptureTrace && clock >= cfg.Warmup {
			samples = append(samples, snapshot(clock, servers))
		}
	}
	if clock < cfg.Horizon {
		integrate(servers, clock, cfg.Horizon, cfg.Warmup, &jobsIntegral, busyIntegral)
	}

	measurement := cfg.Horizon - cfg.Warmup
	utilizationByID := make([]float64, cfg.Servers)
	busyTotal := 0.0
	for index := range busyIntegral {
		utilizationByID[index] = busyIntegral[index] / measurement
		busyTotal += busyIntegral[index]
	}
	result := Trial{
		Throughput:      float64(completed) / measurement,
		Utilization:     busyTotal / (measurement * float64(cfg.Servers)),
		UtilizationByID: utilizationByID,
		AverageJobs:     jobsIntegral / measurement,
		FinalJobs:       totalJobs(servers),
		Assigned:        assigned,
		Samples:         samples,
	}
	if completed > 0 {
		result.AverageResponse = responseTotal / float64(completed)
		result.LittleRight = result.Throughput * result.AverageResponse
	}
	return result, nil
}

func (cfg Config) validate() error {
	if !cfg.Policy.Valid() || !finitePositive(cfg.Lambda) || !finitePositive(cfg.Mu) || cfg.Servers <= 0 || !finitePositive(cfg.Horizon) || cfg.Warmup < 0 || cfg.Warmup >= cfg.Horizon {
		return fmt.Errorf("invalid simulation configuration")
	}
	if len(cfg.ServiceRates) > 0 {
		if len(cfg.ServiceRates) != cfg.Servers {
			return fmt.Errorf("service rate count %d does not match server count %d", len(cfg.ServiceRates), cfg.Servers)
		}
		for _, rate := range cfg.ServiceRates {
			if !finitePositive(rate) {
				return fmt.Errorf("service rates must be positive and finite")
			}
		}
	}
	return nil
}

func (cfg Config) serviceRate(serverID int) float64 {
	if len(cfg.ServiceRates) == 0 {
		return cfg.Mu
	}
	return cfg.ServiceRates[serverID]
}

func integrate(servers []server, start, end, warmup float64, jobsIntegral *float64, busyIntegral []float64) {
	from := math.Max(start, warmup)
	if end <= from {
		return
	}
	elapsed := end - from
	jobs := 0
	for index, server := range servers {
		jobs += len(server.queue)
		if server.busy {
			jobs++
			busyIntegral[index] += elapsed
		}
	}
	*jobsIntegral += float64(jobs) * elapsed
}

func snapshot(time float64, servers []server) Sample {
	sample := Sample{Time: time, Jobs: totalJobs(servers), Queues: make([]int, len(servers)), Completed: make([]int, len(servers))}
	for index, server := range servers {
		sample.Queues[index] = len(server.queue) + boolInt(server.busy)
		sample.Completed[index] = server.completed
	}
	return sample
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

func ci95(values []float64) float64 {
	if len(values) < 2 {
		return 0
	}
	mean := 0.0
	for _, value := range values {
		mean += value
	}
	mean /= float64(len(values))
	variance := 0.0
	for _, value := range values {
		delta := value - mean
		variance += delta * delta
	}
	standardError := math.Sqrt(variance/float64(len(values)-1)) / math.Sqrt(float64(len(values)))
	return 2.2621571628540993 * standardError
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

func arrivalSeed(seed uint64, lambda float64, trial int) uint64 {
	return seed ^ math.Float64bits(lambda)*0x9e3779b97f4a7c15 ^ uint64(trial)*0xbf58476d1ce4e5b9
}

func routingSeed(seed uint64, policy balancer.Policy, trial int) uint64 {
	return seed ^ policySeed(policy) ^ uint64(trial)*0x94d049bb133111eb
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
	busy      bool
	queue     []request
	completed int
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
