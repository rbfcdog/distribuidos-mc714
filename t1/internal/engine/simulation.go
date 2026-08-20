package engine

import (
	"container/heap"
	"fmt"

	"mc714-t1/internal/balancer"
	"mc714-t1/internal/domain"
	"mc714-t1/pkg/mathutil"
)

// Config armazena os parâmetros de uma rodada de simulação
type Config struct {
	Policy      string
	NumRequests int
	L           float64
	H           float64
	Alpha       float64
}

// Run executa uma simulação isolada e cospe os resultados
func Run(cfg Config) {
	fmt.Printf("[Política: %15s | Rajada: %3d reqs] -> ", cfg.Policy, cfg.NumRequests)

	var pq PriorityQueue
	heap.Init(&pq)

	servers := []*domain.Server{{ID: 0}, {ID: 1}, {ID: 2}}
	currentTime := 0.0

	// Acumulador para o Tempo de Resposta (Response Time) de todas as requisições
	var totalResponseTime float64

	// 1. Injetar Chegadas Iniciais
	for i := 0; i < cfg.NumRequests; i++ {
		currentTime += 1.0 // Inter-chegada
		serviceTime := mathutil.RandomBoundedPareto(cfg.L, cfg.H, cfg.Alpha)

		req := &domain.Request{
			ID:          i,
			ArrivalTime: currentTime,
			ServiceTime: serviceTime,
		}

		heap.Push(&pq, &domain.Event{Time: req.ArrivalTime, Type: domain.Arrival, Req: req})
	}

	// 2. Loop de Eventos (Event Loop Principal)
	clock := 0.0

	for pq.Len() > 0 {
		event := heap.Pop(&pq).(*domain.Event)
		clock = event.Time

		if event.Type == domain.Arrival {
			serverID := balancer.RouteRequest(cfg.Policy, servers)
			srv := servers[serverID]

			if !srv.IsBusy {
				srv.IsBusy = true
				departureTime := clock + event.Req.ServiceTime
				heap.Push(&pq, &domain.Event{
					Time: departureTime, Type: domain.Departure, Req: event.Req, ServerID: srv.ID,
				})
			} else {
				srv.Queue = append(srv.Queue, event.Req)
			}

		} else if event.Type == domain.Departure {
			srv := servers[event.ServerID]
			srv.TotalServed++

			// Calcula o tempo total desde a chegada até a saída (Tempo de Resposta)
			responseTime := clock - event.Req.ArrivalTime
			totalResponseTime += responseTime

			if len(srv.Queue) > 0 {
				nextReq := srv.Queue[0]
				srv.Queue = srv.Queue[1:]

				departureTime := clock + nextReq.ServiceTime
				heap.Push(&pq, &domain.Event{This is for your reference only. You can use it to guide the direction of your conversation if you are unsure how to proceed with follow-ups.

Do NOT send it to the model verbatim, and you do not have to follow it exactly or limit your follow-ups to it.

The goal is to test whether the model can debug a subtle distributed backend correctness report in etcd without blindly accepting the symptom as true. A correct response should trace the interaction between lease expiry/revocation, Raft proposal/application, MVCC revision assignment, watchable-store event delivery, and watch creation from a specific revision.

The ideal model should determine whether the reported behavior is actually reachable. It should inspect the lessor code, server/apply path, MVCC delete path, watchable store synced/unsynced watcher logic, and compaction/watch-start behavior. A strong answer should explain whether a client can observe a key at a revision and then start watching from that revision while missing the lease-delete event, and it should cite concrete functions/files rather than giving generic Raft or MVCC explanations.

The model is likely to fail by assuming the production report is true, patching the lessor locally instead of following the Raft-applied delete path, misunderstanding etcd watch start semantics, ignoring synced vs unsynced watcher behavior, or proposing a fix that changes revision/watch semantics. In follow-up turns, I plan to challenge any broad claim by asking for exact reachability through the code, then push the model to either prove the invariant that makes the report impossible or design a minimal regression test/fix if it finds a real bug.
					Time: departureTime, Type: domain.Departure, Req: nThis is for your reference only. You can use it to guide the direction of your conversation if you are unsure how to proceed with follow-ups.

Do NOT send it to the model verbatim, and you do not have to follow it exactly or limit your follow-ups to it.

The goal is to test whether the model can debug a subtle distributed backend correctness report in etcd without blindly accepting the symptom as true. A correct response should trace the interaction between lease expiry/revocation, Raft proposal/application, MVCC revision assignment, watchable-store event delivery, and watch creation from a specific revision.

The ideal model should determine whether the reported behavior is actually reachable. It should inspect the lessor code, server/apply path, MVCC delete path, watchable store synced/unsynced watcher logic, and compaction/watch-start behavior. A strong answer should explain whether a client can observe a key at a revision and then start watching from that revision while missing the lease-delete event, and it should cite concrete functions/files rather than giving generic Raft or MVCC explanations.

The model is likely to fail by assuming the production report is true, patching the lessor locally instead of following the Raft-applied delete path, misunderstanding etcd watch start semantics, ignoring synced vs unsynced watcher behavior, or proposing a fix that changes revision/watch semantics. In follow-up turns, I plan to challenge any broad claim by asking for exact reachability through the code, then push the model to either prove the invariant that makes the report impossible or design a minimal regression test/fix if it finds a real bug.extReq, ServerID: srv.ID,
				})
			} else {
				srv.IsBusy = false
			}
		}
	}

	avgResponseTime := totalResponseTime / float64(cfg.NumRequests)

	// 3. Resultados
	fmt.Printf("Tempo Total: %8.2f | Média Resp: %6.2f | Servidores: [%d, %d, %d]\n",
		clock, avgResponseTime, servers[0].TotalServed, servers[1].TotalServed, servers[2].TotalServed)
}
