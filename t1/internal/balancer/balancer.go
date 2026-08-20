package balancer

import (
	"math/rand"

	"mc714-t1/internal/domain"
)

var lastRoundRobin = -1

// RouteRequest centraliza o roteamento de acordo com a política
func RouteRequest(policy string, servers []*domain.Server) int {
	switch policy {
	case "Aleatoria":
		return rand.Intn(len(servers))

	case "RoundRobin":
		lastRoundRobin = (lastRoundRobin + 1) % len(servers)
		return lastRoundRobin

	case "FilaMaisCurta":
		// TODO: Implemente sua lógica de Fila Mais Curta aqui.
		// Encontre qual server tem o menor len(Queue)
		bestServer := 0
		return bestServer
	}
	return 0
}
