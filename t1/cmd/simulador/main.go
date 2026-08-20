package main

import (
	"fmt"
	"math/rand"
	"time"

	"mc714-t1/internal/engine"
)

func main() {
	rand.Seed(time.Now().UnixNano())

	rajadas := []int{30, 60, 90, 120}
	policies := []string{"Aleatoria", "RoundRobin", "FilaMaisCurta"}

	fmt.Println("=========================================================================")
	fmt.Println("   MC714 - Simulador de Balanceamento de Carga (Eventos Discretos)")
	fmt.Println("=========================================================================")

	// 10 rodadas de repeticao poderiam ser envelopadas num loop aqui,
	// mas para manter a tela limpa, vamos demonstrar a bateria basica:
	for _, policy := range policies {
		for _, numRequests := range rajadas {
			cfg := engine.Config{
				Policy:      policy,
				NumRequests: numRequests,
				L:           1.0,
				H:           100.0,
				Alpha:       1.5,
			}
			engine.Run(cfg)
		}
		fmt.Println("-------------------------------------------------------------------------")
	}
}
