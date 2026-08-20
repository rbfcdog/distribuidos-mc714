package mathutil

import (
	"math"
	"math/rand"
)

// RandomBoundedPareto gera número Bounded Pareto via Transformação Inversa
func RandomBoundedPareto(L, H, alpha float64) float64 {
	u := rand.Float64()
	term := 1.0 - math.Pow(L/H, alpha)
	den := math.Pow(1.0-(u*term), 1.0/alpha)
	return L / den
}
