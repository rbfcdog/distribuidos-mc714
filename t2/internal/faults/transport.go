// Package faults injects delay and loss on outbound HTTP for experiments.
package faults

import (
	"errors"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config controls artificial delay and probabilistic loss.
type Config struct {
	Delay    time.Duration
	LossRate float64 // 0 disables; 1 drops every request
}

// ErrInjectedLoss is returned when a request is dropped by the fault model.
var ErrInjectedLoss = errors.New("injected message loss")

// FromEnv reads FAULT_DELAY_MS and FAULT_LOSS_RATE (0..1).
func FromEnv() Config {
	var cfg Config
	if raw := strings.TrimSpace(os.Getenv("FAULT_DELAY_MS")); raw != "" {
		if ms, err := strconv.Atoi(raw); err == nil && ms > 0 {
			cfg.Delay = time.Duration(ms) * time.Millisecond
		}
	}
	if raw := strings.TrimSpace(os.Getenv("FAULT_LOSS_RATE")); raw != "" {
		if rate, err := strconv.ParseFloat(raw, 64); err == nil && rate > 0 {
			if rate > 1 {
				rate = 1
			}
			cfg.LossRate = rate
		}
	}
	return cfg
}

// Enabled reports whether any fault is active.
func (c Config) Enabled() bool {
	return c.Delay > 0 || c.LossRate > 0
}

type tripper struct {
	base http.RoundTripper
	cfg  Config
	rng  *rand.Rand
}

// Wrap returns a RoundTripper that applies cfg before delegating.
func Wrap(base http.RoundTripper, cfg Config) http.RoundTripper {
	if base == nil {
		base = http.DefaultTransport
	}
	if !cfg.Enabled() {
		return base
	}
	return &tripper{base: base, cfg: cfg, rng: rand.New(rand.NewSource(time.Now().UnixNano()))}
}

func (t *tripper) RoundTrip(req *http.Request) (*http.Response, error) {
	if t.cfg.LossRate > 0 && t.rng.Float64() < t.cfg.LossRate {
		return nil, ErrInjectedLoss
	}
	if t.cfg.Delay > 0 {
		timer := time.NewTimer(t.cfg.Delay)
		defer timer.Stop()
		select {
		case <-req.Context().Done():
			return nil, req.Context().Err()
		case <-timer.C:
		}
	}
	return t.base.RoundTrip(req)
}
