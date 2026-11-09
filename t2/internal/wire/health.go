package wire

// Health reports process availability, not distributed algorithm readiness.
type Health struct {
	Status string `json:"status"`
	NodeID string `json:"node_id"`
	RunID  string `json:"run_id"`
}
