package wire

// IncrementRequest bounds acquisition plus both ABD operations in milliseconds.
type IncrementRequest struct {
	TimeoutMS int64 `json:"timeout_ms"`
}

// IncrementResult is emitted only after the protected write and RA release.
type IncrementResult struct {
	Status     string    `json:"status"`
	NodeID     string    `json:"node_id"`
	RunID      string    `json:"run_id"`
	ClusterID  string    `json:"cluster_id"`
	Request    RequestID `json:"request"`
	Before     *int64    `json:"before"`
	Register   *Register `json:"register"`
	DurationNS int64     `json:"duration_ns"`
}
