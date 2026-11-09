package wire

// ExecuteRequest bounds one acquisition attempt and its critical-section work.
// HoldMS may be zero; WaitMS must be positive. Limits are enforced by the server.
type ExecuteRequest struct {
	HoldMS int64 `json:"hold_ms"`
	WaitMS int64 `json:"wait_ms"`
}

// ExecuteResult is returned only after the critical section has been released.
type ExecuteResult struct {
	Status    string    `json:"status"`
	NodeID    string    `json:"node_id"`
	RunID     string    `json:"run_id"`
	ClusterID string    `json:"cluster_id"`
	Request   RequestID `json:"request"`
	WaitNS    int64     `json:"wait_ns"`
	HeldNS    int64     `json:"held_ns"`
}

// CriticalEvent is correctness instrumentation, never a permission to enter.
type CriticalEvent struct {
	Phase     string    `json:"phase"`
	NodeID    string    `json:"node_id"`
	RunID     string    `json:"run_id"`
	ClusterID string    `json:"cluster_id"`
	Request   RequestID `json:"request"`
}
