package wire

// ABDRequest is bounded by the server. Reads forbid Value; writes require it.
type ABDRequest struct {
	TimeoutMS int64  `json:"timeout_ms"`
	Value     *int64 `json:"value,omitempty"`
}

// ABDResult is emitted only after both quorum phases have completed.
type ABDResult struct {
	Status     string    `json:"status"`
	Operation  string    `json:"operation"`
	NodeID     string    `json:"node_id"`
	RunID      string    `json:"run_id"`
	ClusterID  string    `json:"cluster_id"`
	Request    RequestID `json:"request"`
	Register   *Register `json:"register"`
	DurationNS int64     `json:"duration_ns"`
}

// ABDError distinguishes a write known not to have started propagation from one
// that may be visible despite its failed response. It is never a retry token.
type ABDError struct {
	Status  string     `json:"status"`
	Error   string     `json:"error"`
	Outcome string     `json:"outcome"`
	Request *RequestID `json:"request,omitempty"`
}
