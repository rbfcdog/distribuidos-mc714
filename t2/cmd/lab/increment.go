package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"strings"

	"mc714-t2/internal/wire"
)

func executeIncrement(client *http.Client, baseURL string, input wire.IncrementRequest) (wire.IncrementResult, error) {
	var result wire.IncrementResult
	body, err := json.Marshal(input)
	if err != nil {
		return result, fmt.Errorf("encode increment request: %w", err)
	}
	// Following a redirect would issue another increment, not recover this one.
	requestClient := *client
	requestClient.CheckRedirect = func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse }
	response, err := requestClient.Post(strings.TrimRight(baseURL, "/")+"/increment", "application/json", bytes.NewReader(body))
	if err != nil {
		return result, unknownIncrementOutcome(err)
	}
	defer response.Body.Close()
	const maxResponseBytes = 4096
	body, err = io.ReadAll(io.LimitReader(response.Body, maxResponseBytes+1))
	if err != nil {
		return result, unknownIncrementOutcome(fmt.Errorf("read response: %w", err))
	}
	if len(body) > maxResponseBytes {
		return result, unknownIncrementOutcome(errors.New("response exceeds 4096 bytes"))
	}
	if response.StatusCode != http.StatusOK {
		var failure wire.ABDError
		if err := decodeABDResponse(body, &failure); err != nil {
			return result, unknownIncrementOutcome(fmt.Errorf("HTTP %d: invalid error response: %w", response.StatusCode, err))
		}
		if response.StatusCode < 400 || response.StatusCode > 599 || failure.Status != "error" ||
			strings.TrimSpace(failure.Error) == "" ||
			(failure.Outcome != "not_applied" && failure.Outcome != "indeterminate") ||
			(failure.Request != nil && (!validIdentity(failure.Request.Owner) || failure.Request.Sequence == 0)) {
			return result, unknownIncrementOutcome(fmt.Errorf("HTTP %d: invalid error status, outcome, or request", response.StatusCode))
		}
		return result, fmt.Errorf("increment: HTTP %d, outcome=%s: %s (do not retry automatically)", response.StatusCode, failure.Outcome, failure.Error)
	}
	var completion struct {
		wire.IncrementResult
		DurationNS *int64 `json:"duration_ns"`
	}
	if err := decodeABDResponse(body, &completion); err != nil {
		return result, unknownIncrementOutcome(fmt.Errorf("decode completion: %w", err))
	}
	if completion.Status != "completed" || !validIdentity(completion.NodeID) ||
		!validIdentity(completion.RunID) || !validClusterID(completion.ClusterID) ||
		completion.Request.Owner != completion.NodeID || completion.Request.Sequence == 0 ||
		completion.Before == nil || completion.DurationNS == nil || *completion.DurationNS < 0 || completion.Register == nil {
		return result, unknownIncrementOutcome(errors.New("response does not prove completion for a valid request"))
	}
	register := completion.Register
	if register.Tag.Counter == 0 || register.Tag.WriterID != completion.NodeID ||
		*completion.Before == math.MaxInt64 || register.Value != *completion.Before+1 {
		return result, unknownIncrementOutcome(errors.New("completion does not prove a protected +1 increment"))
	}
	completion.IncrementResult.DurationNS = *completion.DurationNS
	return completion.IncrementResult, nil
}

func unknownIncrementOutcome(err error) error {
	return fmt.Errorf("increment (outcome unknown; may be indeterminate; do not retry automatically): %w", err)
}
