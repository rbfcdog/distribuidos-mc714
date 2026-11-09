package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"mc714-t2/internal/wire"
)

func executeABD(client *http.Client, baseURL, operation string, input wire.ABDRequest) (wire.ABDResult, error) {
	var result wire.ABDResult
	body, err := json.Marshal(input)
	if err != nil {
		return result, fmt.Errorf("encode ABD request: %w", err)
	}
	// A redirect would start another logical operation, not retry the same one.
	requestClient := *client
	requestClient.CheckRedirect = func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse }
	response, err := requestClient.Post(strings.TrimRight(baseURL, "/")+"/abd/"+operation, "application/json", bytes.NewReader(body))
	if err != nil {
		return result, unknownABDOutcome(operation, err)
	}
	defer response.Body.Close()
	const maxResponseBytes = 4096
	body, err = io.ReadAll(io.LimitReader(response.Body, maxResponseBytes+1))
	if err != nil {
		return result, unknownABDOutcome(operation, fmt.Errorf("read response: %w", err))
	}
	if len(body) > maxResponseBytes {
		return result, unknownABDOutcome(operation, errors.New("response exceeds 4096 bytes"))
	}
	if response.StatusCode != http.StatusOK {
		var failure wire.ABDError
		if err := decodeABDResponse(body, &failure); err != nil {
			return result, unknownABDOutcome(operation, fmt.Errorf("HTTP %d: invalid error response: %w", response.StatusCode, err))
		}
		validOutcome := operation == "write" && (failure.Outcome == "not_applied" || failure.Outcome == "indeterminate") ||
			operation == "read" && failure.Outcome == "failed"
		if response.StatusCode < 400 || response.StatusCode > 599 || failure.Status != "error" ||
			strings.TrimSpace(failure.Error) == "" || !validOutcome ||
			(failure.Request != nil && (!validIdentity(failure.Request.Owner) || failure.Request.Sequence == 0)) {
			return result, unknownABDOutcome(operation, fmt.Errorf("HTTP %d: invalid error status, outcome, or request", response.StatusCode))
		}
		return result, fmt.Errorf("ABD %s: HTTP %d, outcome=%s: %s (do not retry automatically)", operation, response.StatusCode, failure.Outcome, failure.Error)
	}
	var completion struct {
		wire.ABDResult
		DurationNS *int64 `json:"duration_ns"`
	}
	if err := decodeABDResponse(body, &completion); err != nil {
		return result, unknownABDOutcome(operation, fmt.Errorf("decode completion: %w", err))
	}
	if completion.DurationNS == nil {
		return result, unknownABDOutcome(operation, errors.New("completion requires duration_ns"))
	}
	result = completion.ABDResult
	result.DurationNS = *completion.DurationNS
	if result.Status != "completed" || result.Operation != operation ||
		!validIdentity(result.NodeID) || !validIdentity(result.RunID) || !validClusterID(result.ClusterID) ||
		result.Request.Owner != result.NodeID || result.Request.Sequence == 0 || result.DurationNS < 0 || result.Register == nil {
		return result, unknownABDOutcome(operation, errors.New("response does not prove completion for a valid request"))
	}
	register := *result.Register
	if register.Tag.Counter == 0 && (register.Tag.WriterID != "" || register.Value != 0) ||
		register.Tag.Counter > 0 && !validIdentity(register.Tag.WriterID) {
		return result, unknownABDOutcome(operation, errors.New("completion contains an invalid register tag or initial value"))
	}
	if operation == "write" && (input.Value == nil || register.Value != *input.Value ||
		register.Tag.Counter == 0 || register.Tag.WriterID != result.NodeID) {
		return result, unknownABDOutcome(operation, errors.New("completion does not identify the requested write"))
	}
	return result, nil
}

func decodeABDResponse(body []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		return errors.New("response must contain exactly one JSON object")
	}
	return nil
}

func unknownABDOutcome(operation string, err error) error {
	return fmt.Errorf("ABD %s (outcome unknown; may be indeterminate; do not retry automatically): %w", operation, err)
}
