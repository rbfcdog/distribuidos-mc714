package faults

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"
)

type stubTripper struct {
	calls int
}

func (s *stubTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	s.calls++
	return &http.Response{StatusCode: 204, Body: http.NoBody, Request: req}, nil
}

func TestWrapDisabledPassthrough(t *testing.T) {
	base := &stubTripper{}
	rt := Wrap(base, Config{})
	req, _ := http.NewRequest(http.MethodGet, "http://example", nil)
	if _, err := rt.RoundTrip(req); err != nil {
		t.Fatal(err)
	}
	if base.calls != 1 {
		t.Fatalf("calls=%d", base.calls)
	}
}

func TestWrapLoss(t *testing.T) {
	base := &stubTripper{}
	rt := Wrap(base, Config{LossRate: 1})
	req, _ := http.NewRequest(http.MethodGet, "http://example", nil)
	_, err := rt.RoundTrip(req)
	if !errors.Is(err, ErrInjectedLoss) {
		t.Fatalf("got %v", err)
	}
	if base.calls != 0 {
		t.Fatal("loss should skip base transport")
	}
}

func TestWrapDelayRespectsCancel(t *testing.T) {
	base := &stubTripper{}
	rt := Wrap(base, Config{Delay: time.Second})
	ctx, cancel := context.WithCancel(context.Background())
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "http://example", nil)
	cancel()
	_, err := rt.RoundTrip(req)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v", err)
	}
}
