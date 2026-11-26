package main

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/VoltTech21/reostream/internal/baichuan"
	"github.com/VoltTech21/reostream/internal/hub"
	"github.com/VoltTech21/reostream/internal/stream"
)

// fakeHTTPShutdowner records when Shutdown was called, standing in for
// *http.Server so runShutdown can be tested without a real listener.
type fakeHTTPShutdowner struct {
	fn func(ctx context.Context) error
}

func (f fakeHTTPShutdowner) Shutdown(ctx context.Context) error {
	return f.fn(ctx)
}

type codecSink struct {
	mu        sync.Mutex
	frames    []baichuan.Frame
	connected int
}

func (s *codecSink) Frame(f baichuan.Frame) {
	s.mu.Lock()
	s.frames = append(s.frames, f)
	s.mu.Unlock()
}

func (s *codecSink) Connected() {
	s.mu.Lock()
	s.connected++
	s.mu.Unlock()
}

func TestBatteryPresencePlayerReloadChangesCodec(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sink := &codecSink{}
	name := "reload-codec-" + t.Name()
	p1 := batteryPresencePlayer(ctx, name, hub.New(1), func() stream.FrameSink { return sink })
	p1.Frame(baichuan.Frame{Kind: baichuan.FrameIFrame, Codec: "H265", Data: []byte{0, 0, 0, 1, 0x26}, Micros: 1})
	p2 := batteryPresencePlayer(ctx, name, hub.New(1), func() stream.FrameSink { return sink })
	if p2 != p1 {
		t.Fatal("reload replaced the registered player instead of resetting it")
	}
	p2.Frame(baichuan.Frame{Kind: baichuan.FrameIFrame, Codec: "H264", Data: []byte{0, 0, 0, 1, 0x65}, Micros: 2})
	sink.mu.Lock()
	defer sink.mu.Unlock()
	gotH264 := false
	for _, f := range sink.frames {
		if f.Codec == "H264" {
			gotH264 = true
			break
		}
	}
	if !gotH264 {
		t.Fatalf("frames after reload contain no H264: %+v", sink.frames)
	}
	if sink.connected != 1 {
		t.Fatalf("Connected calls = %d, want 1", sink.connected)
	}
}

func TestRunShutdownCancelsFirstThenWaitsThenShutsDownHTTP(t *testing.T) {
	// The order matters and is exactly what cost nine minutes of camera
	// footage when it was gotten backwards on 2026-09-08: cancel releases
	// every camera's session, HTTP must not go down until that has had its
	// chance to happen.
	var mu sync.Mutex
	var events []string
	record := func(s string) {
		mu.Lock()
		events = append(events, s)
		mu.Unlock()
	}

	runDone := make(chan error, 1)
	cancel := func() {
		record("cancel")
		// A real Supervisor.Run only returns once every stream has actually
		// stopped; this stands in for that happening promptly.
		runDone <- nil
	}
	srv := fakeHTTPShutdowner{fn: func(ctx context.Context) error {
		record("http_shutdown")
		return nil
	}}

	if err := runShutdown(cancel, runDone, srv, time.Second, time.Second); err != nil {
		t.Fatalf("runShutdown returned %v, want nil", err)
	}

	mu.Lock()
	defer mu.Unlock()
	want := []string{"cancel", "http_shutdown"}
	if len(events) != len(want) {
		t.Fatalf("events = %v, want %v", events, want)
	}
	for i := range want {
		if events[i] != want[i] {
			t.Fatalf("events = %v, want %v", events, want)
		}
	}
}

func TestRunShutdownGivesUpAfterGraceAndStillShutsDownHTTP(t *testing.T) {
	// A stream that never stops (a hung camera, a bug) must not hang the
	// whole process forever: grace bounds the wait, and HTTP comes down
	// anyway once it elapses. This is the bound in "wait up to grace",
	// checked directly rather than only asserted in a comment.
	runDone := make(chan error) // never sent: nothing acknowledges cancellation
	cancel := func() {}

	const grace = 100 * time.Millisecond
	start := time.Now()
	shutdownAt := make(chan time.Time, 1)
	srv := fakeHTTPShutdowner{fn: func(ctx context.Context) error {
		shutdownAt <- time.Now()
		return nil
	}}

	done := make(chan error, 1)
	go func() { done <- runShutdown(cancel, runDone, srv, grace, time.Second) }()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("runShutdown returned %v, want nil", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("runShutdown did not return; the grace timeout was not enforced")
	}

	elapsed := (<-shutdownAt).Sub(start)
	if elapsed < grace {
		t.Fatalf("HTTP shutdown ran after %v, before the %v grace period elapsed", elapsed, grace)
	}
	if elapsed > grace+500*time.Millisecond {
		t.Fatalf("HTTP shutdown ran %v after start, want close to the %v grace period", elapsed, grace)
	}
}

func TestRunShutdownReturnsTheHTTPShutdownError(t *testing.T) {
	runDone := make(chan error, 1)
	cancel := func() { runDone <- nil }
	wantErr := errors.New("listener already closed")
	srv := fakeHTTPShutdowner{fn: func(ctx context.Context) error { return wantErr }}

	if err := runShutdown(cancel, runDone, srv, time.Second, time.Second); !errors.Is(err, wantErr) {
		t.Fatalf("runShutdown returned %v, want %v", err, wantErr)
	}
}
