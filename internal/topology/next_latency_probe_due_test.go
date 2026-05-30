package topology

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/Resinat/Resin/internal/node"
)

func TestRecordLatencyUpdatesNextLatencyProbeDueAfterSuccessReset(t *testing.T) {
	pool := NewGlobalNodePool(PoolConfig{
		MaxLatencyTableEntries: 16,
		MaxConsecutiveFailures: func() int { return 3 },
		MaxLatencyTestInterval: func() time.Duration { return 10 * time.Second },
	})
	raw := json.RawMessage(`{"type":"ss","server":"next-due-success"}`)
	h := node.HashFromRawOptions(raw)
	pool.AddNodeFromSub(h, raw, "s1")
	entry, ok := pool.GetEntry(h)
	if !ok {
		t.Fatal("entry not found")
	}
	entry.FailureCount.Store(3)
	pool.RecordResult(h, true)
	before := time.Now().Add(10 * time.Second).UnixNano()
	latency := time.Millisecond
	pool.RecordLatency(h, "example.com", &latency)
	after := time.Now().Add(10 * time.Second).UnixNano()

	got := entry.NextLatencyProbeDue.Load()
	if got < before || got > after {
		t.Fatalf("next due after success: got %d, want between %d and %d", got, before, after)
	}
}

func TestRecordLatencyUpdatesNextLatencyProbeDueAfterFailureBackoff(t *testing.T) {
	pool := NewGlobalNodePool(PoolConfig{
		MaxLatencyTableEntries: 16,
		MaxConsecutiveFailures: func() int { return 3 },
		MaxLatencyTestInterval: func() time.Duration { return 10 * time.Second },
	})
	raw := json.RawMessage(`{"type":"ss","server":"next-due-failure"}`)
	h := node.HashFromRawOptions(raw)
	pool.AddNodeFromSub(h, raw, "s1")
	entry, ok := pool.GetEntry(h)
	if !ok {
		t.Fatal("entry not found")
	}
	pool.RecordResult(h, false) // post-result failure count = 1, so 2x interval
	before := time.Now().Add(20 * time.Second).UnixNano()
	pool.RecordLatency(h, "example.com", nil)
	after := time.Now().Add(20 * time.Second).UnixNano()

	got := entry.NextLatencyProbeDue.Load()
	if got < before || got > after {
		t.Fatalf("next due after failure: got %d, want between %d and %d", got, before, after)
	}
}
