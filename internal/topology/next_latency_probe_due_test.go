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

func TestRecordResultFailureRefreshesExistingNextLatencyProbeDue(t *testing.T) {
	pool := NewGlobalNodePool(PoolConfig{
		MaxLatencyTableEntries: 16,
		MaxConsecutiveFailures: func() int { return 3 },
		MaxLatencyTestInterval: func() time.Duration { return 10 * time.Second },
	})
	raw := json.RawMessage(`{"type":"ss","server":"next-due-record-result-failure"}`)
	h := node.HashFromRawOptions(raw)
	pool.AddNodeFromSub(h, raw, "s1")
	entry, ok := pool.GetEntry(h)
	if !ok {
		t.Fatal("entry not found")
	}
	entry.LastLatencyProbeAttempt.Store(time.Now().Add(-time.Second).UnixNano())
	entry.NextLatencyProbeDue.Store(time.Now().Add(10 * time.Second).UnixNano())

	before := time.Now().Add(20 * time.Second).UnixNano()
	pool.RecordResult(h, false)
	after := time.Now().Add(20 * time.Second).UnixNano()

	got := entry.NextLatencyProbeDue.Load()
	if got < before || got > after {
		t.Fatalf("next due after standalone failure: got %d, want between %d and %d", got, before, after)
	}
}

func TestRecordResultSuccessRefreshesExistingNextLatencyProbeDueAfterClearingFailure(t *testing.T) {
	pool := NewGlobalNodePool(PoolConfig{
		MaxLatencyTableEntries: 16,
		MaxConsecutiveFailures: func() int { return 3 },
		MaxLatencyTestInterval: func() time.Duration { return 10 * time.Second },
	})
	raw := json.RawMessage(`{"type":"ss","server":"next-due-record-result-success"}`)
	h := node.HashFromRawOptions(raw)
	pool.AddNodeFromSub(h, raw, "s1")
	entry, ok := pool.GetEntry(h)
	if !ok {
		t.Fatal("entry not found")
	}
	entry.FailureCount.Store(2)
	entry.LastLatencyProbeAttempt.Store(time.Now().Add(-time.Second).UnixNano())
	entry.NextLatencyProbeDue.Store(time.Now().Add(time.Hour).UnixNano())

	before := time.Now().Add(10 * time.Second).UnixNano()
	pool.RecordResult(h, true)
	after := time.Now().Add(10 * time.Second).UnixNano()

	got := entry.NextLatencyProbeDue.Load()
	if got < before || got > after {
		t.Fatalf("next due after standalone success: got %d, want between %d and %d", got, before, after)
	}
}

func TestRecordResultDirtyCallbackSeesRefreshedNextLatencyProbeDue(t *testing.T) {
	raw := json.RawMessage(`{"type":"ss","server":"next-due-record-result-dirty"}`)
	h := node.HashFromRawOptions(raw)
	var entry *node.NodeEntry
	callbackSeen := int64(0)
	pool := NewGlobalNodePool(PoolConfig{
		MaxLatencyTableEntries: 16,
		MaxConsecutiveFailures: func() int { return 3 },
		MaxLatencyTestInterval: func() time.Duration { return 10 * time.Second },
		OnNodeDynamicChanged: func(hash node.Hash) {
			if hash != h || entry == nil {
				return
			}
			callbackSeen = entry.NextLatencyProbeDue.Load()
		},
	})
	pool.AddNodeFromSub(h, raw, "s1")
	var ok bool
	entry, ok = pool.GetEntry(h)
	if !ok {
		t.Fatal("entry not found")
	}
	entry.LastLatencyProbeAttempt.Store(time.Now().Add(-time.Second).UnixNano())
	entry.NextLatencyProbeDue.Store(1)

	pool.RecordResult(h, false)

	if callbackSeen <= time.Now().UnixNano() {
		t.Fatalf("dynamic dirty callback saw stale next due %d, want refreshed future due", callbackSeen)
	}
}
