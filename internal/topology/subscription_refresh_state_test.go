package topology

import (
	"errors"
	"testing"
	"time"

	"github.com/Resinat/Resin/internal/subscription"
)

func TestScheduler_OnSubRefreshState_PersistsSuccessAndFailureSemantics(t *testing.T) {
	subMgr := NewSubscriptionManager()
	sub := subscription.NewSubscription("s1", "TestSub", "http://example.com", true, false)
	sub.SetFetchConfig(sub.URL(), int64(time.Hour))
	subMgr.Register(sub)

	pool := newTestPool(subMgr)
	body := makeSubscriptionJSON(`{"type":"shadowsocks","tag":"n","server":"1.1.1.1","server_port":443}`)

	type refreshCall struct {
		id        string
		checkedNs int64
		updatedNs *int64
		lastError string
	}
	var calls []refreshCall
	sched := NewSubscriptionScheduler(SchedulerConfig{
		SubManager: subMgr,
		Pool:       pool,
		Fetcher:    makeMockFetcher(body, nil),
		OnSubRefreshState: func(subID string, checkedNs int64, updatedNs *int64, lastError string) {
			calls = append(calls, refreshCall{subID, checkedNs, updatedNs, lastError})
		},
	})

	sched.UpdateSubscription(sub)
	if len(calls) != 1 {
		t.Fatalf("expected one success refresh-state call, got %d", len(calls))
	}
	if calls[0].id != "s1" || calls[0].checkedNs == 0 || calls[0].updatedNs == nil || *calls[0].updatedNs == 0 || calls[0].lastError != "" {
		t.Fatalf("unexpected success refresh-state call: %+v", calls[0])
	}
	successUpdated := *calls[0].updatedNs

	sched.Fetcher = makeMockFetcher(nil, errors.New("fetch failed"))
	sched.UpdateSubscription(sub)
	if len(calls) != 2 {
		t.Fatalf("expected success and failure refresh-state calls, got %d", len(calls))
	}
	if calls[1].id != "s1" || calls[1].checkedNs == 0 || calls[1].updatedNs != nil || calls[1].lastError != "fetch failed" {
		t.Fatalf("unexpected failure refresh-state call: %+v", calls[1])
	}
	if got := sub.LastUpdatedNs.Load(); got != successUpdated {
		t.Fatalf("failure should preserve LastUpdatedNs: got %d, want %d", got, successUpdated)
	}
}
