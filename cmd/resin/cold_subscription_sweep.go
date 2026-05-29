package main

import (
	"context"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Resinat/Resin/internal/node"
	"github.com/Resinat/Resin/internal/subscription"
	"github.com/Resinat/Resin/internal/topology"
)

type coldNodeCandidateStore interface {
	LoadDueColdNodeCandidates(nowNs int64, interval time.Duration, limit int) ([]topology.ColdNodeCandidate, error)
}

type coldNodeChecker interface {
	Check(topology.ColdNodeCandidate)
}

type coldNodeBatchChecker interface {
	CheckBatch(context.Context, []topology.ColdNodeCandidate) int
}

type coldSubscriptionNodeSweepRunnerConfig struct {
	store         coldNodeCandidateStore
	checker       coldNodeChecker
	pool          *topology.GlobalNodePool
	subManager    *topology.SubscriptionManager
	sweepInterval time.Duration
	batchSize     int
	now           func() time.Time
}

type coldSubscriptionNodeSweepRunner struct {
	store         coldNodeCandidateStore
	checker       coldNodeChecker
	pool          *topology.GlobalNodePool
	subManager    *topology.SubscriptionManager
	sweepInterval time.Duration
	batchSize     int
	now           func() time.Time

	triggerCh chan struct{}
	stopCh    chan struct{}
	cancel    context.CancelFunc
	started   atomic.Bool
	stopped   atomic.Bool
	wg        sync.WaitGroup
}

func newColdSubscriptionNodeSweepRunner(cfg coldSubscriptionNodeSweepRunnerConfig) *coldSubscriptionNodeSweepRunner {
	sweepInterval := cfg.sweepInterval
	if sweepInterval <= 0 {
		sweepInterval = coldSubscriptionNodeSweepInterval
	}
	batchSize := cfg.batchSize
	if batchSize <= 0 {
		batchSize = coldSubscriptionNodeSweepBatchSize
	}
	now := cfg.now
	if now == nil {
		now = time.Now
	}
	return &coldSubscriptionNodeSweepRunner{
		store:         cfg.store,
		checker:       cfg.checker,
		pool:          cfg.pool,
		subManager:    cfg.subManager,
		sweepInterval: sweepInterval,
		batchSize:     batchSize,
		now:           now,
		triggerCh:     make(chan struct{}, 1),
		stopCh:        make(chan struct{}),
	}
}

func (r *coldSubscriptionNodeSweepRunner) Start() {
	if r == nil || !r.started.CompareAndSwap(false, true) {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	r.cancel = cancel
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		r.loop(ctx)
	}()
}

func (r *coldSubscriptionNodeSweepRunner) Stop() {
	if r == nil || !r.stopped.CompareAndSwap(false, true) {
		return
	}
	if r.cancel != nil {
		r.cancel()
	}
	close(r.stopCh)
	r.wg.Wait()
}

func (r *coldSubscriptionNodeSweepRunner) TriggerColdNodeSweep(reason string) bool {
	if r == nil || r.stopped.Load() {
		return false
	}
	select {
	case r.triggerCh <- struct{}{}:
	default:
	}
	return true
}

func (r *coldSubscriptionNodeSweepRunner) loop(ctx context.Context) {
	ticker := time.NewTicker(r.sweepInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-r.stopCh:
			return
		case <-ticker.C:
			r.runSweep(ctx)
		case <-r.triggerCh:
			r.runSweep(ctx)
		}
	}
}

func (r *coldSubscriptionNodeSweepRunner) runSweep(ctx context.Context) {
	if r == nil || r.store == nil || r.checker == nil || r.batchSize <= 0 {
		return
	}

	skippedCandidates := make(map[coldNodeCandidateKey]struct{})
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		limit := r.batchSize + len(skippedCandidates)
		candidates, err := r.store.LoadDueColdNodeCandidates(r.now().UnixNano(), r.sweepInterval, limit)
		if err != nil {
			log.Printf("cold subscription node sweep: load candidates: %v", err)
			return
		}
		if len(candidates) == 0 {
			return
		}

		toCheck := make([]topology.ColdNodeCandidate, 0, r.batchSize)
		for _, candidate := range candidates {
			key := coldNodeCandidateKey{subscriptionID: candidate.SubscriptionID, hash: candidate.Hash}
			if _, ok := skippedCandidates[key]; ok {
				continue
			}
			if r.shouldSkipCandidate(candidate) {
				skippedCandidates[key] = struct{}{}
				continue
			}
			toCheck = append(toCheck, candidate)
			if len(toCheck) >= r.batchSize {
				break
			}
		}

		if len(toCheck) == 0 {
			if len(candidates) < limit {
				return
			}
			continue
		}

		if checked := r.dispatchChecks(ctx, toCheck); checked < len(toCheck) {
			return
		}
	}
}

func (r *coldSubscriptionNodeSweepRunner) dispatchChecks(ctx context.Context, candidates []topology.ColdNodeCandidate) int {
	if len(candidates) == 0 || r == nil || r.checker == nil {
		return 0
	}
	if batchChecker, ok := r.checker.(coldNodeBatchChecker); ok {
		return batchChecker.CheckBatch(ctx, candidates)
	}
	checked := 0
	for _, candidate := range candidates {
		select {
		case <-ctx.Done():
			return checked
		default:
		}
		r.checker.Check(candidate)
		checked++
	}
	return checked
}

func (r *coldSubscriptionNodeSweepRunner) shouldSkipCandidate(candidate topology.ColdNodeCandidate) bool {
	if r == nil || r.subManager == nil {
		return false
	}
	sub := r.subManager.Lookup(candidate.SubscriptionID)
	if sub == nil || !sub.Enabled() {
		return true
	}
	if r.pool == nil {
		return false
	}
	entry, ok := r.pool.GetEntry(candidate.Hash)
	if !ok || entry == nil || entry.IsCircuitOpen() || !entry.HasOutbound() || !entry.HasLatency() {
		return false
	}
	if managed, ok := sub.ManagedNodes().LoadNode(candidate.Hash); ok && !managed.Evicted {
		return true
	}
	sub.ManagedNodes().StoreNode(candidate.Hash, subscription.ManagedNode{Tags: append([]string(nil), candidate.Tags...)})
	r.pool.AddNodeFromSub(candidate.Hash, candidate.RawOptions, candidate.SubscriptionID)
	return true
}

type coldNodeCandidateKey struct {
	subscriptionID string
	hash           node.Hash
}

var _ topology.ColdNodeSweepTrigger = (*coldSubscriptionNodeSweepRunner)(nil)
