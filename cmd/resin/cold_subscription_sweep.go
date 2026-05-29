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
	store             coldNodeCandidateStore
	checker           coldNodeChecker
	pool              *topology.GlobalNodePool
	subManager        *topology.SubscriptionManager
	relationValidator topology.ColdNodeRelationValidator
	sweepInterval     time.Duration
	batchSize         int
	now               func() time.Time
}

type coldSubscriptionNodeSweepRunner struct {
	store             coldNodeCandidateStore
	checker           coldNodeChecker
	pool              *topology.GlobalNodePool
	subManager        *topology.SubscriptionManager
	relationValidator topology.ColdNodeRelationValidator
	sweepInterval     time.Duration
	batchSize         int
	now               func() time.Time

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
	relationValidator := cfg.relationValidator
	if relationValidator == nil {
		if validator, ok := cfg.store.(topology.ColdNodeRelationValidator); ok {
			relationValidator = validator
		}
	}
	return &coldSubscriptionNodeSweepRunner{
		store:             cfg.store,
		checker:           cfg.checker,
		pool:              cfg.pool,
		subManager:        cfg.subManager,
		relationValidator: relationValidator,
		sweepInterval:     sweepInterval,
		batchSize:         batchSize,
		now:               now,
		triggerCh:         make(chan struct{}, 1),
		stopCh:            make(chan struct{}),
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
			key := coldNodeCandidateKey{hash: candidate.Hash}
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
	relations := candidate.EffectiveRelations()
	if len(relations) == 0 {
		return true
	}
	type activeColdNodeRelation struct {
		relation topology.ColdNodeRelation
		sub      *subscription.Subscription
	}
	activeRelations := make([]activeColdNodeRelation, 0, len(relations))
	for _, relation := range relations {
		sub := r.subManager.Lookup(relation.SubscriptionID)
		if sub != nil && sub.Enabled() && r.isCurrentColdRelation(relation.SubscriptionID, candidate.Hash) {
			activeRelations = append(activeRelations, activeColdNodeRelation{relation: relation, sub: sub})
		}
	}
	if len(activeRelations) == 0 {
		return true
	}
	if r.pool == nil {
		return false
	}
	entry, ok := r.pool.GetEntry(candidate.Hash)
	if !ok || entry == nil || entry.IsCircuitOpen() || !entry.HasOutbound() || !entry.HasLatency() {
		return false
	}
	allRestored := true
	for _, active := range activeRelations {
		if !r.currentColdRelationRestored(candidate, active.relation, active.sub) {
			allRestored = false
			break
		}
	}
	if allRestored {
		return true
	}
	for _, active := range activeRelations {
		r.attachCurrentColdRelation(candidate, active.relation, active.sub)
	}
	return true
}

func (r *coldSubscriptionNodeSweepRunner) currentColdRelationRestored(candidate topology.ColdNodeCandidate, relation topology.ColdNodeRelation, sub *subscription.Subscription) bool {
	if sub == nil {
		return false
	}
	restored := false
	sub.WithOpLock(func() {
		current := r.subManager.Lookup(relation.SubscriptionID)
		if current == nil || current != sub || !current.Enabled() || !r.isCurrentColdRelation(relation.SubscriptionID, candidate.Hash) {
			return
		}
		managed, ok := current.ManagedNodes().LoadNode(candidate.Hash)
		restored = ok && !managed.Evicted
	})
	return restored
}

func (r *coldSubscriptionNodeSweepRunner) attachCurrentColdRelation(candidate topology.ColdNodeCandidate, relation topology.ColdNodeRelation, sub *subscription.Subscription) bool {
	if sub == nil || r.pool == nil {
		return false
	}
	attached := false
	sub.WithOpLock(func() {
		current := r.subManager.Lookup(relation.SubscriptionID)
		if current == nil || current != sub || !current.Enabled() || !r.isCurrentColdRelation(relation.SubscriptionID, candidate.Hash) {
			return
		}
		current.ManagedNodes().StoreNode(candidate.Hash, subscription.ManagedNode{Tags: append([]string(nil), relation.Tags...)})
		r.pool.AddNodeFromSub(candidate.Hash, candidate.RawOptions, relation.SubscriptionID)
		attached = true
	})
	return attached
}

func (r *coldSubscriptionNodeSweepRunner) isCurrentColdRelation(subID string, hash node.Hash) bool {
	if r == nil || r.relationValidator == nil {
		return true
	}
	return r.relationValidator.IsColdNodeRelationCurrent(subID, hash)
}

type coldNodeCandidateKey struct {
	hash node.Hash
}

var _ topology.ColdNodeSweepTrigger = (*coldSubscriptionNodeSweepRunner)(nil)
