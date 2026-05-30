package state

import (
	"fmt"
	"log"
	"time"

	"github.com/Resinat/Resin/internal/model"
	"github.com/Resinat/Resin/internal/node"
	"github.com/Resinat/Resin/internal/topology"
)

// NodeLatencyDirtyKey is the composite key for the node_latency dirty set.
type NodeLatencyDirtyKey = model.NodeLatencyKey

// LeaseDirtyKey is the composite key for the leases dirty set.
type LeaseDirtyKey = model.LeaseKey

// SubscriptionNodeDirtyKey is the composite key for the subscription_nodes dirty set.
type SubscriptionNodeDirtyKey = model.SubscriptionNodeKey

// CacheReaders provides callbacks for reading current in-memory values at flush time.
// If a reader returns nil for a key marked OpUpsert, the key is
// treated as a delete (the object was removed between mark and flush).
type CacheReaders struct {
	ReadNodeStatic       func(hash string) *model.NodeStatic
	ReadNodeDynamic      func(hash string) *model.NodeDynamic
	ReadNodeLatency      func(key NodeLatencyDirtyKey) *model.NodeLatency
	ReadLease            func(key LeaseDirtyKey) *model.Lease
	ReadSubscriptionNode func(key SubscriptionNodeDirtyKey) *model.SubscriptionNode
}

// StateEngine is the single write entry point for all persistence operations.
// Strong-persist data (config, platforms, subscriptions, rules) goes through
// transactional writes to state.db. Weak-persist data (nodes, leases) is
// marked dirty and batch-flushed to cache.db.
type StateEngine struct {
	*StateRepo
	*CacheRepo

	dirtyNodesStatic       *DirtySet[string]
	dirtyNodesDynamic      *DirtySet[string]
	dirtyNodeLatency       *DirtySet[NodeLatencyDirtyKey]
	dirtyLeases            *DirtySet[LeaseDirtyKey]
	dirtySubscriptionNodes *DirtySet[SubscriptionNodeDirtyKey]
}

// newStateEngine creates a StateEngine with the given repos.
func newStateEngine(stateRepo *StateRepo, cacheRepo *CacheRepo) *StateEngine {
	return &StateEngine{
		StateRepo:              stateRepo,
		CacheRepo:              cacheRepo,
		dirtyNodesStatic:       NewDirtySet[string](),
		dirtyNodesDynamic:      NewDirtySet[string](),
		dirtyNodeLatency:       NewDirtySet[NodeLatencyDirtyKey](),
		dirtyLeases:            NewDirtySet[LeaseDirtyKey](),
		dirtySubscriptionNodes: NewDirtySet[SubscriptionNodeDirtyKey](),
	}
}

// --- Weak-persist methods (dirty-mark only) ---

func (e *StateEngine) MarkNodeStatic(hash string)        { e.dirtyNodesStatic.MarkUpsert(hash) }
func (e *StateEngine) MarkNodeStaticDelete(hash string)  { e.dirtyNodesStatic.MarkDelete(hash) }
func (e *StateEngine) MarkNodeDynamic(hash string)       { e.dirtyNodesDynamic.MarkUpsert(hash) }
func (e *StateEngine) MarkNodeDynamicDelete(hash string) { e.dirtyNodesDynamic.MarkDelete(hash) }

func (e *StateEngine) MarkNodeLatency(nodeHash, domain string) {
	e.dirtyNodeLatency.MarkUpsert(NodeLatencyDirtyKey{NodeHash: nodeHash, Domain: domain})
}
func (e *StateEngine) MarkNodeLatencyDelete(nodeHash, domain string) {
	e.dirtyNodeLatency.MarkDelete(NodeLatencyDirtyKey{NodeHash: nodeHash, Domain: domain})
}

func (e *StateEngine) MarkLease(platformID, account string) {
	e.dirtyLeases.MarkUpsert(LeaseDirtyKey{PlatformID: platformID, Account: account})
}
func (e *StateEngine) MarkLeaseDelete(platformID, account string) {
	e.dirtyLeases.MarkDelete(LeaseDirtyKey{PlatformID: platformID, Account: account})
}

func (e *StateEngine) MarkSubscriptionNode(subID, nodeHash string) {
	e.dirtySubscriptionNodes.MarkUpsert(SubscriptionNodeDirtyKey{SubscriptionID: subID, NodeHash: nodeHash})
}
func (e *StateEngine) MarkSubscriptionNodeDelete(subID, nodeHash string) {
	e.dirtySubscriptionNodes.MarkDelete(SubscriptionNodeDirtyKey{SubscriptionID: subID, NodeHash: nodeHash})
}

func (e *StateEngine) enabledSubscriptionIDs() ([]string, error) {
	subs, err := e.ListSubscriptions()
	if err != nil {
		return nil, err
	}
	ids := make([]string, 0, len(subs))
	for _, sub := range subs {
		if sub.Enabled {
			ids = append(ids, sub.ID)
		}
	}
	return ids, nil
}

func (e *StateEngine) LoadDueColdNodeCandidates(nowNs int64, interval time.Duration, limit int) ([]topology.ColdNodeCandidate, error) {
	ids, err := e.enabledSubscriptionIDs()
	if err != nil {
		return nil, err
	}
	if len(ids) == 0 {
		return nil, nil
	}
	return e.CacheRepo.loadDueColdNodeCandidates(nowNs, interval, limit, ids)
}

func (e *StateEngine) LoadCurrentColdNodeRelations(hash node.Hash) ([]topology.ColdNodeRelation, error) {
	ids, err := e.enabledSubscriptionIDs()
	if err != nil {
		return nil, err
	}
	if len(ids) == 0 {
		return nil, nil
	}
	return e.CacheRepo.loadCurrentColdNodeRelations(hash, ids)
}

func (e *StateEngine) UpdateSubscriptionRefreshState(id string, checkedNs int64, updatedNs *int64, lastError string) error {
	return e.StateRepo.UpdateSubscriptionRefreshState(id, checkedNs, updatedNs, lastError)
}

// DirtyCount returns the total number of dirty entries across all sets.
func (e *StateEngine) DirtyCount() int {
	return e.dirtyNodesStatic.Len() +
		e.dirtyNodesDynamic.Len() +
		e.dirtyNodeLatency.Len() +
		e.dirtyLeases.Len() +
		e.dirtySubscriptionNodes.Len()
}

// classifyDirtySet splits a drained dirty-set snapshot into upsert values and
// delete keys. For OpUpsert entries, the reader is called to fetch the current
// in-memory value; a nil return is treated as a delete.
func classifyDirtySet[K comparable, V any](
	drained map[K]DirtyOp,
	reader func(K) *V,
) (upserts []V, deletes []K) {
	for key, op := range drained {
		if op == OpDelete {
			deletes = append(deletes, key)
			continue
		}
		v := reader(key)
		if v == nil {
			deletes = append(deletes, key)
		} else {
			upserts = append(upserts, *v)
		}
	}
	return
}

// FlushDirtySets drains all dirty sets, reads current values via readers,
// and batch-writes to cache.db in a single transaction.
// On failure, undrained entries are merged back.
func (e *StateEngine) FlushDirtySets(readers CacheReaders) error {
	return e.flushDirtySets(readers, true)
}

// FlushNodeDirtySets drains only node/inventory dirty sets, leaving leases for
// the regular cache flush worker. This is useful for cold-node checks, which
// need to persist probe outcomes but do not have a router/lease reader.
func (e *StateEngine) FlushNodeDirtySets(readers CacheReaders) error {
	return e.flushDirtySets(readers, false)
}

func (e *StateEngine) flushDirtySets(readers CacheReaders, includeLeases bool) error {
	// Drain all sets atomically (each set is individually atomic).
	drainedStatic := e.dirtyNodesStatic.Drain()
	drainedSubNodes := e.dirtySubscriptionNodes.Drain()
	drainedDynamic := e.dirtyNodesDynamic.Drain()
	drainedLatency := e.dirtyNodeLatency.Drain()
	drainedLeases := map[LeaseDirtyKey]DirtyOp(nil)
	if includeLeases {
		drainedLeases = e.dirtyLeases.Drain()
	}

	// Re-merge helper on failure.
	remerge := func() {
		e.dirtyNodesStatic.Merge(drainedStatic)
		e.dirtySubscriptionNodes.Merge(drainedSubNodes)
		e.dirtyNodesDynamic.Merge(drainedDynamic)
		e.dirtyNodeLatency.Merge(drainedLatency)
		if includeLeases {
			e.dirtyLeases.Merge(drainedLeases)
		}
	}

	// Classify each dirty set into upsert values and delete keys.
	upsertStatic, deleteStatic := classifyDirtySet(drainedStatic, readers.ReadNodeStatic)
	upsertSubNodes, deleteSubNodes := classifyDirtySet(drainedSubNodes, readers.ReadSubscriptionNode)
	upsertDynamic, deleteDynamic := classifyDirtySet(drainedDynamic, readers.ReadNodeDynamic)
	upsertLatency, deleteLatency := classifyDirtySet(drainedLatency, readers.ReadNodeLatency)
	var upsertLeases []model.Lease
	var deleteLeases []LeaseDirtyKey
	if includeLeases {
		upsertLeases, deleteLeases = classifyDirtySet(drainedLeases, readers.ReadLease)
	}

	// Execute all writes in a single transaction.
	if err := e.CacheRepo.FlushTx(FlushOps{
		UpsertNodesStatic:       upsertStatic,
		DeleteNodesStatic:       deleteStatic,
		UpsertSubscriptionNodes: upsertSubNodes,
		DeleteSubscriptionNodes: deleteSubNodes,
		UpsertNodesDynamic:      upsertDynamic,
		DeleteNodesDynamic:      deleteDynamic,
		UpsertNodeLatency:       upsertLatency,
		DeleteNodeLatency:       deleteLatency,
		UpsertLeases:            upsertLeases,
		DeleteLeases:            deleteLeases,
	}); err != nil {
		remerge()
		return fmt.Errorf("flush: %w", err)
	}

	leaseCount := 0
	if includeLeases {
		leaseCount = len(drainedLeases)
	}
	log.Printf("[state] flushed dirty sets: static=%d, sub_nodes=%d, dynamic=%d, latency=%d, leases=%d",
		len(drainedStatic), len(drainedSubNodes), len(drainedDynamic), len(drainedLatency), leaseCount)
	return nil
}
