package service

import (
	"net/netip"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Resinat/Resin/internal/config"
	"github.com/Resinat/Resin/internal/geoip"
	"github.com/Resinat/Resin/internal/model"
	"github.com/Resinat/Resin/internal/node"
	"github.com/Resinat/Resin/internal/platform"
	"github.com/Resinat/Resin/internal/probe"
	"github.com/Resinat/Resin/internal/state"
	"github.com/Resinat/Resin/internal/subscription"
	"github.com/Resinat/Resin/internal/testutil"
	"github.com/Resinat/Resin/internal/topology"
)

func newNodeListTestPool(subMgr *topology.SubscriptionManager) *topology.GlobalNodePool {
	return topology.NewGlobalNodePool(topology.PoolConfig{
		SubLookup:              subMgr.Lookup,
		GeoLookup:              func(netip.Addr) string { return "us" },
		MaxLatencyTableEntries: 16,
		MaxConsecutiveFailures: func() int { return 3 },
		LatencyDecayWindow:     func() time.Duration { return 10 * time.Minute },
	})
}

func addRoutableNodeForSubscription(
	t *testing.T,
	pool *topology.GlobalNodePool,
	sub *subscription.Subscription,
	raw []byte,
	egressIP string,
) node.Hash {
	return addRoutableNodeForSubscriptionWithTag(t, pool, sub, raw, egressIP, "tag")
}

func addRoutableNodeForSubscriptionWithTag(
	t *testing.T,
	pool *topology.GlobalNodePool,
	sub *subscription.Subscription,
	raw []byte,
	egressIP string,
	tag string,
) node.Hash {
	t.Helper()

	hash := node.HashFromRawOptions(raw)
	pool.AddNodeFromSub(hash, raw, sub.ID)
	sub.ManagedNodes().StoreNode(hash, subscription.ManagedNode{Tags: []string{tag}})

	entry, ok := pool.GetEntry(hash)
	if !ok {
		t.Fatalf("node %s not found after add", hash.Hex())
	}
	entry.SetEgressIP(netip.MustParseAddr(egressIP))
	if entry.LatencyTable == nil {
		t.Fatalf("node %s latency table not initialized", hash.Hex())
	}
	entry.LatencyTable.Update("example.com", 25*time.Millisecond, 10*time.Minute)
	ob := testutil.NewNoopOutbound()
	entry.Outbound.Store(&ob)
	pool.RecordResult(hash, true)
	pool.NotifyNodeDirty(hash)
	return hash
}

func TestListNodes_PlatformAndSubscriptionFiltersReturnIntersection(t *testing.T) {
	subMgr := topology.NewSubscriptionManager()
	pool := newNodeListTestPool(subMgr)

	plat := platform.NewPlatform("plat-1", "plat", nil, nil)
	pool.RegisterPlatform(plat)

	subA := subscription.NewSubscription("sub-a", "sub-a", "https://example.com/a", true, false)
	subB := subscription.NewSubscription("sub-b", "sub-b", "https://example.com/b", true, false)
	subMgr.Register(subA)
	subMgr.Register(subB)

	hashA := addRoutableNodeForSubscription(
		t,
		pool,
		subA,
		[]byte(`{"type":"ss","server":"1.1.1.1","port":443}`),
		"203.0.113.10",
	)
	_ = addRoutableNodeForSubscription(
		t,
		pool,
		subB,
		[]byte(`{"type":"ss","server":"2.2.2.2","port":443}`),
		"203.0.113.11",
	)

	cp := &ControlPlaneService{
		Pool:   pool,
		SubMgr: subMgr,
		GeoIP:  &geoip.Service{},
	}
	filters := NodeFilters{
		PlatformID:     &plat.ID,
		SubscriptionID: &subA.ID,
	}

	nodes, err := cp.ListNodes(filters)
	if err != nil {
		t.Fatalf("ListNodes: %v", err)
	}
	if len(nodes) != 1 {
		t.Fatalf("intersection size = %d, want 1", len(nodes))
	}
	if nodes[0].NodeHash != hashA.Hex() {
		t.Fatalf("intersection node hash = %q, want %q", nodes[0].NodeHash, hashA.Hex())
	}
}

func TestListNodes_SubscriptionFilterSkipsStaleManagedNodes(t *testing.T) {
	subMgr := topology.NewSubscriptionManager()
	pool := newNodeListTestPool(subMgr)

	sub := subscription.NewSubscription("sub-a", "sub-a", "https://example.com/a", true, false)
	subMgr.Register(sub)

	staleHash := node.HashFromRawOptions([]byte(`{"type":"ss","server":"9.9.9.9","port":443}`))
	sub.ManagedNodes().StoreNode(staleHash, subscription.ManagedNode{Tags: []string{"stale"}})

	cp := &ControlPlaneService{
		Pool:   pool,
		SubMgr: subMgr,
		GeoIP:  &geoip.Service{},
	}
	filters := NodeFilters{
		SubscriptionID: &sub.ID,
	}

	nodes, err := cp.ListNodes(filters)
	if err != nil {
		t.Fatalf("ListNodes with stale hash: %v", err)
	}
	if len(nodes) != 0 {
		t.Fatalf("nodes with stale managed hash = %d, want 0", len(nodes))
	}

	liveHash := addRoutableNodeForSubscription(
		t,
		pool,
		sub,
		[]byte(`{"type":"ss","server":"1.1.1.1","port":443}`),
		"203.0.113.20",
	)

	nodes, err = cp.ListNodes(filters)
	if err != nil {
		t.Fatalf("ListNodes with stale+live hashes: %v", err)
	}
	if len(nodes) != 1 {
		t.Fatalf("nodes with stale+live hashes = %d, want 1", len(nodes))
	}
	if nodes[0].NodeHash != liveHash.Hex() {
		t.Fatalf("live node hash = %q, want %q", nodes[0].NodeHash, liveHash.Hex())
	}
}

func TestListNodes_SubscriptionFilterSkipsEvictedManagedNodes(t *testing.T) {
	subMgr := topology.NewSubscriptionManager()
	pool := newNodeListTestPool(subMgr)

	subA := subscription.NewSubscription("sub-a", "sub-a", "https://example.com/a", true, false)
	subB := subscription.NewSubscription("sub-b", "sub-b", "https://example.com/b", true, false)
	subMgr.Register(subA)
	subMgr.Register(subB)

	raw := []byte(`{"type":"ss","server":"7.7.7.7","port":443}`)
	hash := addRoutableNodeForSubscriptionWithTag(t, pool, subA, raw, "203.0.113.40", "a-tag")
	pool.AddNodeFromSub(hash, raw, subB.ID)
	subB.ManagedNodes().StoreNode(hash, subscription.ManagedNode{Tags: []string{"b-tag"}})

	managedA, ok := subA.ManagedNodes().LoadNode(hash)
	if !ok {
		t.Fatal("subA managed node missing before eviction")
	}
	managedA.Evicted = true
	subA.ManagedNodes().StoreNode(hash, managedA)
	pool.RemoveNodeFromSub(hash, subA.ID)

	cp := &ControlPlaneService{
		Pool:   pool,
		SubMgr: subMgr,
		GeoIP:  &geoip.Service{},
	}

	filtersA := NodeFilters{SubscriptionID: &subA.ID}
	nodesA, err := cp.ListNodes(filtersA)
	if err != nil {
		t.Fatalf("ListNodes subA: %v", err)
	}
	if len(nodesA) != 0 {
		t.Fatalf("subA filtered nodes = %d, want 0", len(nodesA))
	}

	filtersB := NodeFilters{SubscriptionID: &subB.ID}
	nodesB, err := cp.ListNodes(filtersB)
	if err != nil {
		t.Fatalf("ListNodes subB: %v", err)
	}
	if len(nodesB) != 1 || nodesB[0].NodeHash != hash.Hex() {
		t.Fatalf("subB filtered nodes = %+v, want [%s]", nodesB, hash.Hex())
	}
}

func TestGetNode_TagIncludesSubscriptionNamePrefix(t *testing.T) {
	subMgr := topology.NewSubscriptionManager()
	pool := newNodeListTestPool(subMgr)

	sub := subscription.NewSubscription("sub-a", "sub-a", "https://example.com/a", true, false)
	subMgr.Register(sub)

	hash := addRoutableNodeForSubscription(
		t,
		pool,
		sub,
		[]byte(`{"type":"ss","server":"1.1.1.1","port":443}`),
		"203.0.113.30",
	)

	cp := &ControlPlaneService{
		Pool:   pool,
		SubMgr: subMgr,
		GeoIP:  &geoip.Service{},
	}

	got, err := cp.GetNode(hash.Hex())
	if err != nil {
		t.Fatalf("GetNode: %v", err)
	}
	if len(got.Tags) != 1 {
		t.Fatalf("tags len = %d, want 1", len(got.Tags))
	}
	if got.Tags[0].Tag != "sub-a/tag" {
		t.Fatalf("tag = %q, want %q", got.Tags[0].Tag, "sub-a/tag")
	}
}

func TestListNodes_ActiveTrueUsesRuntimePoolActiveFalseMergesInventoryAndRuntimeWithRuntimePriority(t *testing.T) {
	dir := t.TempDir()
	engine, closer, err := state.PersistenceBootstrap(filepath.Join(dir, "state"), filepath.Join(dir, "cache"))
	if err != nil {
		t.Fatalf("PersistenceBootstrap: %v", err)
	}
	t.Cleanup(func() { _ = closer.Close() })

	subMgr := topology.NewSubscriptionManager()
	pool := newNodeListTestPool(subMgr)
	sub := subscription.NewSubscription("sub-a", "sub-a", "https://example.com/a", true, false)
	subMgr.Register(sub)

	overlapRaw := []byte(`{"type":"ss","server":"1.1.1.1","port":443}`)
	memoryOnlyRaw := []byte(`{"type":"ss","server":"3.3.3.3","port":443}`)
	coldRaw := []byte(`{"type":"ss","server":"2.2.2.2","port":443}`)
	overlapHash := addRoutableNodeForSubscriptionWithTag(t, pool, sub, overlapRaw, "203.0.113.30", "runtime-overlap")
	memoryOnlyHash := addRoutableNodeForSubscriptionWithTag(t, pool, sub, memoryOnlyRaw, "203.0.113.31", "runtime-only")
	coldHash := node.HashFromRawOptions(coldRaw)

	if err := engine.ReplaceSubscriptionRefresh(sub.ID,
		[]model.NodeStatic{
			{Hash: overlapHash.Hex(), RawOptions: overlapRaw, CreatedAtNs: time.Now().Add(-2 * time.Minute).UnixNano()},
			{Hash: coldHash.Hex(), RawOptions: coldRaw, CreatedAtNs: time.Now().Add(-time.Minute).UnixNano()},
		},
		[]model.SubscriptionNode{
			{SubscriptionID: sub.ID, NodeHash: overlapHash.Hex(), Tags: []string{"inventory-overlap"}},
			{SubscriptionID: sub.ID, NodeHash: coldHash.Hex(), Tags: []string{"cold"}},
		},
		nil,
	); err != nil {
		t.Fatalf("ReplaceSubscriptionRefresh: %v", err)
	}

	cp := &ControlPlaneService{Engine: engine, Pool: pool, SubMgr: subMgr, GeoIP: &geoip.Service{}}
	activeOnly := true
	activeNodes, err := cp.ListNodes(NodeFilters{Active: &activeOnly})
	if err != nil {
		t.Fatalf("ListNodes(active=true): %v", err)
	}
	activeSeen := make(map[string]NodeSummary)
	for _, n := range activeNodes {
		activeSeen[n.NodeHash] = n
	}
	if len(activeSeen) != 2 || !activeSeen[overlapHash.Hex()].HasOutbound || !activeSeen[memoryOnlyHash.Hex()].HasOutbound {
		t.Fatalf("active nodes = %+v, want only runtime overlap+memory-only", activeNodes)
	}
	if _, ok := activeSeen[coldHash.Hex()]; ok {
		t.Fatalf("active nodes should not include DB-only cold hash %s: %+v", coldHash.Hex(), activeNodes)
	}

	allInventory := false
	allNodes, err := cp.ListNodes(NodeFilters{Active: &allInventory})
	if err != nil {
		t.Fatalf("ListNodes(active=false): %v", err)
	}
	seen := make(map[string]NodeSummary)
	for _, n := range allNodes {
		seen[n.NodeHash] = n
	}
	if len(seen) != 3 {
		t.Fatalf("inventory+runtime nodes len = %d, want 3: %+v", len(seen), allNodes)
	}
	overlap, ok := seen[overlapHash.Hex()]
	if !ok {
		t.Fatalf("merged nodes missing overlap hash %s: %+v", overlapHash.Hex(), allNodes)
	}
	if !overlap.HasOutbound || len(overlap.Tags) != 1 || overlap.Tags[0].Tag != "sub-a/runtime-overlap" {
		t.Fatalf("overlap summary = %+v, want runtime summary to win", overlap)
	}
	memoryOnly, ok := seen[memoryOnlyHash.Hex()]
	if !ok {
		t.Fatalf("merged nodes missing memory-only hash %s: %+v", memoryOnlyHash.Hex(), allNodes)
	}
	if !memoryOnly.HasOutbound || len(memoryOnly.Tags) != 1 || memoryOnly.Tags[0].Tag != "sub-a/runtime-only" {
		t.Fatalf("memory-only summary = %+v, want runtime summary", memoryOnly)
	}
	cold, ok := seen[coldHash.Hex()]
	if !ok {
		t.Fatalf("merged nodes missing DB-only cold hash %s: %+v", coldHash.Hex(), allNodes)
	}
	if cold.HasOutbound || len(cold.Tags) != 1 || cold.Tags[0].Tag != "sub-a/cold" {
		t.Fatalf("cold summary = %+v, want DB inventory summary", cold)
	}
}

func TestListNodes_ActiveFalseKeepsInventoryMatchWhenRuntimeDoesNotMatchFilter(t *testing.T) {
	dir := t.TempDir()
	engine, closer, err := state.PersistenceBootstrap(filepath.Join(dir, "state"), filepath.Join(dir, "cache"))
	if err != nil {
		t.Fatalf("PersistenceBootstrap: %v", err)
	}
	t.Cleanup(func() { _ = closer.Close() })

	subMgr := topology.NewSubscriptionManager()
	pool := newNodeListTestPool(subMgr)
	sub := subscription.NewSubscription("sub-a", "sub-a", "https://example.com/a", true, false)
	subMgr.Register(sub)

	raw := []byte(`{"type":"ss","server":"4.4.4.4","port":443}`)
	hash := addRoutableNodeForSubscriptionWithTag(t, pool, sub, raw, "203.0.113.40", "runtime-only")
	if err := engine.ReplaceSubscriptionRefresh(sub.ID,
		[]model.NodeStatic{{Hash: hash.Hex(), RawOptions: raw, CreatedAtNs: time.Now().Add(-time.Minute).UnixNano()}},
		[]model.SubscriptionNode{{SubscriptionID: sub.ID, NodeHash: hash.Hex(), Tags: []string{"inventory-only"}}},
		nil,
	); err != nil {
		t.Fatalf("ReplaceSubscriptionRefresh: %v", err)
	}

	cp := &ControlPlaneService{Engine: engine, Pool: pool, SubMgr: subMgr, GeoIP: &geoip.Service{}}
	allInventory := false
	keyword := "inventory-only"
	nodes, err := cp.ListNodes(NodeFilters{Active: &allInventory, TagKeyword: &keyword})
	if err != nil {
		t.Fatalf("ListNodes(active=false tag_keyword): %v", err)
	}
	if len(nodes) != 1 || nodes[0].NodeHash != hash.Hex() {
		t.Fatalf("nodes = %+v, want DB inventory match %s", nodes, hash.Hex())
	}
	if nodes[0].HasOutbound || len(nodes[0].Tags) != 1 || nodes[0].Tags[0].Tag != "sub-a/inventory-only" {
		t.Fatalf("node = %+v, want inventory summary because runtime did not match filter", nodes[0])
	}
}

type fixedGeoReader struct {
	regions map[string]string
}

func (r fixedGeoReader) Lookup(ip netip.Addr) string {
	if r.regions == nil {
		return ""
	}
	return r.regions[ip.String()]
}

func (fixedGeoReader) Close() error { return nil }

func newStartedFixedGeoIP(t *testing.T, regions map[string]string) *geoip.Service {
	t.Helper()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "country.mmdb"), []byte("test-db"), 0o644); err != nil {
		t.Fatalf("write fake geoip db: %v", err)
	}
	svc := geoip.NewService(geoip.ServiceConfig{
		CacheDir: dir,
		OpenDB: func(string) (geoip.GeoReader, error) {
			return fixedGeoReader{regions: regions}, nil
		},
	})
	if err := svc.Start(); err != nil {
		t.Fatalf("start fake geoip service: %v", err)
	}
	t.Cleanup(svc.Stop)
	return svc
}

func TestListNodesPage_ActiveFalseSubscriptionIDUsesDBMetadataWhenRuntimeMissing(t *testing.T) {
	dir := t.TempDir()
	engine, closer, err := state.PersistenceBootstrap(filepath.Join(dir, "state"), filepath.Join(dir, "cache"))
	if err != nil {
		t.Fatalf("PersistenceBootstrap: %v", err)
	}
	t.Cleanup(func() { _ = closer.Close() })

	now := time.Now()
	subID := "db-only-sub"
	if err := engine.UpsertSubscription(model.Subscription{
		ID:                        subID,
		Name:                      "DB Only",
		URL:                       "https://example.com/db-only",
		UpdateIntervalNs:          int64(6 * time.Hour),
		Enabled:                   true,
		EphemeralNodeEvictDelayNs: int64(72 * time.Hour),
		CreatedAtNs:               now.Add(-time.Hour).UnixNano(),
		UpdatedAtNs:               now.UnixNano(),
	}); err != nil {
		t.Fatalf("UpsertSubscription: %v", err)
	}

	raw := []byte(`{"type":"ss","server":"5.5.5.5","port":443}`)
	hash := node.HashFromRawOptions(raw)
	if err := engine.ReplaceSubscriptionRefresh(subID,
		[]model.NodeStatic{{Hash: hash.Hex(), RawOptions: raw, CreatedAtNs: now.UnixNano()}},
		[]model.SubscriptionNode{{SubscriptionID: subID, NodeHash: hash.Hex(), Tags: []string{"cold"}}},
		nil,
	); err != nil {
		t.Fatalf("ReplaceSubscriptionRefresh: %v", err)
	}

	cp := &ControlPlaneService{
		Engine: engine,
		Pool:   newNodeListTestPool(topology.NewSubscriptionManager()),
		SubMgr: topology.NewSubscriptionManager(),
		GeoIP:  &geoip.Service{},
	}
	allInventory := false
	page, err := cp.ListNodesPage(NodeFilters{Active: &allInventory, SubscriptionID: &subID}, NodeListPageOptions{
		SortBy: "created_at",
		Limit:  10,
	})
	if err != nil {
		t.Fatalf("ListNodesPage(active=false subscription_id DB-only): %v", err)
	}
	if page.Total != 1 || len(page.Items) != 1 || page.Items[0].NodeHash != hash.Hex() {
		t.Fatalf("page = %+v, want the DB-only subscription node %s", page, hash.Hex())
	}
	if page.Items[0].DisplayTag != "DB Only/cold" {
		t.Fatalf("display_tag = %q, want %q", page.Items[0].DisplayTag, "DB Only/cold")
	}
}

func TestListNodesPage_ActiveFalseInventoryRegionMatchesPersistedRegionOnly(t *testing.T) {
	dir := t.TempDir()
	engine, closer, err := state.PersistenceBootstrap(filepath.Join(dir, "state"), filepath.Join(dir, "cache"))
	if err != nil {
		t.Fatalf("PersistenceBootstrap: %v", err)
	}
	t.Cleanup(func() { _ = closer.Close() })

	now := time.Now()
	subID := "sub-a"
	if err := engine.UpsertSubscription(model.Subscription{
		ID:                        subID,
		Name:                      "Sub A",
		URL:                       "https://example.com/a",
		UpdateIntervalNs:          int64(6 * time.Hour),
		Enabled:                   true,
		EphemeralNodeEvictDelayNs: int64(72 * time.Hour),
		CreatedAtNs:               now.Add(-time.Hour).UnixNano(),
		UpdatedAtNs:               now.UnixNano(),
	}); err != nil {
		t.Fatalf("UpsertSubscription: %v", err)
	}

	rawFallback := []byte(`{"type":"ss","server":"6.6.6.6","port":443}`)
	hashFallback := node.HashFromRawOptions(rawFallback)
	rawStored := []byte(`{"type":"ss","server":"7.7.7.7","port":443}`)
	hashStored := node.HashFromRawOptions(rawStored)
	if err := engine.ReplaceSubscriptionRefresh(subID,
		[]model.NodeStatic{
			{Hash: hashFallback.Hex(), RawOptions: rawFallback, CreatedAtNs: now.Add(-2 * time.Minute).UnixNano()},
			{Hash: hashStored.Hex(), RawOptions: rawStored, CreatedAtNs: now.Add(-time.Minute).UnixNano()},
		},
		[]model.SubscriptionNode{
			{SubscriptionID: subID, NodeHash: hashFallback.Hex(), Tags: []string{"fallback"}},
			{SubscriptionID: subID, NodeHash: hashStored.Hex(), Tags: []string{"stored"}},
		},
		nil,
	); err != nil {
		t.Fatalf("ReplaceSubscriptionRefresh: %v", err)
	}
	if err := engine.BulkUpsertNodesDynamic([]model.NodeDynamic{
		{Hash: hashFallback.Hex(), EgressIP: "198.51.100.10", EgressRegion: ""},
		{Hash: hashStored.Hex(), EgressIP: "198.51.100.11", EgressRegion: "jp"},
	}); err != nil {
		t.Fatalf("BulkUpsertNodesDynamic: %v", err)
	}

	cp := &ControlPlaneService{
		Engine: engine,
		Pool:   newNodeListTestPool(topology.NewSubscriptionManager()),
		SubMgr: topology.NewSubscriptionManager(),
		GeoIP:  newStartedFixedGeoIP(t, map[string]string{"198.51.100.10": "jp"}),
	}
	allInventory := false
	page, err := cp.ListNodesPage(NodeFilters{Active: &allInventory}, NodeListPageOptions{
		SortBy: "created_at",
		Limit:  10,
	})
	if err != nil {
		t.Fatalf("ListNodesPage(active=false): %v", err)
	}
	byHash := make(map[string]NodeSummary, len(page.Items))
	for _, item := range page.Items {
		byHash[item.NodeHash] = item
	}
	if got := byHash[hashFallback.Hex()].Region; got != "" {
		t.Fatalf("DB inventory summary region with empty persisted region = %q, want empty even when GeoIP can resolve it", got)
	}
	if got := byHash[hashStored.Hex()].Region; got != "jp" {
		t.Fatalf("DB inventory summary region with stored region = %q, want jp", got)
	}

	sortedByRegion, err := cp.ListNodesPage(NodeFilters{Active: &allInventory}, NodeListPageOptions{
		SortBy: "region",
		Limit:  10,
	})
	if err != nil {
		t.Fatalf("ListNodesPage(active=false sort_by=region): %v", err)
	}
	if len(sortedByRegion.Items) != 2 || sortedByRegion.Items[0].NodeHash != hashFallback.Hex() || sortedByRegion.Items[1].NodeHash != hashStored.Hex() {
		t.Fatalf("region-sorted page = %+v, want empty persisted region before jp", sortedByRegion.Items)
	}

	region := "jp"
	filtered, err := cp.ListNodesPage(NodeFilters{Active: &allInventory, Region: &region}, NodeListPageOptions{
		SortBy: "created_at",
		Limit:  10,
	})
	if err != nil {
		t.Fatalf("ListNodesPage(active=false region=jp): %v", err)
	}
	if filtered.Total != 1 || len(filtered.Items) != 1 || filtered.Items[0].NodeHash != hashStored.Hex() {
		t.Fatalf("region-filtered page = %+v, want only persisted-region node %s", filtered, hashStored.Hex())
	}
}

func TestListNodesPage_ActiveFalseHasOutboundFalseDoesNotReturnRuntimeOutboundAsInventoryCold(t *testing.T) {
	dir := t.TempDir()
	engine, closer, err := state.PersistenceBootstrap(filepath.Join(dir, "state"), filepath.Join(dir, "cache"))
	if err != nil {
		t.Fatalf("PersistenceBootstrap: %v", err)
	}
	t.Cleanup(func() { _ = closer.Close() })

	subMgr := topology.NewSubscriptionManager()
	pool := newNodeListTestPool(subMgr)
	sub := subscription.NewSubscription("sub-a", "sub-a", "https://example.com/a", true, false)
	sub.CreatedAtNs = time.Now().Add(-time.Hour).UnixNano()
	subMgr.Register(sub)

	raw := []byte(`{"type":"ss","server":"8.8.8.8","port":443}`)
	hash := addRoutableNodeForSubscriptionWithTag(t, pool, sub, raw, "203.0.113.88", "runtime")
	if err := engine.UpsertSubscription(model.Subscription{
		ID:                        sub.ID,
		Name:                      sub.Name(),
		URL:                       sub.URL(),
		Enabled:                   true,
		UpdateIntervalNs:          int64(6 * time.Hour),
		EphemeralNodeEvictDelayNs: int64(72 * time.Hour),
		CreatedAtNs:               sub.CreatedAtNs,
		UpdatedAtNs:               time.Now().UnixNano(),
	}); err != nil {
		t.Fatalf("UpsertSubscription: %v", err)
	}
	if err := engine.ReplaceSubscriptionRefresh(sub.ID,
		[]model.NodeStatic{{Hash: hash.Hex(), RawOptions: raw, CreatedAtNs: time.Now().UnixNano()}},
		[]model.SubscriptionNode{{SubscriptionID: sub.ID, NodeHash: hash.Hex(), Tags: []string{"inventory"}}},
		nil,
	); err != nil {
		t.Fatalf("ReplaceSubscriptionRefresh: %v", err)
	}

	cp := &ControlPlaneService{Engine: engine, Pool: pool, SubMgr: subMgr, GeoIP: &geoip.Service{}}
	allInventory := false
	hasOutbound := false
	page, err := cp.ListNodesPage(NodeFilters{Active: &allInventory, HasOutbound: &hasOutbound}, NodeListPageOptions{
		SortBy: "created_at",
		Limit:  10,
	})
	if err != nil {
		t.Fatalf("ListNodesPage(active=false has_outbound=false): %v", err)
	}
	if page.Total != 0 || len(page.Items) != 0 {
		t.Fatalf("page = %+v, want runtime node with outbound excluded instead of DB cold summary", page)
	}
}

func TestGetNode_ReferenceLatencyMsUsesAuthorityAverage(t *testing.T) {
	subMgr := topology.NewSubscriptionManager()
	pool := newNodeListTestPool(subMgr)

	sub := subscription.NewSubscription("sub-a", "sub-a", "https://example.com/a", true, false)
	subMgr.Register(sub)

	hash := addRoutableNodeForSubscription(
		t,
		pool,
		sub,
		[]byte(`{"type":"ss","server":"1.1.1.1","port":443}`),
		"203.0.113.30",
	)

	entry, ok := pool.GetEntry(hash)
	if !ok {
		t.Fatalf("node %s missing", hash.Hex())
	}
	entry.LatencyTable.LoadEntry("cloudflare.com", node.DomainLatencyStats{
		Ewma:        40 * time.Millisecond,
		LastUpdated: time.Now(),
	})
	entry.LatencyTable.LoadEntry("github.com", node.DomainLatencyStats{
		Ewma:        60 * time.Millisecond,
		LastUpdated: time.Now(),
	})
	entry.LatencyTable.LoadEntry("example.com", node.DomainLatencyStats{
		Ewma:        5 * time.Millisecond,
		LastUpdated: time.Now(),
	})

	runtimeCfg := &atomic.Pointer[config.RuntimeConfig]{}
	cfg := config.NewDefaultRuntimeConfig()
	cfg.LatencyAuthorities = []string{"cloudflare.com", "github.com", "google.com"}
	runtimeCfg.Store(cfg)

	cp := &ControlPlaneService{
		Pool:       pool,
		SubMgr:     subMgr,
		GeoIP:      &geoip.Service{},
		RuntimeCfg: runtimeCfg,
	}

	got, err := cp.GetNode(hash.Hex())
	if err != nil {
		t.Fatalf("GetNode: %v", err)
	}
	if got.ReferenceLatencyMs == nil {
		t.Fatal("reference_latency_ms should be present")
	}
	if *got.ReferenceLatencyMs != 50 {
		t.Fatalf("reference_latency_ms = %v, want 50", *got.ReferenceLatencyMs)
	}
}

func TestListNodes_ProbedSinceUsesLastLatencyProbeAttempt(t *testing.T) {
	subMgr := topology.NewSubscriptionManager()
	pool := newNodeListTestPool(subMgr)

	sub := subscription.NewSubscription("sub-a", "sub-a", "https://example.com/a", true, false)
	subMgr.Register(sub)

	hash := addRoutableNodeForSubscription(
		t,
		pool,
		sub,
		[]byte(`{"type":"ss","server":"1.1.1.1","port":443}`),
		"203.0.113.30",
	)

	entry, ok := pool.GetEntry(hash)
	if !ok {
		t.Fatalf("node %s missing", hash.Hex())
	}

	latencyAttempt := time.Now().Add(-2 * time.Minute).UnixNano()
	entry.LastLatencyProbeAttempt.Store(latencyAttempt)
	// Keep egress update older to ensure filter is using LastLatencyProbeAttempt.
	entry.LastEgressUpdate.Store(time.Now().Add(-10 * time.Minute).UnixNano())

	cp := &ControlPlaneService{
		Pool:   pool,
		SubMgr: subMgr,
		GeoIP:  &geoip.Service{},
	}

	before := time.Unix(0, latencyAttempt).Add(-1 * time.Minute)
	nodes, err := cp.ListNodes(NodeFilters{ProbedSince: &before})
	if err != nil {
		t.Fatalf("ListNodes(before): %v", err)
	}
	if len(nodes) != 1 {
		t.Fatalf("ListNodes(before) len = %d, want 1", len(nodes))
	}

	after := time.Unix(0, latencyAttempt).Add(1 * time.Minute)
	nodes, err = cp.ListNodes(NodeFilters{ProbedSince: &after})
	if err != nil {
		t.Fatalf("ListNodes(after): %v", err)
	}
	if len(nodes) != 0 {
		t.Fatalf("ListNodes(after) len = %d, want 0", len(nodes))
	}
}

func TestListNodes_TagKeywordFuzzyMatchIsCaseInsensitive(t *testing.T) {
	subMgr := topology.NewSubscriptionManager()
	pool := newNodeListTestPool(subMgr)

	sub := subscription.NewSubscription("sub-a", "sub-a", "https://example.com/a", true, false)
	subMgr.Register(sub)

	matchHash := addRoutableNodeForSubscriptionWithTag(
		t,
		pool,
		sub,
		[]byte(`{"type":"ss","server":"1.1.1.1","port":443}`),
		"203.0.113.30",
		"hongkong-fast-01",
	)
	_ = addRoutableNodeForSubscriptionWithTag(
		t,
		pool,
		sub,
		[]byte(`{"type":"ss","server":"2.2.2.2","port":443}`),
		"203.0.113.31",
		"japan-slow-01",
	)

	cp := &ControlPlaneService{
		Pool:   pool,
		SubMgr: subMgr,
		GeoIP:  &geoip.Service{},
	}

	keyword := "FAST"
	nodes, err := cp.ListNodes(NodeFilters{TagKeyword: &keyword})
	if err != nil {
		t.Fatalf("ListNodes(tag_keyword): %v", err)
	}
	if len(nodes) != 1 {
		t.Fatalf("ListNodes(tag_keyword) len = %d, want 1", len(nodes))
	}
	if nodes[0].NodeHash != matchHash.Hex() {
		t.Fatalf("ListNodes(tag_keyword) hash = %q, want %q", nodes[0].NodeHash, matchHash.Hex())
	}
}

func TestListNodes_RegionFilterAndSummaryPreferStoredRegion(t *testing.T) {
	subMgr := topology.NewSubscriptionManager()
	pool := newNodeListTestPool(subMgr)

	sub := subscription.NewSubscription("sub-a", "sub-a", "https://example.com/a", true, false)
	subMgr.Register(sub)

	hash := addRoutableNodeForSubscription(
		t,
		pool,
		sub,
		[]byte(`{"type":"ss","server":"1.1.1.1","port":443}`),
		"203.0.113.40",
	)

	entry, ok := pool.GetEntry(hash)
	if !ok {
		t.Fatalf("node %s missing", hash.Hex())
	}
	entry.SetEgressRegion("jp")

	cp := &ControlPlaneService{
		Pool:   pool,
		SubMgr: subMgr,
		GeoIP:  &geoip.Service{}, // empty service returns "", forcing stored-region path
	}

	region := "jp"
	nodes, err := cp.ListNodes(NodeFilters{Region: &region})
	if err != nil {
		t.Fatalf("ListNodes(region): %v", err)
	}
	if len(nodes) != 1 || nodes[0].NodeHash != hash.Hex() {
		t.Fatalf("region-filtered nodes = %+v, want [%s]", nodes, hash.Hex())
	}

	got, err := cp.GetNode(hash.Hex())
	if err != nil {
		t.Fatalf("GetNode: %v", err)
	}
	if got.Region != "jp" {
		t.Fatalf("summary region: got %q, want %q", got.Region, "jp")
	}
}

func TestListNodes_EnabledFilter(t *testing.T) {
	subMgr := topology.NewSubscriptionManager()
	pool := newNodeListTestPool(subMgr)

	subEnabled := subscription.NewSubscription("sub-enabled", "sub-enabled", "https://example.com/enabled", true, false)
	subDisabled := subscription.NewSubscription("sub-disabled", "sub-disabled", "https://example.com/disabled", false, false)
	subMgr.Register(subEnabled)
	subMgr.Register(subDisabled)

	enabledHash := addRoutableNodeForSubscription(
		t,
		pool,
		subEnabled,
		[]byte(`{"type":"ss","server":"1.1.1.1","port":443}`),
		"203.0.113.70",
	)
	disabledHash := addRoutableNodeForSubscription(
		t,
		pool,
		subDisabled,
		[]byte(`{"type":"ss","server":"2.2.2.2","port":443}`),
		"203.0.113.71",
	)

	cp := &ControlPlaneService{
		Pool:   pool,
		SubMgr: subMgr,
		GeoIP:  &geoip.Service{},
	}

	enabled := true
	nodes, err := cp.ListNodes(NodeFilters{Enabled: &enabled})
	if err != nil {
		t.Fatalf("ListNodes(enabled=true): %v", err)
	}
	if len(nodes) != 1 || nodes[0].NodeHash != enabledHash.Hex() {
		t.Fatalf("enabled filter result = %+v, want [%s]", nodes, enabledHash.Hex())
	}

	disabled := false
	nodes, err = cp.ListNodes(NodeFilters{Enabled: &disabled})
	if err != nil {
		t.Fatalf("ListNodes(enabled=false): %v", err)
	}
	if len(nodes) != 1 || nodes[0].NodeHash != disabledHash.Hex() {
		t.Fatalf("disabled filter result = %+v, want [%s]", nodes, disabledHash.Hex())
	}
}

func TestProbeEgress_ReturnsRegion(t *testing.T) {
	subMgr := topology.NewSubscriptionManager()
	pool := newNodeListTestPool(subMgr)

	sub := subscription.NewSubscription("sub-a", "sub-a", "https://example.com/a", true, false)
	subMgr.Register(sub)

	hash := addRoutableNodeForSubscription(
		t,
		pool,
		sub,
		[]byte(`{"type":"ss","server":"1.1.1.1","port":443}`),
		"203.0.113.60",
	)

	cp := &ControlPlaneService{
		Pool:   pool,
		SubMgr: subMgr,
		GeoIP:  &geoip.Service{}, // empty service keeps focus on stored region from loc
		ProbeMgr: probe.NewProbeManager(probe.ProbeConfig{
			Pool: pool,
			Fetcher: func(_ node.Hash, _ string) ([]byte, time.Duration, error) {
				return []byte("ip=198.51.100.88\nloc=JP"), 20 * time.Millisecond, nil
			},
		}),
	}

	got, err := cp.ProbeEgress(hash.Hex())
	if err != nil {
		t.Fatalf("ProbeEgress: %v", err)
	}
	if got.EgressIP != "198.51.100.88" {
		t.Fatalf("egress_ip: got %q, want %q", got.EgressIP, "198.51.100.88")
	}
	if got.Region != "jp" {
		t.Fatalf("region: got %q, want %q", got.Region, "jp")
	}
}
