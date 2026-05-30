package state

import (
	"encoding/json"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/Resinat/Resin/internal/model"
	"github.com/Resinat/Resin/internal/node"
	"github.com/Resinat/Resin/internal/topology"
)

func newTestCacheRepo(t *testing.T) *CacheRepo {
	t.Helper()
	dir := t.TempDir()
	db, err := OpenDB(dir + "/cache.db")
	if err != nil {
		t.Fatal(err)
	}
	if err := MigrateCacheDB(db); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return newCacheRepo(db)
}

// --- nodes_static ---

func TestCacheRepo_NodesStatic_BulkUpsertAndLoad(t *testing.T) {
	repo := newTestCacheRepo(t)

	nodes := []model.NodeStatic{
		{Hash: "aaa", RawOptions: json.RawMessage(`{"type":"ss"}`), CreatedAtNs: 100},
		{Hash: "bbb", RawOptions: json.RawMessage(`{"type":"vmess"}`), CreatedAtNs: 200},
	}
	if err := repo.BulkUpsertNodesStatic(nodes); err != nil {
		t.Fatal(err)
	}

	loaded, err := repo.LoadAllNodesStatic()
	if err != nil {
		t.Fatal(err)
	}
	if len(loaded) != 2 {
		t.Fatalf("expected 2 nodes, got %d", len(loaded))
	}

	// Idempotent upsert: update existing.
	nodes[0].RawOptions = json.RawMessage(`{"type":"ss","updated":true}`)
	if err := repo.BulkUpsertNodesStatic(nodes[:1]); err != nil {
		t.Fatal(err)
	}
	loaded, _ = repo.LoadAllNodesStatic()
	for _, n := range loaded {
		if n.Hash == "aaa" && string(n.RawOptions) != `{"type":"ss","updated":true}` {
			t.Fatalf("expected updated options, got %s", string(n.RawOptions))
		}
	}
}

func TestCacheRepo_NodesStatic_BulkDelete(t *testing.T) {
	repo := newTestCacheRepo(t)

	nodes := []model.NodeStatic{
		{Hash: "aaa", RawOptions: json.RawMessage(`{}`), CreatedAtNs: 100},
		{Hash: "bbb", RawOptions: json.RawMessage(`{}`), CreatedAtNs: 200},
	}
	repo.BulkUpsertNodesStatic(nodes)

	if err := repo.BulkDeleteNodesStatic([]string{"aaa"}); err != nil {
		t.Fatal(err)
	}
	loaded, _ := repo.LoadAllNodesStatic()
	if len(loaded) != 1 || loaded[0].Hash != "bbb" {
		t.Fatalf("expected only bbb, got %+v", loaded)
	}
}

// --- nodes_dynamic ---

func TestCacheRepo_NodesDynamic_BulkUpsertAndLoad(t *testing.T) {
	repo := newTestCacheRepo(t)

	nodes := []model.NodeDynamic{
		{
			Hash:                               "aaa",
			FailureCount:                       3,
			CircuitOpenSince:                   1000,
			EgressIP:                           "1.2.3.4",
			EgressIPs:                          []string{"1.2.3.4", "5.6.7.8"},
			EgressRegion:                       "us",
			EgressUpdatedAtNs:                  500,
			LastLatencyProbeAttemptNs:          700,
			NextLatencyProbeDueNs:              1700,
			LastAuthorityLatencyProbeAttemptNs: 800,
			LastEgressUpdateAttemptNs:          900,
		},
	}
	if err := repo.BulkUpsertNodesDynamic(nodes); err != nil {
		t.Fatal(err)
	}

	loaded, err := repo.LoadAllNodesDynamic()
	if err != nil {
		t.Fatal(err)
	}
	if len(loaded) != 1 || loaded[0].FailureCount != 3 {
		t.Fatalf("unexpected: %+v", loaded)
	}
	if loaded[0].EgressRegion != "us" {
		t.Fatalf("egress_region: got %q, want %q", loaded[0].EgressRegion, "us")
	}
	if !reflect.DeepEqual(loaded[0].EgressIPs, []string{"1.2.3.4", "5.6.7.8"}) {
		t.Fatalf("egress_ips: got %v", loaded[0].EgressIPs)
	}
	if loaded[0].LastLatencyProbeAttemptNs != 700 ||
		loaded[0].NextLatencyProbeDueNs != 1700 ||
		loaded[0].LastAuthorityLatencyProbeAttemptNs != 800 ||
		loaded[0].LastEgressUpdateAttemptNs != 900 {
		t.Fatalf("unexpected probe attempt fields: %+v", loaded[0])
	}

	// Update.
	nodes[0].FailureCount = 0
	repo.BulkUpsertNodesDynamic(nodes)
	loaded, _ = repo.LoadAllNodesDynamic()
	if loaded[0].FailureCount != 0 {
		t.Fatalf("expected 0 failures after reset, got %d", loaded[0].FailureCount)
	}
}

func TestCacheRepo_NodesDynamic_BulkDelete(t *testing.T) {
	repo := newTestCacheRepo(t)

	repo.BulkUpsertNodesDynamic([]model.NodeDynamic{{Hash: "aaa"}, {Hash: "bbb"}})
	repo.BulkDeleteNodesDynamic([]string{"bbb"})

	loaded, _ := repo.LoadAllNodesDynamic()
	if len(loaded) != 1 || loaded[0].Hash != "aaa" {
		t.Fatalf("expected only aaa, got %+v", loaded)
	}
}

// --- node_latency ---

func TestCacheRepo_NodeLatency_BulkUpsertAndLoad(t *testing.T) {
	repo := newTestCacheRepo(t)

	entries := []model.NodeLatency{
		{NodeHash: "aaa", Domain: "google.com", EwmaNs: 5000, LastUpdatedNs: 100},
		{NodeHash: "aaa", Domain: "github.com", EwmaNs: 8000, LastUpdatedNs: 200},
	}
	if err := repo.BulkUpsertNodeLatency(entries); err != nil {
		t.Fatal(err)
	}

	loaded, err := repo.LoadAllNodeLatency()
	if err != nil {
		t.Fatal(err)
	}
	if len(loaded) != 2 {
		t.Fatalf("expected 2, got %d", len(loaded))
	}
}

func TestCacheRepo_NodeLatency_BulkDelete(t *testing.T) {
	repo := newTestCacheRepo(t)

	repo.BulkUpsertNodeLatency([]model.NodeLatency{
		{NodeHash: "aaa", Domain: "google.com", EwmaNs: 5000, LastUpdatedNs: 100},
		{NodeHash: "aaa", Domain: "github.com", EwmaNs: 8000, LastUpdatedNs: 200},
	})

	repo.BulkDeleteNodeLatency([]model.NodeLatencyKey{{NodeHash: "aaa", Domain: "google.com"}})
	loaded, _ := repo.LoadAllNodeLatency()
	if len(loaded) != 1 || loaded[0].Domain != "github.com" {
		t.Fatalf("expected only github.com, got %+v", loaded)
	}
}

// --- leases ---

func TestCacheRepo_Leases_BulkUpsertAndLoad(t *testing.T) {
	repo := newTestCacheRepo(t)

	leases := []model.Lease{
		{PlatformID: "p1", Account: "user1", NodeHash: "n1", EgressIP: "1.2.3.4", CreatedAtNs: 50, ExpiryNs: 9999, LastAccessedNs: 100},
	}
	if err := repo.BulkUpsertLeases(leases); err != nil {
		t.Fatal(err)
	}

	loaded, err := repo.LoadAllLeases()
	if err != nil {
		t.Fatal(err)
	}
	if len(loaded) != 1 || loaded[0].Account != "user1" {
		t.Fatalf("unexpected: %+v", loaded)
	}
	if loaded[0].CreatedAtNs != 50 {
		t.Fatalf("created_at_ns: got %d, want %d", loaded[0].CreatedAtNs, 50)
	}
}

func TestCacheRepo_Leases_BulkDelete(t *testing.T) {
	repo := newTestCacheRepo(t)

	repo.BulkUpsertLeases([]model.Lease{
		{PlatformID: "p1", Account: "user1", NodeHash: "n1", CreatedAtNs: 10, ExpiryNs: 9999, LastAccessedNs: 100},
		{PlatformID: "p1", Account: "user2", NodeHash: "n2", CreatedAtNs: 20, ExpiryNs: 9999, LastAccessedNs: 100},
	})
	repo.BulkDeleteLeases([]model.LeaseKey{{PlatformID: "p1", Account: "user1"}})

	loaded, _ := repo.LoadAllLeases()
	if len(loaded) != 1 || loaded[0].Account != "user2" {
		t.Fatalf("expected only user2, got %+v", loaded)
	}
}

// --- subscription_nodes ---

func TestCacheRepo_SubscriptionNodes_BulkUpsertAndLoad(t *testing.T) {
	repo := newTestCacheRepo(t)

	sns := []model.SubscriptionNode{
		{SubscriptionID: "s1", NodeHash: "n1", Tags: []string{"tag1", "tag2"}, Evicted: true},
		{SubscriptionID: "s1", NodeHash: "n2", Tags: []string{"tag3"}, Evicted: false},
	}
	if err := repo.BulkUpsertSubscriptionNodes(sns); err != nil {
		t.Fatal(err)
	}

	loaded, err := repo.LoadAllSubscriptionNodes()
	if err != nil {
		t.Fatal(err)
	}
	if len(loaded) != 2 {
		t.Fatalf("expected 2, got %d", len(loaded))
	}

	// Idempotent upsert: update tags.
	sns[0].Tags = []string{"tag1-updated"}
	sns[0].Evicted = false
	repo.BulkUpsertSubscriptionNodes(sns[:1])
	loaded, _ = repo.LoadAllSubscriptionNodes()
	for _, sn := range loaded {
		if sn.NodeHash == "n1" {
			if !reflect.DeepEqual(sn.Tags, []string{"tag1-updated"}) {
				t.Fatalf("expected updated tags, got %+v", sn.Tags)
			}
			if sn.Evicted {
				t.Fatal("expected evicted=false after idempotent upsert update")
			}
		}
	}
}

func TestCacheRepo_SubscriptionNodes_BulkDelete(t *testing.T) {
	repo := newTestCacheRepo(t)

	repo.BulkUpsertSubscriptionNodes([]model.SubscriptionNode{
		{SubscriptionID: "s1", NodeHash: "n1", Tags: []string{}},
		{SubscriptionID: "s1", NodeHash: "n2", Tags: []string{}},
	})
	repo.BulkDeleteSubscriptionNodes([]model.SubscriptionNodeKey{{SubscriptionID: "s1", NodeHash: "n1"}})

	loaded, _ := repo.LoadAllSubscriptionNodes()
	if len(loaded) != 1 || loaded[0].NodeHash != "n2" {
		t.Fatalf("expected only n2, got %+v", loaded)
	}
}

func TestMigrateCacheDB_BackfillsLegacyNextLatencyProbeDue(t *testing.T) {
	dir := t.TempDir()
	db, err := OpenDB(dir + "/cache.db")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	_, err = db.Exec(`
		CREATE TABLE nodes_static (
			hash             TEXT PRIMARY KEY,
			raw_options_json TEXT NOT NULL,
			created_at_ns    INTEGER NOT NULL
		);
		CREATE TABLE nodes_dynamic (
			hash                                TEXT PRIMARY KEY,
			failure_count                       INTEGER NOT NULL DEFAULT 0,
			circuit_open_since                  INTEGER NOT NULL DEFAULT 0,
			egress_ip                           TEXT NOT NULL DEFAULT '',
			egress_region                       TEXT NOT NULL DEFAULT '',
			egress_updated_at_ns                INTEGER NOT NULL DEFAULT 0,
			last_latency_probe_attempt_ns       INTEGER NOT NULL DEFAULT 0,
			last_authority_latency_probe_attempt_ns INTEGER NOT NULL DEFAULT 0,
			last_egress_update_attempt_ns       INTEGER NOT NULL DEFAULT 0,
			egress_ips_json                     TEXT NOT NULL DEFAULT '[]'
		);
		CREATE TABLE node_latency (
			node_hash       TEXT NOT NULL,
			domain          TEXT NOT NULL,
			ewma_ns         INTEGER NOT NULL,
			last_updated_ns INTEGER NOT NULL,
			PRIMARY KEY (node_hash, domain)
		);
		CREATE TABLE leases (
			platform_id      TEXT NOT NULL,
			account          TEXT NOT NULL,
			node_hash        TEXT NOT NULL,
			egress_ip        TEXT NOT NULL DEFAULT '',
			created_at_ns    INTEGER NOT NULL DEFAULT 0,
			expiry_ns        INTEGER NOT NULL,
			last_accessed_ns INTEGER NOT NULL,
			PRIMARY KEY (platform_id, account)
		);
		CREATE TABLE subscription_nodes (
			subscription_id TEXT NOT NULL,
			node_hash       TEXT NOT NULL,
			tags_json       TEXT NOT NULL DEFAULT '[]',
			evicted         INTEGER NOT NULL DEFAULT 0,
			PRIMARY KEY (subscription_id, node_hash)
		);
		CREATE TABLE schema_migrations (version INTEGER NOT NULL, dirty BOOLEAN NOT NULL);
		INSERT INTO schema_migrations (version, dirty) VALUES (3, false);
	`)
	if err != nil {
		t.Fatalf("create legacy cache schema: %v", err)
	}

	lastAttemptNs := int64(time.Hour)
	dueRaw := json.RawMessage(`{"type":"stub","server":"198.51.100.60","server_port":443}`)
	neverAttemptedRaw := json.RawMessage(`{"type":"stub","server":"198.51.100.61","server_port":443}`)
	dueHash := node.HashFromRawOptions(dueRaw)
	neverAttemptedHash := node.HashFromRawOptions(neverAttemptedRaw)
	if _, err := db.Exec(`INSERT INTO nodes_static (hash, raw_options_json, created_at_ns) VALUES (?, ?, 1), (?, ?, 1)`, dueHash.Hex(), string(dueRaw), neverAttemptedHash.Hex(), string(neverAttemptedRaw)); err != nil {
		t.Fatalf("seed legacy static rows: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO subscription_nodes (subscription_id, node_hash, tags_json, evicted) VALUES ('sub-a', ?, '[]', 0), ('sub-a', ?, '[]', 0)`, dueHash.Hex(), neverAttemptedHash.Hex()); err != nil {
		t.Fatalf("seed legacy subscription rows: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO nodes_dynamic (hash, failure_count, last_latency_probe_attempt_ns) VALUES (?, 2, ?), (?, 0, 0)`, dueHash.Hex(), lastAttemptNs, neverAttemptedHash.Hex()); err != nil {
		t.Fatalf("seed legacy dynamic rows: %v", err)
	}

	if err := MigrateCacheDB(db); err != nil {
		t.Fatalf("MigrateCacheDB: %v", err)
	}

	var nextDueNs int64
	if err := db.QueryRow(`SELECT next_latency_probe_due_ns FROM nodes_dynamic WHERE hash = ?`, dueHash.Hex()).Scan(&nextDueNs); err != nil {
		t.Fatalf("query migrated next due: %v", err)
	}
	wantNextDueNs := lastAttemptNs + int64(20*time.Minute)
	if nextDueNs != wantNextDueNs {
		t.Fatalf("migrated next due: got %d, want %d", nextDueNs, wantNextDueNs)
	}

	var zeroNextDueNs int64
	if err := db.QueryRow(`SELECT next_latency_probe_due_ns FROM nodes_dynamic WHERE hash = ?`, neverAttemptedHash.Hex()).Scan(&zeroNextDueNs); err != nil {
		t.Fatalf("query never attempted next due: %v", err)
	}
	if zeroNextDueNs != 0 {
		t.Fatalf("never attempted next due: got %d, want 0", zeroNextDueNs)
	}

	repo := newCacheRepo(db)
	early, err := repo.LoadDueColdNodeCandidates(wantNextDueNs-1, time.Hour, 10)
	if err != nil {
		t.Fatalf("LoadDueColdNodeCandidates early: %v", err)
	}
	for _, candidate := range early {
		if candidate.Hash == dueHash {
			t.Fatalf("migrated node became due before persisted next due: got %+v", early)
		}
	}
	due, err := repo.LoadDueColdNodeCandidates(wantNextDueNs, time.Hour, 10)
	if err != nil {
		t.Fatalf("LoadDueColdNodeCandidates at due: %v", err)
	}
	foundDue := false
	for _, candidate := range due {
		if candidate.Hash == dueHash {
			foundDue = true
		}
	}
	if !foundDue {
		t.Fatalf("migrated node was not due at persisted next due: got %+v, want %s", due, dueHash.Hex())
	}
}

func TestCacheRepo_LoadDueColdNodeCandidates_FiltersOrdersAndLimits(t *testing.T) {
	repo := newTestCacheRepo(t)

	nowNs := int64(10_000)
	interval := 100 * time.Nanosecond
	type seedNode struct {
		subID      string
		raw        json.RawMessage
		tags       []string
		evicted    bool
		hasDynamic bool
		nextDueNs  int64
	}
	seeds := []seedNode{
		{subID: "sub-b", raw: json.RawMessage(`{"type":"stub","server":"198.51.100.1","server_port":443}`), tags: []string{"missing-dynamic"}},
		{subID: "sub-a", raw: json.RawMessage(`{"type":"stub","server":"198.51.100.2","server_port":443}`), tags: []string{"zero-next-due"}, hasDynamic: true, nextDueNs: 0},
		{subID: "sub-a", raw: json.RawMessage(`{"type":"stub","server":"198.51.100.3","server_port":443}`), tags: []string{"past-next-due"}, hasDynamic: true, nextDueNs: nowNs - 1},
		{subID: "sub-a", raw: json.RawMessage(`{"type":"stub","server":"198.51.100.4","server_port":443}`), tags: []string{"future-next-due"}, hasDynamic: true, nextDueNs: nowNs + 1},
		{subID: "sub-a", raw: json.RawMessage(`{"type":"stub","server":"198.51.100.5","server_port":443}`), tags: []string{"evicted"}, evicted: true},
	}

	var statics []model.NodeStatic
	var subNodes []model.SubscriptionNode
	var dynamics []model.NodeDynamic
	dueByKey := make(map[string]seedNode)
	var sharedDueHash node.Hash
	for _, seed := range seeds {
		hash := node.HashFromRawOptions(seed.raw)
		if seed.subID == "sub-b" {
			sharedDueHash = hash
		}
		statics = append(statics, model.NodeStatic{Hash: hash.Hex(), RawOptions: seed.raw, CreatedAtNs: nowNs - 1_000})
		subNodes = append(subNodes, model.SubscriptionNode{
			SubscriptionID: seed.subID,
			NodeHash:       hash.Hex(),
			Tags:           seed.tags,
			Evicted:        seed.evicted,
		})
		if seed.hasDynamic {
			dynamics = append(dynamics, model.NodeDynamic{Hash: hash.Hex(), NextLatencyProbeDueNs: seed.nextDueNs})
		}
		if !seed.evicted && (!seed.hasDynamic || seed.nextDueNs <= nowNs) {
			dueByKey[seed.subID+"/"+hash.Hex()] = seed
		}
	}
	subNodes = append(subNodes, model.SubscriptionNode{
		SubscriptionID: "sub-c",
		NodeHash:       sharedDueHash.Hex(),
		Tags:           []string{"shared-due"},
	})
	if err := repo.BulkUpsertNodesStatic(statics); err != nil {
		t.Fatalf("BulkUpsertNodesStatic: %v", err)
	}
	if err := repo.BulkUpsertSubscriptionNodes(subNodes); err != nil {
		t.Fatalf("BulkUpsertSubscriptionNodes: %v", err)
	}
	if err := repo.BulkUpsertNodesDynamic(dynamics); err != nil {
		t.Fatalf("BulkUpsertNodesDynamic: %v", err)
	}
	// Orphan subscription_nodes rows without nodes_static must not become candidates.
	if err := repo.BulkUpsertSubscriptionNodes([]model.SubscriptionNode{{
		SubscriptionID: "sub-a",
		NodeHash:       "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
		Tags:           []string{"orphan"},
	}}); err != nil {
		t.Fatalf("BulkUpsertSubscriptionNodes orphan: %v", err)
	}

	got, err := repo.LoadDueColdNodeCandidates(nowNs, interval, 2)
	if err != nil {
		t.Fatalf("LoadDueColdNodeCandidates limited: %v", err)
	}
	wantKeys := make([]string, 0, len(dueByKey))
	for key := range dueByKey {
		wantKeys = append(wantKeys, key)
	}
	sort.Strings(wantKeys)
	if len(got) != 2 {
		t.Fatalf("limited candidates: got %d, want 2", len(got))
	}
	for i, candidate := range got {
		key := candidate.SubscriptionID + "/" + candidate.Hash.Hex()
		if key != wantKeys[i] {
			t.Fatalf("candidate %d key: got %s, want %s", i, key, wantKeys[i])
		}
		seed := dueByKey[key]
		if string(candidate.RawOptions) != string(seed.raw) {
			t.Fatalf("candidate %d raw: got %s, want %s", i, candidate.RawOptions, seed.raw)
		}
		if !reflect.DeepEqual(candidate.Tags, seed.tags) {
			t.Fatalf("candidate %d tags: got %v, want %v", i, candidate.Tags, seed.tags)
		}
	}

	got, err = repo.LoadDueColdNodeCandidates(nowNs, interval, 20)
	if err != nil {
		t.Fatalf("LoadDueColdNodeCandidates full: %v", err)
	}
	if len(got) != len(wantKeys) {
		t.Fatalf("full candidates: got %d, want %d (%v)", len(got), len(wantKeys), wantKeys)
	}
	for i, candidate := range got {
		key := candidate.SubscriptionID + "/" + candidate.Hash.Hex()
		if key != wantKeys[i] {
			t.Fatalf("full candidate %d key: got %s, want %s", i, key, wantKeys[i])
		}
	}
	var sharedCandidate *topology.ColdNodeCandidate
	for i := range got {
		if got[i].Hash == sharedDueHash {
			sharedCandidate = &got[i]
			break
		}
	}
	if sharedCandidate == nil {
		t.Fatalf("shared due candidate %s missing from %+v", sharedDueHash.Hex(), got)
	}
	if len(sharedCandidate.Relations) != 2 || sharedCandidate.Relations[0].SubscriptionID != "sub-b" || sharedCandidate.Relations[1].SubscriptionID != "sub-c" {
		t.Fatalf("shared due relations: got %+v, want sub-b and sub-c", sharedCandidate.Relations)
	}
}

func TestCacheRepo_LoadDueColdNodeCandidates_UsesNextDueInsteadOfAttemptBackoff(t *testing.T) {
	repo := newTestCacheRepo(t)
	nowNs := int64(time.Hour)
	interval := 100 * time.Nanosecond

	rawFuture := json.RawMessage(`{"type":"stub","server":"198.51.100.30","server_port":443}`)
	rawDue := json.RawMessage(`{"type":"stub","server":"198.51.100.31","server_port":443}`)
	rawZero := json.RawMessage(`{"type":"stub","server":"198.51.100.32","server_port":443}`)
	hashFuture := node.HashFromRawOptions(rawFuture)
	hashDue := node.HashFromRawOptions(rawDue)
	hashZero := node.HashFromRawOptions(rawZero)

	if err := repo.BulkUpsertNodesStatic([]model.NodeStatic{
		{Hash: hashFuture.Hex(), RawOptions: rawFuture, CreatedAtNs: nowNs},
		{Hash: hashDue.Hex(), RawOptions: rawDue, CreatedAtNs: nowNs},
		{Hash: hashZero.Hex(), RawOptions: rawZero, CreatedAtNs: nowNs},
	}); err != nil {
		t.Fatalf("BulkUpsertNodesStatic: %v", err)
	}
	if err := repo.BulkUpsertSubscriptionNodes([]model.SubscriptionNode{
		{SubscriptionID: "sub-a", NodeHash: hashFuture.Hex(), Tags: []string{"future"}},
		{SubscriptionID: "sub-a", NodeHash: hashDue.Hex(), Tags: []string{"due"}},
		{SubscriptionID: "sub-a", NodeHash: hashZero.Hex(), Tags: []string{"zero"}},
	}); err != nil {
		t.Fatalf("BulkUpsertSubscriptionNodes: %v", err)
	}
	if err := repo.BulkUpsertNodesDynamic([]model.NodeDynamic{
		// Old attempt/backoff math would consider this due, but persisted next_due keeps it out.
		{Hash: hashFuture.Hex(), FailureCount: 9, LastLatencyProbeAttemptNs: nowNs - int64(100*interval), NextLatencyProbeDueNs: nowNs + 1},
		// Old attempt/backoff math would keep this out, but persisted next_due makes it due.
		{Hash: hashDue.Hex(), FailureCount: 9, LastLatencyProbeAttemptNs: nowNs - int64(interval), NextLatencyProbeDueNs: nowNs - 1},
		// Zero means unprobed/immediately due.
		{Hash: hashZero.Hex(), FailureCount: 9, LastLatencyProbeAttemptNs: nowNs - int64(interval), NextLatencyProbeDueNs: 0},
	}); err != nil {
		t.Fatalf("BulkUpsertNodesDynamic: %v", err)
	}

	got, err := repo.LoadDueColdNodeCandidates(nowNs, interval, 10)
	if err != nil {
		t.Fatalf("LoadDueColdNodeCandidates: %v", err)
	}
	gotKeys := make([]string, 0, len(got))
	for _, candidate := range got {
		gotKeys = append(gotKeys, candidate.Hash.Hex())
	}
	wantKeys := []string{hashDue.Hex(), hashZero.Hex()}
	sort.Strings(wantKeys)
	if !reflect.DeepEqual(gotKeys, wantKeys) {
		t.Fatalf("due candidates with persisted next due: got %v, want %v", gotKeys, wantKeys)
	}
}

func TestCacheRepo_LoadDueColdNodeCandidates_UsesPersistedNextLatencyProbeDue(t *testing.T) {
	repo := newTestCacheRepo(t)
	nowNs := int64(time.Hour)
	interval := 100 * time.Nanosecond

	rawFuture := json.RawMessage(`{"type":"stub","server":"198.51.100.33","server_port":443}`)
	rawDue := json.RawMessage(`{"type":"stub","server":"198.51.100.34","server_port":443}`)
	hashFuture := node.HashFromRawOptions(rawFuture)
	hashDue := node.HashFromRawOptions(rawDue)

	if err := repo.BulkUpsertNodesStatic([]model.NodeStatic{
		{Hash: hashFuture.Hex(), RawOptions: rawFuture, CreatedAtNs: nowNs},
		{Hash: hashDue.Hex(), RawOptions: rawDue, CreatedAtNs: nowNs},
	}); err != nil {
		t.Fatalf("BulkUpsertNodesStatic: %v", err)
	}
	if err := repo.BulkUpsertSubscriptionNodes([]model.SubscriptionNode{
		{SubscriptionID: "sub-a", NodeHash: hashFuture.Hex(), Tags: []string{"future"}},
		{SubscriptionID: "sub-a", NodeHash: hashDue.Hex(), Tags: []string{"due"}},
	}); err != nil {
		t.Fatalf("BulkUpsertSubscriptionNodes: %v", err)
	}
	if err := repo.BulkUpsertNodesDynamic([]model.NodeDynamic{
		{
			Hash:                      hashFuture.Hex(),
			FailureCount:              0,
			LastLatencyProbeAttemptNs: nowNs - int64(100*interval),
			NextLatencyProbeDueNs:     nowNs + 1,
		},
		{
			Hash:                      hashDue.Hex(),
			FailureCount:              9,
			LastLatencyProbeAttemptNs: nowNs - int64(interval),
			NextLatencyProbeDueNs:     nowNs - 1,
		},
	}); err != nil {
		t.Fatalf("BulkUpsertNodesDynamic: %v", err)
	}

	got, err := repo.LoadDueColdNodeCandidates(nowNs, interval, 10)
	if err != nil {
		t.Fatalf("LoadDueColdNodeCandidates: %v", err)
	}
	if len(got) != 1 || got[0].Hash != hashDue {
		t.Fatalf("due candidates with persisted next due: got %+v, want only %s", got, hashDue.Hex())
	}
}

func TestCacheRepo_LoadDueColdNodeCandidates_ExcludesDisabledSubscriptions(t *testing.T) {
	engine, closer, err := PersistenceBootstrap(t.TempDir(), t.TempDir())
	if err != nil {
		t.Fatalf("PersistenceBootstrap: %v", err)
	}
	t.Cleanup(func() { _ = closer.Close() })

	nowNs := int64(time.Hour)
	if err := engine.UpsertSubscription(model.Subscription{
		ID:               "sub-enabled",
		Name:             "Enabled",
		URL:              "https://example.com/enabled",
		UpdateIntervalNs: int64(30 * time.Minute),
		Enabled:          true,
		CreatedAtNs:      nowNs,
		UpdatedAtNs:      nowNs,
	}); err != nil {
		t.Fatalf("UpsertSubscription enabled: %v", err)
	}
	if err := engine.UpsertSubscription(model.Subscription{
		ID:               "sub-disabled",
		Name:             "Disabled",
		URL:              "https://example.com/disabled",
		UpdateIntervalNs: int64(30 * time.Minute),
		Enabled:          false,
		CreatedAtNs:      nowNs,
		UpdatedAtNs:      nowNs,
	}); err != nil {
		t.Fatalf("UpsertSubscription disabled: %v", err)
	}

	rawDisabledOnly := json.RawMessage(`{"type":"stub","server":"198.51.100.70","server_port":443}`)
	rawMixed := json.RawMessage(`{"type":"stub","server":"198.51.100.71","server_port":443}`)
	disabledOnlyHash := node.HashFromRawOptions(rawDisabledOnly)
	mixedHash := node.HashFromRawOptions(rawMixed)
	if err := engine.BulkUpsertNodesStatic([]model.NodeStatic{
		{Hash: disabledOnlyHash.Hex(), RawOptions: rawDisabledOnly, CreatedAtNs: nowNs},
		{Hash: mixedHash.Hex(), RawOptions: rawMixed, CreatedAtNs: nowNs},
	}); err != nil {
		t.Fatalf("BulkUpsertNodesStatic: %v", err)
	}
	if err := engine.BulkUpsertSubscriptionNodes([]model.SubscriptionNode{
		{SubscriptionID: "sub-disabled", NodeHash: disabledOnlyHash.Hex(), Tags: []string{"disabled-only"}},
		{SubscriptionID: "sub-disabled", NodeHash: mixedHash.Hex(), Tags: []string{"disabled-mixed"}},
		{SubscriptionID: "sub-enabled", NodeHash: mixedHash.Hex(), Tags: []string{"enabled-mixed"}},
	}); err != nil {
		t.Fatalf("BulkUpsertSubscriptionNodes: %v", err)
	}

	got, err := engine.LoadDueColdNodeCandidates(nowNs, time.Minute, 10)
	if err != nil {
		t.Fatalf("LoadDueColdNodeCandidates: %v", err)
	}
	if len(got) != 1 || got[0].Hash != mixedHash {
		t.Fatalf("due candidates: got %+v, want only mixed enabled relation hash %s", got, mixedHash.Hex())
	}
	if len(got[0].Relations) != 1 || got[0].Relations[0].SubscriptionID != "sub-enabled" {
		t.Fatalf("candidate relations: got %+v, want only enabled subscription relation", got[0].Relations)
	}

	relations, err := engine.LoadCurrentColdNodeRelations(mixedHash)
	if err != nil {
		t.Fatalf("LoadCurrentColdNodeRelations: %v", err)
	}
	if len(relations) != 1 || relations[0].SubscriptionID != "sub-enabled" {
		t.Fatalf("current cold relations: got %+v, want only enabled subscription relation", relations)
	}
}

func TestCacheRepo_LoadBootstrapActiveNodes_FiltersAtDBAndGroupsRelations(t *testing.T) {
	repo := newTestCacheRepo(t)
	nowNs := int64(20_000)

	rawActive := json.RawMessage(`{"type":"stub","server":"198.51.100.20","server_port":443}`)
	rawNoLatency := json.RawMessage(`{"type":"stub","server":"198.51.100.21","server_port":443}`)
	rawDisabled := json.RawMessage(`{"type":"stub","server":"198.51.100.22","server_port":443}`)
	rawEvicted := json.RawMessage(`{"type":"stub","server":"198.51.100.23","server_port":443}`)
	rawCircuitOpen := json.RawMessage(`{"type":"stub","server":"198.51.100.24","server_port":443}`)
	rawMissingDynamic := json.RawMessage(`{"type":"stub","server":"198.51.100.25","server_port":443}`)
	raws := []json.RawMessage{rawActive, rawNoLatency, rawDisabled, rawEvicted, rawCircuitOpen, rawMissingDynamic}
	hashes := make(map[string]string, len(raws))
	var statics []model.NodeStatic
	for _, raw := range raws {
		hashHex := node.HashFromRawOptions(raw).Hex()
		hashes[string(raw)] = hashHex
		statics = append(statics, model.NodeStatic{Hash: hashHex, RawOptions: raw, CreatedAtNs: nowNs})
	}
	if err := repo.BulkUpsertNodesStatic(statics); err != nil {
		t.Fatalf("BulkUpsertNodesStatic: %v", err)
	}
	if err := repo.BulkUpsertNodesDynamic([]model.NodeDynamic{
		{Hash: hashes[string(rawActive)], CircuitOpenSince: 0, EgressIP: "203.0.113.10", EgressIPs: []string{"203.0.113.10"}, EgressRegion: "sg", LastLatencyProbeAttemptNs: nowNs - 100},
		{Hash: hashes[string(rawNoLatency)], CircuitOpenSince: 0},
		{Hash: hashes[string(rawDisabled)], CircuitOpenSince: 0},
		{Hash: hashes[string(rawEvicted)], CircuitOpenSince: 0},
		{Hash: hashes[string(rawCircuitOpen)], CircuitOpenSince: nowNs - 1},
	}); err != nil {
		t.Fatalf("BulkUpsertNodesDynamic: %v", err)
	}
	if err := repo.BulkUpsertSubscriptionNodes([]model.SubscriptionNode{
		{SubscriptionID: "sub-a", NodeHash: hashes[string(rawActive)], Tags: []string{"a"}},
		{SubscriptionID: "sub-b", NodeHash: hashes[string(rawActive)], Tags: []string{"b"}},
		{SubscriptionID: "sub-a", NodeHash: hashes[string(rawNoLatency)], Tags: []string{"no-latency"}},
		{SubscriptionID: "sub-disabled", NodeHash: hashes[string(rawDisabled)], Tags: []string{"disabled"}},
		{SubscriptionID: "sub-a", NodeHash: hashes[string(rawEvicted)], Tags: []string{"evicted"}, Evicted: true},
		{SubscriptionID: "sub-a", NodeHash: hashes[string(rawCircuitOpen)], Tags: []string{"open"}},
		{SubscriptionID: "sub-a", NodeHash: hashes[string(rawMissingDynamic)], Tags: []string{"missing-dynamic"}},
	}); err != nil {
		t.Fatalf("BulkUpsertSubscriptionNodes: %v", err)
	}

	got, err := repo.LoadBootstrapActiveNodes([]string{"sub-b", "sub-a", "sub-a", ""})
	if err != nil {
		t.Fatalf("LoadBootstrapActiveNodes: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("active bootstrap nodes: got %d, want 2: %+v", len(got), got)
	}
	if got[0].Static.Hash != hashes[string(rawActive)] {
		t.Fatalf("first active hash: got %s, want %s", got[0].Static.Hash, hashes[string(rawActive)])
	}
	if got[0].Dynamic.EgressRegion != "sg" || !reflect.DeepEqual(got[0].Dynamic.EgressIPs, []string{"203.0.113.10"}) {
		t.Fatalf("active dynamic state not decoded: %+v", got[0].Dynamic)
	}
	if len(got[0].Relations) != 2 || got[0].Relations[0].SubscriptionID != "sub-a" || got[0].Relations[1].SubscriptionID != "sub-b" {
		t.Fatalf("active relations not grouped/sorted: %+v", got[0].Relations)
	}
	if got[1].Static.Hash != hashes[string(rawNoLatency)] {
		t.Fatalf("second active hash: got %s, want no-latency %s", got[1].Static.Hash, hashes[string(rawNoLatency)])
	}
}

// --- empty bulk operations ---

func TestCacheRepo_BulkEmpty(t *testing.T) {
	repo := newTestCacheRepo(t)

	// All empty bulk operations should be no-ops.
	if err := repo.BulkUpsertNodesStatic(nil); err != nil {
		t.Fatal(err)
	}
	if err := repo.BulkDeleteNodesStatic(nil); err != nil {
		t.Fatal(err)
	}
	if err := repo.BulkUpsertNodesDynamic(nil); err != nil {
		t.Fatal(err)
	}
	if err := repo.BulkDeleteNodesDynamic(nil); err != nil {
		t.Fatal(err)
	}
	if err := repo.BulkUpsertNodeLatency(nil); err != nil {
		t.Fatal(err)
	}
	if err := repo.BulkDeleteNodeLatency(nil); err != nil {
		t.Fatal(err)
	}
	if err := repo.BulkUpsertLeases(nil); err != nil {
		t.Fatal(err)
	}
	if err := repo.BulkDeleteLeases(nil); err != nil {
		t.Fatal(err)
	}
	if err := repo.BulkUpsertSubscriptionNodes(nil); err != nil {
		t.Fatal(err)
	}
	if err := repo.BulkDeleteSubscriptionNodes(nil); err != nil {
		t.Fatal(err)
	}
}

// TestCacheRepo_FlushTx_RollbackOnFailure verifies that if any step inside
// FlushTx fails, the entire transaction is rolled back and no partial writes
// are committed.
func TestCacheRepo_FlushTx_RollbackOnFailure(t *testing.T) {
	repo := newTestCacheRepo(t)

	// Seed: insert a node_static that should survive the failed FlushTx.
	seed := []model.NodeStatic{
		{Hash: "pre-existing", RawOptions: json.RawMessage(`{"seed":true}`), CreatedAtNs: 1},
	}
	if err := repo.BulkUpsertNodesStatic(seed); err != nil {
		t.Fatal(err)
	}

	// Drop node_latency table so that the upsert_node_latency step in FlushTx
	// will fail. nodes_static upsert runs first and would succeed in isolation.
	if _, err := repo.db.Exec("DROP TABLE node_latency"); err != nil {
		t.Fatal(err)
	}

	// Build a FlushOps that has work for both nodes_static and node_latency.
	ops := FlushOps{
		UpsertNodesStatic: []model.NodeStatic{
			{Hash: "new-node", RawOptions: json.RawMessage(`{"new":true}`), CreatedAtNs: 2},
		},
		UpsertNodeLatency: []model.NodeLatency{
			{NodeHash: "aaa", Domain: "example.com", EwmaNs: 100, LastUpdatedNs: 200},
		},
	}

	err := repo.FlushTx(ops)
	if err == nil {
		t.Fatal("expected FlushTx to fail because node_latency table was dropped")
	}

	// Verify rollback: "new-node" should NOT be committed.
	loaded, err := repo.LoadAllNodesStatic()
	if err != nil {
		t.Fatal(err)
	}
	if len(loaded) != 1 {
		t.Fatalf("expected 1 pre-existing node (rollback should prevent new-node), got %d: %+v", len(loaded), loaded)
	}
	if loaded[0].Hash != "pre-existing" {
		t.Fatalf("expected pre-existing node, got %s", loaded[0].Hash)
	}
}
