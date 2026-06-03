package state

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/Resinat/Resin/internal/model"
	"github.com/Resinat/Resin/internal/node"
	"github.com/Resinat/Resin/internal/topology"
)

// CacheRepo wraps cache.db and provides batch read/write for weak-persist data.
type CacheRepo struct {
	db *sql.DB
}

// newCacheRepo creates a CacheRepo for the given cache.db connection.
func newCacheRepo(db *sql.DB) *CacheRepo {
	return &CacheRepo{db: db}
}

const sqliteQueryParamBatchSize = 900

func probeFailureBackoffMultiplier(failureCount int) int64 {
	if failureCount <= 0 {
		return 1
	}
	if failureCount > 5 {
		failureCount = 5
	}
	return int64(1) << failureCount
}

// --- nodes_static ---

// BulkUpsertNodesStatic batch-inserts or updates node static records.
func (r *CacheRepo) BulkUpsertNodesStatic(nodes []model.NodeStatic) error {
	return bulkExecRows(
		r,
		upsertNodesStaticSQL,
		nodes,
		func(stmt *sql.Stmt, n model.NodeStatic) error {
			_, err := stmt.Exec(n.Hash, string(n.RawOptions), n.CreatedAtNs)
			return err
		},
	)
}

// BulkDeleteNodesStatic batch-deletes node static records by hash.
func (r *CacheRepo) BulkDeleteNodesStatic(hashes []string) error {
	return bulkExecRows(
		r,
		deleteNodesStaticSQL,
		hashes,
		func(stmt *sql.Stmt, hash string) error {
			_, err := stmt.Exec(hash)
			return err
		},
	)
}

// LoadAllNodesStatic reads all node static records.
func (r *CacheRepo) LoadAllNodesStatic() ([]model.NodeStatic, error) {
	rows, err := r.db.Query("SELECT hash, raw_options_json, created_at_ns FROM nodes_static")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []model.NodeStatic
	for rows.Next() {
		var n model.NodeStatic
		var rawOptionsJSON string
		if err := rows.Scan(&n.Hash, &rawOptionsJSON, &n.CreatedAtNs); err != nil {
			return nil, err
		}
		n.RawOptions = json.RawMessage(rawOptionsJSON)
		result = append(result, n)
	}
	return result, rows.Err()
}

// --- nodes_dynamic ---

// BulkUpsertNodesDynamic batch-inserts or updates node dynamic records.
func (r *CacheRepo) BulkUpsertNodesDynamic(nodes []model.NodeDynamic) error {
	return bulkExecRows(
		r,
		upsertNodesDynamicSQL,
		nodes,
		func(stmt *sql.Stmt, n model.NodeDynamic) error {
			egressIPsJSON, err := encodeStringSliceJSON(n.EgressIPs)
			if err != nil {
				return fmt.Errorf("encode node dynamic egress_ips: %w", err)
			}
			_, err = stmt.Exec(
				n.Hash,
				n.FailureCount,
				n.CircuitOpenSince,
				n.EgressIP,
				egressIPsJSON,
				n.EgressRegion,
				n.EgressUpdatedAtNs,
				n.LastLatencyProbeAttemptNs,
				n.NextLatencyProbeDueNs,
				n.LastAuthorityLatencyProbeAttemptNs,
				n.LastEgressUpdateAttemptNs,
			)
			return err
		},
	)
}

// BulkDeleteNodesDynamic batch-deletes node dynamic records by hash.
func (r *CacheRepo) BulkDeleteNodesDynamic(hashes []string) error {
	return bulkExecRows(
		r,
		deleteNodesDynamicSQL,
		hashes,
		func(stmt *sql.Stmt, hash string) error {
			_, err := stmt.Exec(hash)
			return err
		},
	)
}

// LoadNodeDynamic reads one node dynamic record by hash.
func (r *CacheRepo) LoadNodeDynamic(hash string) (*model.NodeDynamic, error) {
	row := r.db.QueryRow(`
		SELECT hash, failure_count, circuit_open_since, egress_ip, egress_ips_json, egress_region, egress_updated_at_ns,
		       last_latency_probe_attempt_ns, next_latency_probe_due_ns, last_authority_latency_probe_attempt_ns, last_egress_update_attempt_ns
		FROM nodes_dynamic
		WHERE hash = ?`, hash)
	var n model.NodeDynamic
	var egressIPsJSON string
	if err := row.Scan(
		&n.Hash,
		&n.FailureCount,
		&n.CircuitOpenSince,
		&n.EgressIP,
		&egressIPsJSON,
		&n.EgressRegion,
		&n.EgressUpdatedAtNs,
		&n.LastLatencyProbeAttemptNs,
		&n.NextLatencyProbeDueNs,
		&n.LastAuthorityLatencyProbeAttemptNs,
		&n.LastEgressUpdateAttemptNs,
	); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	egressIPs, err := decodeStringSliceJSON(egressIPsJSON)
	if err != nil {
		return nil, fmt.Errorf("decode node dynamic egress_ips for %s: %w", n.Hash, err)
	}
	n.EgressIPs = egressIPs
	return &n, nil
}

// LoadAllNodesDynamic reads all node dynamic records.
func (r *CacheRepo) LoadAllNodesDynamic() ([]model.NodeDynamic, error) {
	rows, err := r.db.Query(`
		SELECT hash, failure_count, circuit_open_since, egress_ip, egress_ips_json, egress_region, egress_updated_at_ns,
		       last_latency_probe_attempt_ns, next_latency_probe_due_ns, last_authority_latency_probe_attempt_ns, last_egress_update_attempt_ns
		FROM nodes_dynamic`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []model.NodeDynamic
	for rows.Next() {
		var n model.NodeDynamic
		var egressIPsJSON string
		if err := rows.Scan(
			&n.Hash,
			&n.FailureCount,
			&n.CircuitOpenSince,
			&n.EgressIP,
			&egressIPsJSON,
			&n.EgressRegion,
			&n.EgressUpdatedAtNs,
			&n.LastLatencyProbeAttemptNs,
			&n.NextLatencyProbeDueNs,
			&n.LastAuthorityLatencyProbeAttemptNs,
			&n.LastEgressUpdateAttemptNs,
		); err != nil {
			return nil, err
		}
		egressIPs, err := decodeStringSliceJSON(egressIPsJSON)
		if err != nil {
			return nil, fmt.Errorf("decode node dynamic egress_ips for %s: %w", n.Hash, err)
		}
		n.EgressIPs = egressIPs
		result = append(result, n)
	}
	return result, rows.Err()
}

// --- node_latency ---

// BulkUpsertNodeLatency batch-inserts or updates node latency records.
func (r *CacheRepo) BulkUpsertNodeLatency(entries []model.NodeLatency) error {
	return bulkExecRows(
		r,
		upsertNodeLatencySQL,
		entries,
		func(stmt *sql.Stmt, e model.NodeLatency) error {
			_, err := stmt.Exec(e.NodeHash, e.Domain, e.EwmaNs, e.LastUpdatedNs)
			return err
		},
	)
}

// BulkDeleteNodeLatency batch-deletes node latency records by composite key.
func (r *CacheRepo) BulkDeleteNodeLatency(keys []model.NodeLatencyKey) error {
	return bulkExecRows(
		r,
		deleteNodeLatencySQL,
		keys,
		func(stmt *sql.Stmt, key model.NodeLatencyKey) error {
			_, err := stmt.Exec(key.NodeHash, key.Domain)
			return err
		},
	)
}

// LoadAllNodeLatency reads all node latency records.
func (r *CacheRepo) LoadAllNodeLatency() ([]model.NodeLatency, error) {
	rows, err := r.db.Query("SELECT node_hash, domain, ewma_ns, last_updated_ns FROM node_latency")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []model.NodeLatency
	for rows.Next() {
		var e model.NodeLatency
		if err := rows.Scan(&e.NodeHash, &e.Domain, &e.EwmaNs, &e.LastUpdatedNs); err != nil {
			return nil, err
		}
		result = append(result, e)
	}
	return result, rows.Err()
}

// LoadBootstrapActiveNodes reads only persisted active runtime node candidates.
// It filters at the DB layer so active-only bootstrap does not materialize the
// full inventory before pruning it in memory. The active predicate intentionally
// does not require latency samples: enabled subscription + non-evicted relation
// + closed circuit dynamic state are enough to restore the node and let outbound
// construction decide final runtime eligibility.
func (r *CacheRepo) LoadBootstrapActiveNodes(enabledSubscriptionIDs []string) ([]topology.BootstrapActiveNode, error) {
	ids := compactUniqueStrings(enabledSubscriptionIDs)
	if len(ids) == 0 {
		return nil, nil
	}

	byHash := make(map[string]*topology.BootstrapActiveNode)
	for start := 0; start < len(ids); start += sqliteQueryParamBatchSize {
		end := start + sqliteQueryParamBatchSize
		if end > len(ids) {
			end = len(ids)
		}
		if err := r.loadBootstrapActiveNodesBatch(ids[start:end], byHash); err != nil {
			return nil, err
		}
	}

	result := make([]topology.BootstrapActiveNode, 0, len(byHash))
	for _, record := range byHash {
		sort.SliceStable(record.Relations, func(i, j int) bool {
			return record.Relations[i].SubscriptionID < record.Relations[j].SubscriptionID
		})
		result = append(result, *record)
	}
	sort.SliceStable(result, func(i, j int) bool {
		leftRaw := string(result[i].Static.RawOptions)
		rightRaw := string(result[j].Static.RawOptions)
		if leftRaw == rightRaw {
			return result[i].Static.Hash < result[j].Static.Hash
		}
		return leftRaw < rightRaw
	})
	return result, nil
}

func (r *CacheRepo) loadBootstrapActiveNodesBatch(ids []string, byHash map[string]*topology.BootstrapActiveNode) error {
	if len(ids) == 0 {
		return nil
	}
	placeholders := strings.TrimRight(strings.Repeat("?,", len(ids)), ",")
	args := make([]any, 0, len(ids))
	for _, id := range ids {
		args = append(args, id)
	}

	rows, err := r.db.Query(`
		SELECT ns.hash, ns.raw_options_json, ns.created_at_ns,
		       nd.failure_count, nd.circuit_open_since, nd.egress_ip, nd.egress_ips_json, nd.egress_region,
		       nd.egress_updated_at_ns, nd.last_latency_probe_attempt_ns,
		       nd.next_latency_probe_due_ns, nd.last_authority_latency_probe_attempt_ns, nd.last_egress_update_attempt_ns,
		       sn.subscription_id, sn.tags_json
		FROM nodes_static AS ns
		JOIN nodes_dynamic AS nd ON nd.hash = ns.hash
		JOIN subscription_nodes AS sn ON sn.node_hash = ns.hash
		WHERE sn.evicted = 0
		  AND nd.circuit_open_since = 0
		  AND sn.subscription_id IN (`+placeholders+`)
		ORDER BY ns.raw_options_json ASC, ns.hash ASC, sn.subscription_id ASC`, args...)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var record topology.BootstrapActiveNode
		var rawOptionsJSON, egressIPsJSON, tagsJSON string
		var relationSubID string
		if err := rows.Scan(
			&record.Static.Hash,
			&rawOptionsJSON,
			&record.Static.CreatedAtNs,
			&record.Dynamic.FailureCount,
			&record.Dynamic.CircuitOpenSince,
			&record.Dynamic.EgressIP,
			&egressIPsJSON,
			&record.Dynamic.EgressRegion,
			&record.Dynamic.EgressUpdatedAtNs,
			&record.Dynamic.LastLatencyProbeAttemptNs,
			&record.Dynamic.NextLatencyProbeDueNs,
			&record.Dynamic.LastAuthorityLatencyProbeAttemptNs,
			&record.Dynamic.LastEgressUpdateAttemptNs,
			&relationSubID,
			&tagsJSON,
		); err != nil {
			return err
		}
		record.Static.RawOptions = json.RawMessage(rawOptionsJSON)
		record.Dynamic.Hash = record.Static.Hash
		egressIPs, err := decodeStringSliceJSON(egressIPsJSON)
		if err != nil {
			return fmt.Errorf("decode node dynamic egress_ips for %s: %w", record.Static.Hash, err)
		}
		tags, err := decodeStringSliceJSON(tagsJSON)
		if err != nil {
			return fmt.Errorf("decode subscription node tags_json for %s/%s: %w", relationSubID, record.Static.Hash, err)
		}

		existing, ok := byHash[record.Static.Hash]
		if !ok {
			record.Dynamic.EgressIPs = egressIPs
			record.Relations = []model.SubscriptionNode{{
				SubscriptionID: relationSubID,
				NodeHash:       record.Static.Hash,
				Tags:           tags,
			}}
			byHash[record.Static.Hash] = &record
			continue
		}
		existing.Relations = append(existing.Relations, model.SubscriptionNode{
			SubscriptionID: relationSubID,
			NodeHash:       record.Static.Hash,
			Tags:           tags,
		})
	}
	return rows.Err()
}

// LoadNodeLatencyForHashes reads persisted latency rows for a bounded set of node hashes.
func (r *CacheRepo) LoadNodeLatencyForHashes(hashes []string) ([]model.NodeLatency, error) {
	hashes = compactUniqueStrings(hashes)
	if len(hashes) == 0 {
		return []model.NodeLatency{}, nil
	}

	result := make([]model.NodeLatency, 0)
	for start := 0; start < len(hashes); start += sqliteQueryParamBatchSize {
		end := start + sqliteQueryParamBatchSize
		if end > len(hashes) {
			end = len(hashes)
		}
		rows, err := r.loadNodeLatencyForHashBatch(hashes[start:end])
		if err != nil {
			return nil, err
		}
		result = append(result, rows...)
	}
	return result, nil
}

func (r *CacheRepo) loadNodeLatencyForHashBatch(hashes []string) ([]model.NodeLatency, error) {
	if len(hashes) == 0 {
		return nil, nil
	}
	placeholders := strings.TrimRight(strings.Repeat("?,", len(hashes)), ",")
	args := make([]any, 0, len(hashes))
	for _, hash := range hashes {
		args = append(args, hash)
	}
	rows, err := r.db.Query(`
		SELECT node_hash, domain, ewma_ns, last_updated_ns
		FROM node_latency
		WHERE node_hash IN (`+placeholders+`)
		ORDER BY node_hash ASC, last_updated_ns DESC, domain ASC`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []model.NodeLatency
	for rows.Next() {
		var e model.NodeLatency
		if err := rows.Scan(&e.NodeHash, &e.Domain, &e.EwmaNs, &e.LastUpdatedNs); err != nil {
			return nil, err
		}
		result = append(result, e)
	}
	return result, rows.Err()
}

// --- leases ---
func (r *CacheRepo) BulkUpsertLeases(leases []model.Lease) error {
	return bulkExecRows(
		r,
		upsertLeasesSQL,
		leases,
		func(stmt *sql.Stmt, l model.Lease) error {
			_, err := stmt.Exec(l.PlatformID, l.Account, l.NodeHash, l.EgressIP, l.CreatedAtNs, l.ExpiryNs, l.LastAccessedNs)
			return err
		},
	)
}

// BulkDeleteLeases batch-deletes lease records by composite key.
func (r *CacheRepo) BulkDeleteLeases(keys []model.LeaseKey) error {
	return bulkExecRows(
		r,
		deleteLeasesSQL,
		keys,
		func(stmt *sql.Stmt, key model.LeaseKey) error {
			_, err := stmt.Exec(key.PlatformID, key.Account)
			return err
		},
	)
}

// LoadAllLeases reads all lease records.
func (r *CacheRepo) LoadAllLeases() ([]model.Lease, error) {
	rows, err := r.db.Query("SELECT platform_id, account, node_hash, egress_ip, created_at_ns, expiry_ns, last_accessed_ns FROM leases")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []model.Lease
	for rows.Next() {
		var l model.Lease
		if err := rows.Scan(&l.PlatformID, &l.Account, &l.NodeHash, &l.EgressIP, &l.CreatedAtNs, &l.ExpiryNs, &l.LastAccessedNs); err != nil {
			return nil, err
		}
		result = append(result, l)
	}
	return result, rows.Err()
}

// --- subscription_nodes ---

// BulkUpsertSubscriptionNodes batch-inserts or updates subscription-node links.
func (r *CacheRepo) BulkUpsertSubscriptionNodes(nodes []model.SubscriptionNode) error {
	return bulkExecRows(
		r,
		upsertSubscriptionNodesSQL,
		nodes,
		func(stmt *sql.Stmt, sn model.SubscriptionNode) error {
			tagsJSON, err := encodeStringSliceJSON(sn.Tags)
			if err != nil {
				return fmt.Errorf("encode subscription node tags: %w", err)
			}
			_, err = stmt.Exec(sn.SubscriptionID, sn.NodeHash, tagsJSON, sn.Evicted)
			return err
		},
	)
}

// BulkDeleteSubscriptionNodes batch-deletes subscription-node links by composite key.
func (r *CacheRepo) BulkDeleteSubscriptionNodes(keys []model.SubscriptionNodeKey) error {
	return bulkExecRows(
		r,
		deleteSubscriptionNodesSQL,
		keys,
		func(stmt *sql.Stmt, key model.SubscriptionNodeKey) error {
			_, err := stmt.Exec(key.SubscriptionID, key.NodeHash)
			return err
		},
	)
}

// LoadAllSubscriptionNodes reads all subscription-node links.
func (r *CacheRepo) LoadAllSubscriptionNodes() ([]model.SubscriptionNode, error) {
	rows, err := r.db.Query("SELECT subscription_id, node_hash, tags_json, evicted FROM subscription_nodes")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []model.SubscriptionNode
	for rows.Next() {
		var sn model.SubscriptionNode
		var tagsJSON string
		if err := rows.Scan(&sn.SubscriptionID, &sn.NodeHash, &tagsJSON, &sn.Evicted); err != nil {
			return nil, err
		}
		tags, err := decodeStringSliceJSON(tagsJSON)
		if err != nil {
			return nil, fmt.Errorf("decode subscription node tags_json: %w", err)
		}
		sn.Tags = tags
		result = append(result, sn)
	}
	return result, rows.Err()
}

// LoadSubscriptionNodes reads subscription-node links for one subscription.
func (r *CacheRepo) LoadSubscriptionNodes(subID string) ([]model.SubscriptionNode, error) {
	rows, err := r.db.Query(
		"SELECT subscription_id, node_hash, tags_json, evicted FROM subscription_nodes WHERE subscription_id = ?",
		subID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []model.SubscriptionNode
	for rows.Next() {
		var sn model.SubscriptionNode
		var tagsJSON string
		if err := rows.Scan(&sn.SubscriptionID, &sn.NodeHash, &tagsJSON, &sn.Evicted); err != nil {
			return nil, err
		}
		tags, err := decodeStringSliceJSON(tagsJSON)
		if err != nil {
			return nil, fmt.Errorf("decode subscription node tags_json for %s/%s: %w", sn.SubscriptionID, sn.NodeHash, err)
		}
		sn.Tags = tags
		result = append(result, sn)
	}
	return result, rows.Err()
}

// IsColdNodeRelationCurrent reports whether the non-evicted subscription-node
// relation is still present in the authoritative inventory.
func (r *CacheRepo) IsColdNodeRelationCurrent(subID string, hash node.Hash) bool {
	var exists int
	err := r.db.QueryRow(
		"SELECT 1 FROM subscription_nodes WHERE subscription_id = ? AND node_hash = ? AND evicted = 0 LIMIT 1",
		subID,
		hash.Hex(),
	).Scan(&exists)
	return err == nil
}

// LoadCurrentColdNodeRelations returns all current non-evicted inventory
// relations for a cold-check node. Promotion uses this fresh view instead of a
// potentially stale sweep snapshot so in-flight subscription refreshes are not
// left cold after the node proves healthy.
func (r *CacheRepo) LoadCurrentColdNodeRelations(hash node.Hash) ([]topology.ColdNodeRelation, error) {
	return r.loadCurrentColdNodeRelations(hash, nil)
}

func (r *CacheRepo) loadCurrentColdNodeRelations(hash node.Hash, enabledSubscriptionIDs []string) ([]topology.ColdNodeRelation, error) {
	enabledSubscriptionIDs = compactUniqueStrings(enabledSubscriptionIDs)
	enabledFilter := ""
	args := []any{hash.Hex()}
	if len(enabledSubscriptionIDs) > 0 {
		placeholders := strings.TrimRight(strings.Repeat("?,", len(enabledSubscriptionIDs)), ",")
		enabledFilter = " AND subscription_id IN (" + placeholders + ")"
		for _, subID := range enabledSubscriptionIDs {
			args = append(args, subID)
		}
	}
	rows, err := r.db.Query(
		"SELECT subscription_id, tags_json FROM subscription_nodes WHERE node_hash = ? AND evicted = 0"+enabledFilter+" ORDER BY subscription_id ASC",
		args...,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	relations := make([]topology.ColdNodeRelation, 0)
	for rows.Next() {
		var subID, tagsJSON string
		if err := rows.Scan(&subID, &tagsJSON); err != nil {
			return nil, err
		}
		tags, err := decodeStringSliceJSON(tagsJSON)
		if err != nil {
			return nil, fmt.Errorf("decode cold relation tags_json for %s/%s: %w", subID, hash.Hex(), err)
		}
		relations = append(relations, topology.ColdNodeRelation{
			SubscriptionID: subID,
			Tags:           append([]string(nil), tags...),
		})
	}
	return relations, rows.Err()
}

// LoadDueColdNodeCandidates reads node-scoped cold-check candidates whose
// persisted latency probe due timestamp is missing, zero, or due by now.
// Each candidate carries all non-evicted inventory relations for that node so a
// successful cold check can restore the full node relationship set atomically.
func (r *CacheRepo) LoadDueColdNodeCandidates(nowNs int64, interval time.Duration, limit int) ([]topology.ColdNodeCandidate, error) {
	return r.loadDueColdNodeCandidates(nowNs, interval, limit, nil)
}

func (r *CacheRepo) loadDueColdNodeCandidates(nowNs int64, interval time.Duration, limit int, enabledSubscriptionIDs []string) ([]topology.ColdNodeCandidate, error) {
	if limit <= 0 {
		return nil, nil
	}
	_ = interval
	enabledSubscriptionIDs = compactUniqueStrings(enabledSubscriptionIDs)

	selected, err := r.loadDueColdNodeCandidateHashes(nowNs, limit, enabledSubscriptionIDs)
	if err != nil {
		return nil, err
	}
	if len(selected) == 0 {
		return nil, nil
	}
	return r.loadColdNodeCandidatesForHashes(selected, enabledSubscriptionIDs)
}

type coldNodeCandidateHash struct {
	hashHex   string
	nextDueNs int64
}

func (r *CacheRepo) loadDueColdNodeCandidateHashes(nowNs int64, limit int, enabledSubscriptionIDs []string) ([]coldNodeCandidateHash, error) {
	selected, err := r.loadImmediateColdNodeCandidateHashes(limit, enabledSubscriptionIDs)
	if err != nil {
		return nil, err
	}
	if len(selected) >= limit {
		return selected, nil
	}
	positiveDue, err := r.loadPositiveDueColdNodeCandidateHashes(nowNs, limit-len(selected), enabledSubscriptionIDs)
	if err != nil {
		return nil, err
	}
	selected = append(selected, positiveDue...)
	return selected, nil
}

func enabledSubscriptionSQLFilter(alias string, enabledSubscriptionIDs []string) (string, []any) {
	if len(enabledSubscriptionIDs) == 0 {
		return "", nil
	}
	placeholders := strings.TrimRight(strings.Repeat("?,", len(enabledSubscriptionIDs)), ",")
	args := make([]any, 0, len(enabledSubscriptionIDs))
	for _, subID := range enabledSubscriptionIDs {
		args = append(args, subID)
	}
	return " AND " + alias + ".subscription_id IN (" + placeholders + ")", args
}

func (r *CacheRepo) loadImmediateColdNodeCandidateHashes(limit int, enabledSubscriptionIDs []string) ([]coldNodeCandidateHash, error) {
	enabledFilter, enabledArgs := enabledSubscriptionSQLFilter("sn", enabledSubscriptionIDs)
	args := make([]any, 0, len(enabledArgs)*2+1)
	args = append(args, enabledArgs...)
	args = append(args, enabledArgs...)
	args = append(args, limit)

	rows, err := r.db.Query(`
		WITH immediate_hashes(node_hash, next_due_ns) AS (
			SELECT nd.hash, nd.next_latency_probe_due_ns
			FROM nodes_dynamic AS nd
			WHERE nd.next_latency_probe_due_ns <= 0
			  AND EXISTS (SELECT 1 FROM nodes_static AS ns WHERE ns.hash = nd.hash)
			  AND EXISTS (
				SELECT 1
				FROM subscription_nodes AS sn
				WHERE sn.node_hash = nd.hash
				  AND sn.evicted = 0`+enabledFilter+`
			  )
			UNION ALL
			SELECT ns.hash, 0
			FROM nodes_static AS ns
			WHERE NOT EXISTS (SELECT 1 FROM nodes_dynamic AS nd WHERE nd.hash = ns.hash)
			  AND EXISTS (
				SELECT 1
				FROM subscription_nodes AS sn
				WHERE sn.node_hash = ns.hash
				  AND sn.evicted = 0`+enabledFilter+`
			  )
		)
		SELECT node_hash, next_due_ns
		FROM immediate_hashes
		ORDER BY next_due_ns ASC, node_hash ASC
		LIMIT ?`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanColdNodeCandidateHashes(rows, limit)
}

func (r *CacheRepo) loadPositiveDueColdNodeCandidateHashes(nowNs int64, limit int, enabledSubscriptionIDs []string) ([]coldNodeCandidateHash, error) {
	if limit <= 0 {
		return nil, nil
	}
	enabledFilter, enabledArgs := enabledSubscriptionSQLFilter("sn", enabledSubscriptionIDs)
	args := make([]any, 0, len(enabledArgs)+2)
	args = append(args, nowNs)
	args = append(args, enabledArgs...)
	args = append(args, limit)

	rows, err := r.db.Query(`
		SELECT nd.hash, nd.next_latency_probe_due_ns
		FROM nodes_dynamic AS nd
		WHERE nd.next_latency_probe_due_ns > 0
		  AND nd.next_latency_probe_due_ns <= ?
		  AND EXISTS (SELECT 1 FROM nodes_static AS ns WHERE ns.hash = nd.hash)
		  AND EXISTS (
			SELECT 1
			FROM subscription_nodes AS sn
			WHERE sn.node_hash = nd.hash
			  AND sn.evicted = 0`+enabledFilter+`
		  )
		ORDER BY nd.next_latency_probe_due_ns ASC, nd.hash ASC
		LIMIT ?`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanColdNodeCandidateHashes(rows, limit)
}

func scanColdNodeCandidateHashes(rows *sql.Rows, limit int) ([]coldNodeCandidateHash, error) {
	selected := make([]coldNodeCandidateHash, 0, limit)
	for rows.Next() {
		var item coldNodeCandidateHash
		if err := rows.Scan(&item.hashHex, &item.nextDueNs); err != nil {
			return nil, err
		}
		selected = append(selected, item)
	}
	return selected, rows.Err()
}

func (r *CacheRepo) loadColdNodeCandidatesForHashes(selected []coldNodeCandidateHash, enabledSubscriptionIDs []string) ([]topology.ColdNodeCandidate, error) {
	if len(selected) == 0 {
		return nil, nil
	}

	selectedHashes := make([]string, 0, len(selected))
	for _, item := range selected {
		selectedHashes = append(selectedHashes, item.hashHex)
	}

	byHash := make(map[string]*topology.ColdNodeCandidate, len(selected))
	for start := 0; start < len(selectedHashes); start += sqliteQueryParamBatchSize {
		end := start + sqliteQueryParamBatchSize
		if end > len(selectedHashes) {
			end = len(selectedHashes)
		}
		if err := r.loadColdNodeCandidatesForHashBatch(selectedHashes[start:end], enabledSubscriptionIDs, byHash); err != nil {
			return nil, err
		}
	}

	candidates := make([]topology.ColdNodeCandidate, 0, len(byHash))
	for _, item := range selected {
		candidate, ok := byHash[item.hashHex]
		if !ok {
			continue
		}
		sort.SliceStable(candidate.Relations, func(i, j int) bool {
			return candidate.Relations[i].SubscriptionID < candidate.Relations[j].SubscriptionID
		})
		if len(candidate.Relations) > 0 {
			candidate.SubscriptionID = candidate.Relations[0].SubscriptionID
			candidate.Tags = append([]string(nil), candidate.Relations[0].Tags...)
		}
		candidates = append(candidates, *candidate)
	}
	return candidates, nil
}

func (r *CacheRepo) loadColdNodeCandidatesForHashBatch(hashBatch []string, enabledSubscriptionIDs []string, byHash map[string]*topology.ColdNodeCandidate) error {
	if len(hashBatch) == 0 {
		return nil
	}
	hashPlaceholders := strings.TrimRight(strings.Repeat("?,", len(hashBatch)), ",")
	args := make([]any, 0, len(hashBatch)+len(enabledSubscriptionIDs))
	for _, hash := range hashBatch {
		args = append(args, hash)
	}
	enabledFilter := ""
	if len(enabledSubscriptionIDs) > 0 {
		placeholders := strings.TrimRight(strings.Repeat("?,", len(enabledSubscriptionIDs)), ",")
		enabledFilter = " AND sn.subscription_id IN (" + placeholders + ")"
		for _, subID := range enabledSubscriptionIDs {
			args = append(args, subID)
		}
	}

	rows, err := r.db.Query(`
		SELECT ns.hash, ns.raw_options_json, sn.subscription_id, sn.tags_json
		FROM nodes_static AS ns
		JOIN subscription_nodes AS sn ON sn.node_hash = ns.hash
		WHERE ns.hash IN (`+hashPlaceholders+`)
		  AND sn.evicted = 0`+enabledFilter+`
		ORDER BY ns.hash ASC, sn.subscription_id ASC`, args...)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var hashHex, rawOptionsJSON, subID, tagsJSON string
		if err := rows.Scan(&hashHex, &rawOptionsJSON, &subID, &tagsJSON); err != nil {
			return err
		}
		tags, err := decodeStringSliceJSON(tagsJSON)
		if err != nil {
			return fmt.Errorf("decode cold candidate tags_json for %s/%s: %w", subID, hashHex, err)
		}
		candidate, ok := byHash[hashHex]
		if !ok {
			hash, err := node.ParseHex(hashHex)
			if err != nil {
				return fmt.Errorf("parse cold candidate hash %s: %w", hashHex, err)
			}
			candidate = &topology.ColdNodeCandidate{
				Hash:       hash,
				RawOptions: json.RawMessage(rawOptionsJSON),
			}
			byHash[hashHex] = candidate
		}
		candidate.Relations = append(candidate.Relations, topology.ColdNodeRelation{
			SubscriptionID: subID,
			Tags:           append([]string(nil), tags...),
		})
	}
	return rows.Err()
}

// ReplaceSubscriptionRefresh atomically applies a DB-first refresh inventory diff.
// It upserts parsed node static rows before subscription-node relation changes
// so cold candidates are durable even when they are not promoted to memory.
func (r *CacheRepo) ReplaceSubscriptionRefresh(
	subID string,
	statics []model.NodeStatic,
	upserts []model.SubscriptionNode,
	deletes []model.SubscriptionNodeKey,
) error {
	tx, err := r.db.Begin()
	if err != nil {
		return fmt.Errorf("begin subscription refresh tx: %w", err)
	}
	defer tx.Rollback()

	if err := bulkExecTx(tx, upsertNodesStaticSQL, len(statics), func(stmt *sql.Stmt, i int) error {
		n := statics[i]
		_, err := stmt.Exec(n.Hash, string(n.RawOptions), n.CreatedAtNs)
		return err
	}); err != nil {
		return fmt.Errorf("upsert refresh nodes_static: %w", err)
	}

	if err := bulkExecTx(tx, upsertSubscriptionNodesSQL, len(upserts), func(stmt *sql.Stmt, i int) error {
		sn := upserts[i]
		if sn.SubscriptionID == "" {
			sn.SubscriptionID = subID
		}
		tagsJSON, err := encodeStringSliceJSON(sn.Tags)
		if err != nil {
			return fmt.Errorf("encode subscription node tags: %w", err)
		}
		_, err = stmt.Exec(sn.SubscriptionID, sn.NodeHash, tagsJSON, sn.Evicted)
		return err
	}); err != nil {
		return fmt.Errorf("upsert refresh subscription_nodes: %w", err)
	}

	if err := bulkExecTx(tx, deleteSubscriptionNodesSQL, len(deletes), func(stmt *sql.Stmt, i int) error {
		key := deletes[i]
		if key.SubscriptionID == "" {
			key.SubscriptionID = subID
		}
		_, err := stmt.Exec(key.SubscriptionID, key.NodeHash)
		return err
	}); err != nil {
		return fmt.Errorf("delete refresh subscription_nodes: %w", err)
	}

	return tx.Commit()
}

// --- internal helpers ---

// bulkExecTx runs a prepared statement within an existing transaction for n rows.
func bulkExecTx(tx *sql.Tx, query string, n int, execFn func(stmt *sql.Stmt, i int) error) error {
	if n == 0 {
		return nil
	}

	stmt, err := tx.Prepare(query)
	if err != nil {
		return fmt.Errorf("prepare: %w", err)
	}
	defer stmt.Close()

	for i := 0; i < n; i++ {
		if err := execFn(stmt, i); err != nil {
			return fmt.Errorf("exec row %d: %w", i, err)
		}
	}
	return nil
}

// bulkExec runs a prepared statement in its own transaction for n rows.
// Used by individual BulkUpsert*/BulkDelete* methods (tests, bootstrap).
func (r *CacheRepo) bulkExec(query string, n int, execFn func(stmt *sql.Stmt, i int) error) error {
	if n == 0 {
		return nil
	}

	tx, err := r.db.Begin()
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	if err := bulkExecTx(tx, query, n, execFn); err != nil {
		return err
	}
	return tx.Commit()
}

func bulkExecRows[T any](
	r *CacheRepo,
	query string,
	rows []T,
	execFn func(stmt *sql.Stmt, row T) error,
) error {
	return r.bulkExec(query, len(rows), func(stmt *sql.Stmt, i int) error {
		return execFn(stmt, rows[i])
	})
}

func compactUniqueStrings(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	result := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		result = append(result, value)
	}
	sort.Strings(result)
	return result
}

// FlushOps holds all upsert/delete slices for a single-transaction cache flush.
type FlushOps struct {
	UpsertNodesStatic       []model.NodeStatic
	DeleteNodesStatic       []string
	UpsertSubscriptionNodes []model.SubscriptionNode
	DeleteSubscriptionNodes []model.SubscriptionNodeKey
	UpsertNodesDynamic      []model.NodeDynamic
	DeleteNodesDynamic      []string
	UpsertNodeLatency       []model.NodeLatency
	DeleteNodeLatency       []model.NodeLatencyKey
	UpsertLeases            []model.Lease
	DeleteLeases            []model.LeaseKey
}

// FlushTx executes all upserts and deletes in a single transaction.
//
// Upsert order: nodes_static → subscription_nodes → nodes_dynamic → node_latency → leases
// Delete order: leases → node_latency → nodes_dynamic → subscription_nodes → nodes_static
func (r *CacheRepo) FlushTx(ops FlushOps) error {
	tx, err := r.db.Begin()
	if err != nil {
		return fmt.Errorf("begin flush tx: %w", err)
	}
	defer tx.Rollback()

	// Upserts in dependency order.
	steps := []struct {
		name  string
		query string
		n     int
		exec  func(*sql.Stmt, int) error
	}{
		{"upsert_nodes_static", upsertNodesStaticSQL, len(ops.UpsertNodesStatic), func(s *sql.Stmt, i int) error {
			n := ops.UpsertNodesStatic[i]
			_, err := s.Exec(n.Hash, string(n.RawOptions), n.CreatedAtNs)
			return err
		}},
		{"upsert_subscription_nodes", upsertSubscriptionNodesSQL, len(ops.UpsertSubscriptionNodes), func(s *sql.Stmt, i int) error {
			sn := ops.UpsertSubscriptionNodes[i]
			tagsJSON, err := encodeStringSliceJSON(sn.Tags)
			if err != nil {
				return fmt.Errorf("encode subscription node tags: %w", err)
			}
			_, err = s.Exec(sn.SubscriptionID, sn.NodeHash, tagsJSON, sn.Evicted)
			return err
		}},
		{"upsert_nodes_dynamic", upsertNodesDynamicSQL, len(ops.UpsertNodesDynamic), func(s *sql.Stmt, i int) error {
			n := ops.UpsertNodesDynamic[i]
			egressIPsJSON, err := encodeStringSliceJSON(n.EgressIPs)
			if err != nil {
				return fmt.Errorf("encode node dynamic egress_ips: %w", err)
			}
			_, err = s.Exec(
				n.Hash,
				n.FailureCount,
				n.CircuitOpenSince,
				n.EgressIP,
				egressIPsJSON,
				n.EgressRegion,
				n.EgressUpdatedAtNs,
				n.LastLatencyProbeAttemptNs,
				n.NextLatencyProbeDueNs,
				n.LastAuthorityLatencyProbeAttemptNs,
				n.LastEgressUpdateAttemptNs,
			)
			return err
		}},
		{"upsert_node_latency", upsertNodeLatencySQL, len(ops.UpsertNodeLatency), func(s *sql.Stmt, i int) error {
			e := ops.UpsertNodeLatency[i]
			_, err := s.Exec(e.NodeHash, e.Domain, e.EwmaNs, e.LastUpdatedNs)
			return err
		}},
		{"upsert_leases", upsertLeasesSQL, len(ops.UpsertLeases), func(s *sql.Stmt, i int) error {
			l := ops.UpsertLeases[i]
			_, err := s.Exec(l.PlatformID, l.Account, l.NodeHash, l.EgressIP, l.CreatedAtNs, l.ExpiryNs, l.LastAccessedNs)
			return err
		}},
		// Deletes in reverse dependency order.
		{"delete_leases", deleteLeasesSQL, len(ops.DeleteLeases), func(s *sql.Stmt, i int) error {
			_, err := s.Exec(ops.DeleteLeases[i].PlatformID, ops.DeleteLeases[i].Account)
			return err
		}},
		{"delete_node_latency", deleteNodeLatencySQL, len(ops.DeleteNodeLatency), func(s *sql.Stmt, i int) error {
			_, err := s.Exec(ops.DeleteNodeLatency[i].NodeHash, ops.DeleteNodeLatency[i].Domain)
			return err
		}},
		{"delete_nodes_dynamic", deleteNodesDynamicSQL, len(ops.DeleteNodesDynamic), func(s *sql.Stmt, i int) error {
			_, err := s.Exec(ops.DeleteNodesDynamic[i])
			return err
		}},
		{"delete_subscription_nodes", deleteSubscriptionNodesSQL, len(ops.DeleteSubscriptionNodes), func(s *sql.Stmt, i int) error {
			_, err := s.Exec(ops.DeleteSubscriptionNodes[i].SubscriptionID, ops.DeleteSubscriptionNodes[i].NodeHash)
			return err
		}},
		{"delete_nodes_static", deleteNodesStaticSQL, len(ops.DeleteNodesStatic), func(s *sql.Stmt, i int) error {
			_, err := s.Exec(ops.DeleteNodesStatic[i])
			return err
		}},
	}

	for _, step := range steps {
		if err := bulkExecTx(tx, step.query, step.n, step.exec); err != nil {
			return fmt.Errorf("%s: %w", step.name, err)
		}
	}

	return tx.Commit()
}

// SQL constants for FlushTx. Extracted to avoid string duplication.
const (
	upsertNodesStaticSQL = `INSERT INTO nodes_static (hash, raw_options_json, created_at_ns)
		 VALUES (?, ?, ?)
		 ON CONFLICT(hash) DO UPDATE SET
			raw_options_json = excluded.raw_options_json,
			created_at_ns    = excluded.created_at_ns`

	upsertNodesDynamicSQL = `INSERT INTO nodes_dynamic (
			hash, failure_count, circuit_open_since, egress_ip, egress_ips_json, egress_region, egress_updated_at_ns,
			last_latency_probe_attempt_ns, next_latency_probe_due_ns, last_authority_latency_probe_attempt_ns, last_egress_update_attempt_ns
		)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		 ON CONFLICT(hash) DO UPDATE SET
			failure_count                          = excluded.failure_count,
			circuit_open_since                     = excluded.circuit_open_since,
			egress_ip                              = excluded.egress_ip,
			egress_ips_json                        = excluded.egress_ips_json,
			egress_region                          = excluded.egress_region,
			egress_updated_at_ns                   = excluded.egress_updated_at_ns,
			last_latency_probe_attempt_ns          = excluded.last_latency_probe_attempt_ns,
			next_latency_probe_due_ns              = excluded.next_latency_probe_due_ns,
			last_authority_latency_probe_attempt_ns = excluded.last_authority_latency_probe_attempt_ns,
			last_egress_update_attempt_ns          = excluded.last_egress_update_attempt_ns`

	upsertNodeLatencySQL = `INSERT INTO node_latency (node_hash, domain, ewma_ns, last_updated_ns)
		 VALUES (?, ?, ?, ?)
		 ON CONFLICT(node_hash, domain) DO UPDATE SET
			ewma_ns         = excluded.ewma_ns,
			last_updated_ns = excluded.last_updated_ns`

	upsertLeasesSQL = `INSERT INTO leases (platform_id, account, node_hash, egress_ip, created_at_ns, expiry_ns, last_accessed_ns)
		 VALUES (?, ?, ?, ?, ?, ?, ?)
		 ON CONFLICT(platform_id, account) DO UPDATE SET
			node_hash       = excluded.node_hash,
			egress_ip       = excluded.egress_ip,
			created_at_ns   = excluded.created_at_ns,
			expiry_ns       = excluded.expiry_ns,
			last_accessed_ns = excluded.last_accessed_ns`

	upsertSubscriptionNodesSQL = `INSERT INTO subscription_nodes (subscription_id, node_hash, tags_json, evicted)
		 VALUES (?, ?, ?, ?)
		 ON CONFLICT(subscription_id, node_hash) DO UPDATE SET
			tags_json = excluded.tags_json,
			evicted = excluded.evicted`

	deleteNodesStaticSQL       = "DELETE FROM nodes_static WHERE hash = ?"
	deleteNodesDynamicSQL      = "DELETE FROM nodes_dynamic WHERE hash = ?"
	deleteNodeLatencySQL       = "DELETE FROM node_latency WHERE node_hash = ? AND domain = ?"
	deleteLeasesSQL            = "DELETE FROM leases WHERE platform_id = ? AND account = ?"
	deleteSubscriptionNodesSQL = "DELETE FROM subscription_nodes WHERE subscription_id = ? AND node_hash = ?"
)
