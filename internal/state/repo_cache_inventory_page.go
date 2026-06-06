package state

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/Resinat/Resin/internal/model"
)

func (e *StateEngine) ListNodeInventoryPage(opts NodeInventoryListOptions) (NodeInventoryPage, error) {
	if e == nil || e.CacheRepo == nil {
		return NodeInventoryPage{Items: []model.NodeInventory{}}, nil
	}
	if len(opts.SubscriptionMeta) == 0 && e.StateRepo != nil {
		subs, err := e.ListSubscriptions()
		if err != nil {
			return NodeInventoryPage{}, err
		}
		opts.SubscriptionMeta = make([]NodeInventorySubscriptionMeta, 0, len(subs))
		for _, sub := range subs {
			opts.SubscriptionMeta = append(opts.SubscriptionMeta, NodeInventorySubscriptionMeta{
				ID:          sub.ID,
				Name:        sub.Name,
				Enabled:     sub.Enabled,
				CreatedAtNs: sub.CreatedAtNs,
			})
		}
	}
	return e.CacheRepo.ListNodeInventoryPage(opts)
}

func (r *CacheRepo) ListNodeInventoryPage(opts NodeInventoryListOptions) (NodeInventoryPage, error) {
	if r == nil || r.db == nil {
		return NodeInventoryPage{Items: []model.NodeInventory{}}, nil
	}
	if opts.Limit <= 0 {
		opts.Limit = 50
	}
	if opts.Offset < 0 {
		opts.Offset = 0
	}

	tx, err := r.db.Begin()
	if err != nil {
		return NodeInventoryPage{}, fmt.Errorf("begin inventory page tx: %w", err)
	}
	defer tx.Rollback()

	if err := setupNodeInventoryTempTables(tx, opts); err != nil {
		return NodeInventoryPage{}, err
	}

	baseCTE, baseArgs, err := nodeInventoryBaseCTE(opts)
	if err != nil {
		return NodeInventoryPage{}, err
	}

	page := NodeInventoryPage{Items: []model.NodeInventory{}}
	uniqueExpr := `COUNT(DISTINCT CASE WHEN COALESCE(nd.egress_ip, '') <> '' THEN nd.egress_ip END)`
	if len(compactUniqueStrings(opts.ExcludedEgressIPs)) > 0 {
		uniqueExpr = `COUNT(DISTINCT CASE WHEN COALESCE(nd.egress_ip, '') <> '' AND NOT EXISTS (SELECT 1 FROM temp_node_inventory_excluded_egress_ip AS ei WHERE ei.egress_ip = nd.egress_ip) THEN nd.egress_ip END)`
	}
	countSQL := baseCTE + `
SELECT COUNT(*), ` + uniqueExpr + `
FROM filtered_nodes AS fn
LEFT JOIN nodes_dynamic AS nd ON nd.hash = fn.hash`
	if err := tx.QueryRow(countSQL, baseArgs...).Scan(&page.Total, &page.UniqueEgressIPs); err != nil {
		return NodeInventoryPage{}, fmt.Errorf("count node inventory page: %w", err)
	}

	orderClause := nodeInventoryOrderClause(opts.SortBy, opts.SortOrder)
	displayRelationWhere, displayRelationArgs := nodeInventoryRelationWhere(opts)
	pageSQL := baseCTE + `,
ranked_nodes AS (
	SELECT ns.hash,
	       ns.raw_options_json,
	       ns.created_at_ns,
	       CASE WHEN nd.hash IS NULL THEN 0 ELSE 1 END AS has_dynamic,
	       COALESCE(nd.failure_count, 0) AS failure_count,
	       COALESCE(nd.circuit_open_since, 0) AS circuit_open_since,
	       COALESCE(nd.egress_ip, '') AS egress_ip,
	       COALESCE(nd.egress_ips_json, '[]') AS egress_ips_json,
	       COALESCE(nd.egress_region, '') AS egress_region,
	       COALESCE(nd.egress_updated_at_ns, 0) AS egress_updated_at_ns,
	       COALESCE(nd.last_latency_probe_attempt_ns, 0) AS last_latency_probe_attempt_ns,
	       COALESCE(nd.next_latency_probe_due_ns, 0) AS next_latency_probe_due_ns,
	       COALESCE(nd.last_authority_latency_probe_attempt_ns, 0) AS last_authority_latency_probe_attempt_ns,
	       COALESCE(nd.last_egress_update_attempt_ns, 0) AS last_egress_update_attempt_ns,
	       COALESCE(
	         (
	           SELECT COALESCE(sm.name, sn.subscription_id) || '/' || COALESCE((SELECT CAST(value AS TEXT) FROM json_each(sn.tags_json) ORDER BY CAST(value AS TEXT) ASC LIMIT 1), '')
	           FROM subscription_nodes AS sn
	           LEFT JOIN temp_node_inventory_sub_meta AS sm ON sm.id = sn.subscription_id
	           WHERE sn.node_hash = ns.hash
	             AND ` + displayRelationWhere + `
	             AND COALESCE(sm.enabled, 0) = 1
	             AND json_array_length(sn.tags_json) > 0
	           ORDER BY COALESCE(sm.created_at_ns, 0) ASC, sn.subscription_id ASC
	           LIMIT 1
	         ),
	         (
	           SELECT COALESCE(sm.name, sn.subscription_id) || '/' || COALESCE((SELECT CAST(value AS TEXT) FROM json_each(sn.tags_json) ORDER BY CAST(value AS TEXT) ASC LIMIT 1), '')
	           FROM subscription_nodes AS sn
	           LEFT JOIN temp_node_inventory_sub_meta AS sm ON sm.id = sn.subscription_id
	           WHERE sn.node_hash = ns.hash
	             AND ` + displayRelationWhere + `
	             AND json_array_length(sn.tags_json) > 0
	           ORDER BY COALESCE(sm.created_at_ns, 0) ASC, sn.subscription_id ASC
	           LIMIT 1
	         ),
	         ''
	       ) AS display_tag
	FROM filtered_nodes AS fn
	JOIN nodes_static AS ns ON ns.hash = fn.hash
	LEFT JOIN nodes_dynamic AS nd ON nd.hash = fn.hash
)
SELECT hash, raw_options_json, created_at_ns, has_dynamic,
       failure_count, circuit_open_since, egress_ip, egress_ips_json, egress_region,
       egress_updated_at_ns, last_latency_probe_attempt_ns, next_latency_probe_due_ns,
       last_authority_latency_probe_attempt_ns, last_egress_update_attempt_ns
FROM ranked_nodes
ORDER BY ` + orderClause + `
LIMIT ? OFFSET ?`
	pageArgs := append([]any{}, baseArgs...)
	pageArgs = append(pageArgs, displayRelationArgs...)
	pageArgs = append(pageArgs, displayRelationArgs...)
	pageArgs = append(pageArgs, opts.Limit, opts.Offset)
	rows, err := tx.Query(pageSQL, pageArgs...)
	if err != nil {
		return NodeInventoryPage{}, fmt.Errorf("query node inventory page: %w", err)
	}
	for rows.Next() {
		item, err := scanNodeInventoryPageRow(rows)
		if err != nil {
			_ = rows.Close()
			return NodeInventoryPage{}, err
		}
		page.Items = append(page.Items, item)
	}
	if err := rows.Close(); err != nil {
		return NodeInventoryPage{}, fmt.Errorf("close node inventory page rows: %w", err)
	}
	if err := rows.Err(); err != nil {
		return NodeInventoryPage{}, fmt.Errorf("scan node inventory page rows: %w", err)
	}
	if len(page.Items) == 0 {
		return page, nil
	}
	if err := loadNodeInventoryPageRelations(tx, opts, page.Items); err != nil {
		return NodeInventoryPage{}, err
	}
	return page, nil
}

func setupNodeInventoryTempTables(tx *sql.Tx, opts NodeInventoryListOptions) error {
	stmts := []string{
		`CREATE TEMP TABLE IF NOT EXISTS temp_node_inventory_sub_meta (id TEXT PRIMARY KEY, name TEXT NOT NULL, enabled INTEGER NOT NULL, created_at_ns INTEGER NOT NULL)`,
		`DELETE FROM temp_node_inventory_sub_meta`,
		`CREATE TEMP TABLE IF NOT EXISTS temp_node_inventory_hash_filter (hash TEXT PRIMARY KEY)`,
		`DELETE FROM temp_node_inventory_hash_filter`,
		`CREATE TEMP TABLE IF NOT EXISTS temp_node_inventory_excluded_hash (hash TEXT PRIMARY KEY)`,
		`DELETE FROM temp_node_inventory_excluded_hash`,
		`CREATE TEMP TABLE IF NOT EXISTS temp_node_inventory_excluded_egress_ip (egress_ip TEXT PRIMARY KEY)`,
		`DELETE FROM temp_node_inventory_excluded_egress_ip`,
		`CREATE TEMP TABLE IF NOT EXISTS temp_node_inventory_page_hash (hash TEXT PRIMARY KEY)`,
		`DELETE FROM temp_node_inventory_page_hash`,
	}
	for _, stmt := range stmts {
		if _, err := tx.Exec(stmt); err != nil {
			return fmt.Errorf("prepare node inventory temp table: %w", err)
		}
	}
	if err := bulkExecTx(tx, `INSERT OR REPLACE INTO temp_node_inventory_sub_meta (id, name, enabled, created_at_ns) VALUES (?, ?, ?, ?)`, len(opts.SubscriptionMeta), func(stmt *sql.Stmt, i int) error {
		m := opts.SubscriptionMeta[i]
		name := m.Name
		if name == "" {
			name = m.ID
		}
		_, err := stmt.Exec(m.ID, name, m.Enabled, m.CreatedAtNs)
		return err
	}); err != nil {
		return fmt.Errorf("seed node inventory subscription metadata: %w", err)
	}
	if err := replaceTempStringSet(tx, "temp_node_inventory_hash_filter", "hash", opts.HashFilter); err != nil {
		return err
	}
	if err := replaceTempStringSet(tx, "temp_node_inventory_excluded_hash", "hash", opts.ExcludedHashes); err != nil {
		return err
	}
	if err := replaceTempStringSet(tx, "temp_node_inventory_excluded_egress_ip", "egress_ip", opts.ExcludedEgressIPs); err != nil {
		return err
	}
	return nil
}

func replaceTempStringSet(tx *sql.Tx, table, column string, values []string) error {
	values = compactUniqueStrings(values)
	if len(values) == 0 {
		return nil
	}
	query := fmt.Sprintf("INSERT OR IGNORE INTO %s (%s) VALUES (?)", table, column)
	if err := bulkExecTx(tx, query, len(values), func(stmt *sql.Stmt, i int) error {
		_, err := stmt.Exec(values[i])
		return err
	}); err != nil {
		return fmt.Errorf("seed %s: %w", table, err)
	}
	return nil
}

func nodeInventoryRelationWhere(opts NodeInventoryListOptions) (string, []any) {
	parts := []string{"sn.evicted = 0"}
	args := []any{}
	if opts.SubscriptionID != nil && *opts.SubscriptionID != "" {
		parts = append(parts, "sn.subscription_id = ?")
		args = append(args, *opts.SubscriptionID)
	}
	return strings.Join(parts, " AND "), args
}

func nodeInventoryBaseCTE(opts NodeInventoryListOptions) (string, []any, error) {
	relationWhere, relationArgs := nodeInventoryRelationWhere(opts)
	args := []any{}
	appendRelationArgs := func() {
		args = append(args, relationArgs...)
	}

	nodeWhere := []string{"EXISTS (SELECT 1 FROM subscription_nodes AS sn WHERE sn.node_hash = ns.hash AND " + relationWhere + ")"}
	appendRelationArgs()
	if len(compactUniqueStrings(opts.HashFilter)) > 0 {
		nodeWhere = append(nodeWhere, "EXISTS (SELECT 1 FROM temp_node_inventory_hash_filter AS hf WHERE hf.hash = ns.hash)")
	}
	if len(compactUniqueStrings(opts.ExcludedHashes)) > 0 {
		nodeWhere = append(nodeWhere, "NOT EXISTS (SELECT 1 FROM temp_node_inventory_excluded_hash AS eh WHERE eh.hash = ns.hash)")
	}
	if opts.Enabled != nil {
		enabledExists := "EXISTS (SELECT 1 FROM subscription_nodes AS sn LEFT JOIN temp_node_inventory_sub_meta AS sm ON sm.id = sn.subscription_id WHERE sn.node_hash = ns.hash AND " + relationWhere + " AND COALESCE(sm.enabled, 0) = 1)"
		if *opts.Enabled {
			nodeWhere = append(nodeWhere, enabledExists)
		} else {
			nodeWhere = append(nodeWhere, "NOT "+enabledExists)
		}
		appendRelationArgs()
	}
	if opts.TagKeyword != nil {
		keyword := strings.ToLower(strings.TrimSpace(*opts.TagKeyword))
		if keyword != "" {
			nodeWhere = append(nodeWhere, `EXISTS (
				SELECT 1
				FROM subscription_nodes AS sn
				LEFT JOIN temp_node_inventory_sub_meta AS sm ON sm.id = sn.subscription_id,
				     json_each(sn.tags_json) AS tag
				WHERE sn.node_hash = ns.hash
				  AND `+relationWhere+`
				  AND lower(COALESCE(sm.name, sn.subscription_id) || '/' || CAST(tag.value AS TEXT)) LIKE ?
			)`)
			appendRelationArgs()
			args = append(args, "%"+keyword+"%")
		}
	}
	if opts.Region != nil && strings.TrimSpace(*opts.Region) != "" {
		nodeWhere = append(nodeWhere, "lower(COALESCE(nd.egress_region, '')) = lower(?)")
		args = append(args, strings.TrimSpace(*opts.Region))
	}
	if opts.CircuitOpen != nil {
		if *opts.CircuitOpen {
			nodeWhere = append(nodeWhere, "COALESCE(nd.circuit_open_since, 0) <> 0")
		} else {
			nodeWhere = append(nodeWhere, "COALESCE(nd.circuit_open_since, 0) = 0")
		}
	}
	if opts.HasOutbound != nil && *opts.HasOutbound {
		// Persisted inventory rows do not carry a live outbound object; active
		// runtime rows are merged by the service layer.
		nodeWhere = append(nodeWhere, "0 = 1")
	}
	if opts.EgressIP != nil {
		nodeWhere = append(nodeWhere, "COALESCE(nd.egress_ip, '') = ?")
		args = append(args, *opts.EgressIP)
	}
	if opts.ProbedSinceNs != nil {
		nodeWhere = append(nodeWhere, "COALESCE(nd.last_latency_probe_attempt_ns, 0) >= ?")
		args = append(args, *opts.ProbedSinceNs)
	}

	return `WITH filtered_nodes AS (
	SELECT ns.hash
	FROM nodes_static AS ns
	LEFT JOIN nodes_dynamic AS nd ON nd.hash = ns.hash
	WHERE ` + strings.Join(nodeWhere, "\n	  AND ") + `
)`, args, nil
}

func nodeInventoryOrderClause(sortBy, sortOrder string) string {
	dir := "ASC"
	if strings.EqualFold(sortOrder, "desc") {
		dir = "DESC"
	}
	primary := "display_tag"
	switch sortBy {
	case "created_at":
		primary = "created_at_ns"
	case "failure_count":
		primary = "failure_count"
	case "region":
		primary = "egress_region"
	}
	return primary + " " + dir + ", hash " + dir
}

func scanNodeInventoryPageRow(rows *sql.Rows) (model.NodeInventory, error) {
	var item model.NodeInventory
	var rawOptionsJSON, egressIPsJSON string
	var hasDynamic int
	var dyn model.NodeDynamic
	if err := rows.Scan(
		&item.Static.Hash,
		&rawOptionsJSON,
		&item.Static.CreatedAtNs,
		&hasDynamic,
		&dyn.FailureCount,
		&dyn.CircuitOpenSince,
		&dyn.EgressIP,
		&egressIPsJSON,
		&dyn.EgressRegion,
		&dyn.EgressUpdatedAtNs,
		&dyn.LastLatencyProbeAttemptNs,
		&dyn.NextLatencyProbeDueNs,
		&dyn.LastAuthorityLatencyProbeAttemptNs,
		&dyn.LastEgressUpdateAttemptNs,
	); err != nil {
		return item, err
	}
	item.Static.RawOptions = json.RawMessage(rawOptionsJSON)
	if hasDynamic != 0 {
		dyn.Hash = item.Static.Hash
		egressIPs, err := decodeStringSliceJSON(egressIPsJSON)
		if err != nil {
			return item, fmt.Errorf("decode node dynamic egress_ips for %s: %w", item.Static.Hash, err)
		}
		dyn.EgressIPs = egressIPs
		item.Dynamic = &dyn
	}
	item.Relations = []model.SubscriptionNode{}
	return item, nil
}

func loadNodeInventoryPageRelations(tx *sql.Tx, opts NodeInventoryListOptions, items []model.NodeInventory) error {
	if len(items) == 0 {
		return nil
	}
	hashes := make([]string, 0, len(items))
	byHash := make(map[string]*model.NodeInventory, len(items))
	for i := range items {
		hashes = append(hashes, items[i].Static.Hash)
		byHash[items[i].Static.Hash] = &items[i]
	}
	if err := replaceTempStringSet(tx, "temp_node_inventory_page_hash", "hash", hashes); err != nil {
		return err
	}
	relationWhere, args := nodeInventoryRelationWhere(opts)
	rows, err := tx.Query(`WITH relation_meta AS (
	SELECT sn.node_hash,
	       sn.subscription_id,
	       sn.tags_json,
	       COALESCE(sm.created_at_ns, 0) AS created_at_ns
	FROM subscription_nodes AS sn
	LEFT JOIN temp_node_inventory_sub_meta AS sm ON sm.id = sn.subscription_id
	WHERE `+relationWhere+`
)
SELECT rm.node_hash, rm.subscription_id, rm.tags_json
FROM relation_meta AS rm
JOIN temp_node_inventory_page_hash AS ph ON ph.hash = rm.node_hash
ORDER BY rm.node_hash ASC, rm.created_at_ns ASC, rm.subscription_id ASC`, args...)
	if err != nil {
		return fmt.Errorf("query node inventory page relations: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var hash, subID, tagsJSON string
		if err := rows.Scan(&hash, &subID, &tagsJSON); err != nil {
			return err
		}
		tags, err := decodeStringSliceJSON(tagsJSON)
		if err != nil {
			return fmt.Errorf("decode subscription node tags_json for %s/%s: %w", subID, hash, err)
		}
		item := byHash[hash]
		if item == nil {
			continue
		}
		item.Relations = append(item.Relations, model.SubscriptionNode{
			SubscriptionID: subID,
			NodeHash:       hash,
			Tags:           tags,
		})
	}
	return rows.Err()
}
