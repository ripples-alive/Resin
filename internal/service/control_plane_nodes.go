package service

import (
	"sort"
	"strings"
	"time"

	"github.com/Resinat/Resin/internal/model"
	"github.com/Resinat/Resin/internal/node"
	"github.com/Resinat/Resin/internal/probe"
	"github.com/Resinat/Resin/internal/state"
	"github.com/Resinat/Resin/internal/subscription"
)

// ------------------------------------------------------------------
// Nodes
// ------------------------------------------------------------------

// NodeFilters holds query filters for listing nodes.
type NodeFilters struct {
	// Active controls the data source: nil/true lists the hot runtime pool, while
	// false lists the persisted DB inventory. It is intentionally separate from
	// Enabled, which filters subscription enable/disable state.
	Active         *bool
	PlatformID     *string
	SubscriptionID *string
	Enabled        *bool
	Region         *string
	CircuitOpen    *bool
	HasOutbound    *bool
	EgressIP       *string
	ProbedSince    *time.Time
	TagKeyword     *string
}

// ListNodes returns nodes with optional filters. By default it uses the hot
// runtime pool; callers must set Active=false to inspect DB inventory rows.
func (s *ControlPlaneService) ListNodes(filters NodeFilters) ([]NodeSummary, error) {
	if filters.Active != nil && !*filters.Active {
		return s.listInventoryNodes(filters)
	}
	return s.listRuntimeNodes(filters)
}

func (s *ControlPlaneService) listRuntimeNodes(filters NodeFilters) ([]NodeSummary, error) {
	var subLookup node.SubLookupFunc
	if s != nil && s.Pool != nil {
		subLookup = s.Pool.MakeSubLookup()
	}

	// If platform_id filter, get the platform view.
	var platformView map[node.Hash]struct{}
	if filters.PlatformID != nil {
		plat, ok := s.Pool.GetPlatform(*filters.PlatformID)
		if !ok {
			return nil, notFound("platform not found")
		}
		platformView = make(map[node.Hash]struct{}, plat.View().Size())
		plat.View().Range(func(h node.Hash) bool {
			platformView[h] = struct{}{}
			return true
		})
	}

	var subNodes map[node.Hash]struct{}
	if filters.SubscriptionID != nil {
		sub := s.SubMgr.Lookup(*filters.SubscriptionID)
		if sub == nil {
			return nil, notFound("subscription not found")
		}
		subNodes = make(map[node.Hash]struct{})
		sub.ManagedNodes().RangeNodes(func(h node.Hash, managed subscription.ManagedNode) bool {
			if managed.Evicted {
				return true
			}
			subNodes[h] = struct{}{}
			return true
		})
	}

	var result []NodeSummary
	appendIfMatched := func(h node.Hash, entry *node.NodeEntry) {
		if !s.nodeEntryMatchesFilters(entry, filters, subLookup) {
			return
		}
		result = append(result, s.nodeEntryToSummary(h, entry))
	}

	appendIfMatchedHash := func(h node.Hash) {
		entry, ok := s.Pool.GetEntry(h)
		if !ok {
			return
		}
		appendIfMatched(h, entry)
	}

	switch {
	case platformView != nil && subNodes != nil:
		// Iterate the smaller candidate set, then intersect by membership.
		if len(platformView) <= len(subNodes) {
			for h := range platformView {
				if _, ok := subNodes[h]; !ok {
					continue
				}
				appendIfMatchedHash(h)
			}
		} else {
			for h := range subNodes {
				if _, ok := platformView[h]; !ok {
					continue
				}
				appendIfMatchedHash(h)
			}
		}
	case platformView != nil:
		for h := range platformView {
			appendIfMatchedHash(h)
		}
	case subNodes != nil:
		for h := range subNodes {
			appendIfMatchedHash(h)
		}
	default:
		s.Pool.Range(func(h node.Hash, entry *node.NodeEntry) bool {
			appendIfMatched(h, entry)
			return true
		})
	}

	if result == nil {
		result = []NodeSummary{}
	}
	return result, nil
}

func (s *ControlPlaneService) listInventoryNodes(filters NodeFilters) ([]NodeSummary, error) {
	if s == nil || s.Engine == nil {
		return s.listRuntimeNodes(filters)
	}
	page, err := s.ListNodesPage(filters, NodeListPageOptions{})
	if err != nil {
		return nil, err
	}
	return page.Items, nil
}

// NodeListPageOptions carries API pagination/sorting into the service layer so
// broad DB inventory lists can page before summary construction.
type NodeListPageOptions struct {
	SortBy    string
	SortOrder string
	Limit     int
	Offset    int
}

// NodeListPage is a paginated node response with whole-scope aggregates.
type NodeListPage struct {
	Items                  []NodeSummary
	Total                  int
	Limit                  int
	Offset                 int
	UniqueEgressIPs        int
	UniqueHealthyEgressIPs int
}

// ListNodesPage returns a paginated node list. Active=false is served from a
// bounded DB query plus runtime overlay; nil/true active keeps existing runtime
// semantics and paginates after the small in-memory result set is built.
func (s *ControlPlaneService) ListNodesPage(filters NodeFilters, opts NodeListPageOptions) (NodeListPage, error) {
	if opts.Limit <= 0 {
		opts.Limit = lenAllNodePage
	}
	if opts.Offset < 0 {
		opts.Offset = 0
	}
	if opts.SortBy == "" {
		opts.SortBy = "tag"
	}
	if opts.SortOrder == "" {
		opts.SortOrder = "asc"
	}

	if filters.Active != nil && !*filters.Active {
		return s.listInventoryNodesPage(filters, opts)
	}

	nodes, err := s.listRuntimeNodes(filters)
	if err != nil {
		return NodeListPage{}, err
	}
	sortNodeSummariesForService(nodes, opts.SortBy, opts.SortOrder)
	return NodeListPage{
		Items:                  paginateNodeSummaries(nodes, opts.Limit, opts.Offset),
		Total:                  len(nodes),
		Limit:                  opts.Limit,
		Offset:                 opts.Offset,
		UniqueEgressIPs:        countNodeSummaryUniqueEgressIPs(nodes, nil),
		UniqueHealthyEgressIPs: countNodeSummaryUniqueHealthyEgressIPs(nodes),
	}, nil
}

const lenAllNodePage = 1 << 30

func (s *ControlPlaneService) listInventoryNodesPage(filters NodeFilters, opts NodeListPageOptions) (NodeListPage, error) {
	if s == nil || s.Engine == nil {
		nodes, err := s.listRuntimeNodes(filters)
		if err != nil {
			return NodeListPage{}, err
		}
		sortNodeSummariesForService(nodes, opts.SortBy, opts.SortOrder)
		return NodeListPage{
			Items:                  paginateNodeSummaries(nodes, opts.Limit, opts.Offset),
			Total:                  len(nodes),
			Limit:                  opts.Limit,
			Offset:                 opts.Offset,
			UniqueEgressIPs:        countNodeSummaryUniqueEgressIPs(nodes, nil),
			UniqueHealthyEgressIPs: countNodeSummaryUniqueHealthyEgressIPs(nodes),
		}, nil
	}

	platformHashes, err := s.inventoryPlatformHashFilter(filters)
	if err != nil {
		return NodeListPage{}, err
	}
	meta, err := s.inventorySubscriptionMetadataForState()
	if err != nil {
		return NodeListPage{}, internal("load subscription metadata", err)
	}
	if filters.SubscriptionID != nil && !inventorySubscriptionExists(meta, *filters.SubscriptionID) {
		return NodeListPage{}, notFound("subscription not found")
	}

	runtimeNodes, err := s.listInventoryRuntimeOverlayNodes(filters)
	if err != nil {
		return NodeListPage{}, err
	}
	excludedRuntimeNodes := runtimeNodes
	if filters.HasOutbound != nil && !*filters.HasOutbound {
		// Runtime state wins over inventory for overlapping hashes. When the
		// caller asks for rows without outbound, a hot runtime node that has an
		// outbound must suppress its persisted DB inventory row instead of being
		// reintroduced as a cold has_outbound=false summary.
		exclusionFilters := filters
		exclusionFilters.HasOutbound = nil
		excludedRuntimeNodes, err = s.listInventoryRuntimeOverlayNodes(exclusionFilters)
		if err != nil {
			return NodeListPage{}, err
		}
	}
	runtimeByHash := make(map[string]NodeSummary, len(runtimeNodes))
	runtimeHashes := make([]string, 0, len(excludedRuntimeNodes))
	runtimeEgressIPs := make([]string, 0, len(runtimeNodes))
	for _, summary := range runtimeNodes {
		runtimeByHash[summary.NodeHash] = summary
		if summary.EgressIP != "" {
			runtimeEgressIPs = append(runtimeEgressIPs, summary.EgressIP)
		}
	}
	for _, summary := range excludedRuntimeNodes {
		runtimeHashes = append(runtimeHashes, summary.NodeHash)
	}

	// Fetch enough DB rows to merge/sort with the (small) runtime overlay before
	// applying the requested page. This preserves old full-list ordering semantics
	// without materializing the entire DB inventory for common limit=50 pages.
	dbLimit := opts.Offset + opts.Limit
	if dbLimit < opts.Limit {
		dbLimit = opts.Limit
	}
	page, err := s.Engine.ListNodeInventoryPage(state.NodeInventoryListOptions{
		SubscriptionMeta:  meta,
		SubscriptionID:    filters.SubscriptionID,
		HashFilter:        platformHashes,
		ExcludedHashes:    runtimeHashes,
		ExcludedEgressIPs: runtimeEgressIPs,
		Enabled:           filters.Enabled,
		Region:            filters.Region,
		CircuitOpen:       filters.CircuitOpen,
		HasOutbound:       filters.HasOutbound,
		EgressIP:          filters.EgressIP,
		ProbedSinceNs:     probedSinceNs(filters.ProbedSince),
		TagKeyword:        filters.TagKeyword,
		SortBy:            opts.SortBy,
		SortOrder:         opts.SortOrder,
		Limit:             dbLimit,
		Offset:            0,
	})
	if err != nil {
		return NodeListPage{}, internal("list node inventory page", err)
	}

	items := make([]NodeSummary, 0, len(runtimeNodes)+len(page.Items))
	items = append(items, runtimeNodes...)
	for _, item := range page.Items {
		items = append(items, s.inventoryNodeToSummary(item.Static, item.Dynamic, item.Relations, metaMapFromState(meta)))
	}
	sortNodeSummariesForService(items, opts.SortBy, opts.SortOrder)
	items = paginateNodeSummaries(items, opts.Limit, opts.Offset)

	return NodeListPage{
		Items:                  items,
		Total:                  page.Total + len(runtimeByHash),
		Limit:                  opts.Limit,
		Offset:                 opts.Offset,
		UniqueEgressIPs:        page.UniqueEgressIPs + countNodeSummaryUniqueEgressIPs(runtimeNodes, nil),
		UniqueHealthyEgressIPs: countNodeSummaryUniqueHealthyEgressIPs(runtimeNodes),
	}, nil
}

func (s *ControlPlaneService) listInventoryRuntimeOverlayNodes(filters NodeFilters) ([]NodeSummary, error) {
	if filters.SubscriptionID != nil {
		if s == nil || s.SubMgr == nil || s.SubMgr.Lookup(*filters.SubscriptionID) == nil {
			return []NodeSummary{}, nil
		}
	}
	return s.listRuntimeNodes(filters)
}

func inventorySubscriptionExists(meta []state.NodeInventorySubscriptionMeta, id string) bool {
	id = strings.TrimSpace(id)
	if id == "" {
		return false
	}
	for _, m := range meta {
		if m.ID == id {
			return true
		}
	}
	return false
}

func (s *ControlPlaneService) inventoryPlatformHashFilter(filters NodeFilters) ([]string, error) {
	if filters.PlatformID == nil {
		return nil, nil
	}
	if s == nil || s.Pool == nil {
		return nil, notFound("platform not found")
	}
	plat, ok := s.Pool.GetPlatform(*filters.PlatformID)
	if !ok {
		return nil, notFound("platform not found")
	}
	hashes := make([]string, 0, plat.View().Size())
	plat.View().Range(func(h node.Hash) bool {
		hashes = append(hashes, h.Hex())
		return true
	})
	return hashes, nil
}

func probedSinceNs(t *time.Time) *int64 {
	if t == nil {
		return nil
	}
	ns := t.UnixNano()
	return &ns
}

func (s *ControlPlaneService) inventorySubscriptionMetadataForState() ([]state.NodeInventorySubscriptionMeta, error) {
	meta := make(map[string]state.NodeInventorySubscriptionMeta)
	if s != nil && s.Engine != nil {
		subs, err := s.Engine.ListSubscriptions()
		if err != nil {
			return nil, err
		}
		for _, sub := range subs {
			meta[sub.ID] = state.NodeInventorySubscriptionMeta{ID: sub.ID, Name: sub.Name, Enabled: sub.Enabled, CreatedAtNs: sub.CreatedAtNs}
		}
	}
	if s != nil && s.SubMgr != nil {
		s.SubMgr.Range(func(id string, sub *subscription.Subscription) bool {
			if sub == nil {
				return true
			}
			meta[id] = state.NodeInventorySubscriptionMeta{ID: id, Name: sub.Name(), Enabled: sub.Enabled(), CreatedAtNs: sub.CreatedAtNs}
			return true
		})
	}
	out := make([]state.NodeInventorySubscriptionMeta, 0, len(meta))
	for _, m := range meta {
		out = append(out, m)
	}
	return out, nil
}

func metaMapFromState(values []state.NodeInventorySubscriptionMeta) map[string]inventorySubscriptionMeta {
	out := make(map[string]inventorySubscriptionMeta, len(values))
	for _, v := range values {
		out[v.ID] = inventorySubscriptionMeta{Name: v.Name, Enabled: v.Enabled, CreatedAtNs: v.CreatedAtNs}
	}
	return out
}

type inventorySubscriptionMeta struct {
	Name        string
	Enabled     bool
	CreatedAtNs int64
}

func inventoryNodeEnabled(rels []model.SubscriptionNode, meta map[string]inventorySubscriptionMeta) bool {
	for _, rel := range rels {
		if m, ok := meta[rel.SubscriptionID]; ok && m.Enabled {
			return true
		}
	}
	return false
}

func inventoryNodeDisplayTag(rels []model.SubscriptionNode, meta map[string]inventorySubscriptionMeta) string {
	pick := func(enabledOnly bool) (string, bool) {
		found := false
		bestCreated := int64(0)
		bestSubID := ""
		bestName := ""
		bestTag := ""
		for _, rel := range rels {
			m, ok := meta[rel.SubscriptionID]
			if !ok {
				m = inventorySubscriptionMeta{Name: rel.SubscriptionID}
			}
			if enabledOnly && !m.Enabled {
				continue
			}
			if len(rel.Tags) == 0 {
				continue
			}
			tags := append([]string(nil), rel.Tags...)
			sort.Strings(tags)
			if !found || m.CreatedAtNs < bestCreated || (m.CreatedAtNs == bestCreated && rel.SubscriptionID < bestSubID) {
				found = true
				bestCreated = m.CreatedAtNs
				bestSubID = rel.SubscriptionID
				bestName = m.Name
				bestTag = tags[0]
			}
		}
		if !found || bestName == "" || bestTag == "" {
			return "", false
		}
		return bestName + "/" + bestTag, true
	}
	if tag, ok := pick(true); ok {
		return tag
	}
	if tag, ok := pick(false); ok {
		return tag
	}
	return ""
}

func (s *ControlPlaneService) inventoryNodeToSummary(
	st model.NodeStatic,
	dyn *model.NodeDynamic,
	rels []model.SubscriptionNode,
	meta map[string]inventorySubscriptionMeta,
) NodeSummary {
	ns := NodeSummary{
		NodeHash:         st.Hash,
		CreatedAt:        time.Unix(0, st.CreatedAtNs).UTC().Format(time.RFC3339Nano),
		Enabled:          inventoryNodeEnabled(rels, meta),
		HasOutbound:      false,
		CircuitOpenSince: nil,
		Tags:             []NodeTag{},
	}
	if dyn != nil {
		ns.FailureCount = dyn.FailureCount
		if dyn.CircuitOpenSince > 0 {
			t := time.Unix(0, dyn.CircuitOpenSince).UTC().Format(time.RFC3339Nano)
			ns.CircuitOpenSince = &t
		}
		ns.EgressIP = dyn.EgressIP
		ns.EgressIPs = append([]string(nil), dyn.EgressIPs...)
		ns.Region = dyn.EgressRegion
		if dyn.EgressUpdatedAtNs > 0 {
			ns.LastEgressUpdate = time.Unix(0, dyn.EgressUpdatedAtNs).UTC().Format(time.RFC3339Nano)
		}
		if dyn.LastLatencyProbeAttemptNs > 0 {
			ns.LastLatencyProbeAttempt = time.Unix(0, dyn.LastLatencyProbeAttemptNs).UTC().Format(time.RFC3339Nano)
		}
		if dyn.LastAuthorityLatencyProbeAttemptNs > 0 {
			ns.LastAuthorityLatencyProbeAttempt = time.Unix(0, dyn.LastAuthorityLatencyProbeAttemptNs).UTC().Format(time.RFC3339Nano)
		}
		if dyn.LastEgressUpdateAttemptNs > 0 {
			ns.LastEgressUpdateAttempt = time.Unix(0, dyn.LastEgressUpdateAttemptNs).UTC().Format(time.RFC3339Nano)
		}
	}
	ns.DisplayTag = inventoryNodeDisplayTag(rels, meta)
	for _, rel := range rels {
		m, ok := meta[rel.SubscriptionID]
		name := rel.SubscriptionID
		createdAtNs := int64(0)
		if ok {
			if m.Name != "" {
				name = m.Name
			}
			createdAtNs = m.CreatedAtNs
		}
		for _, tag := range rel.Tags {
			ns.Tags = append(ns.Tags, NodeTag{
				SubscriptionID:          rel.SubscriptionID,
				SubscriptionName:        name,
				Tag:                     name + "/" + tag,
				SubscriptionCreatedAtNs: createdAtNs,
			})
		}
	}
	return ns
}

func sortNodeSummariesForService(nodes []NodeSummary, sortBy, sortOrder string) {
	sort.SliceStable(nodes, func(i, j int) bool {
		cmp := compareNodeSummaryForService(sortBy, nodes[i], nodes[j])
		if strings.EqualFold(sortOrder, "desc") {
			cmp = -cmp
		}
		return cmp < 0
	})
}

func compareNodeSummaryForService(sortBy string, a, b NodeSummary) int {
	var c int
	switch sortBy {
	case "created_at":
		c = strings.Compare(a.CreatedAt, b.CreatedAt)
	case "failure_count":
		if a.FailureCount < b.FailureCount {
			c = -1
		} else if a.FailureCount > b.FailureCount {
			c = 1
		}
	case "region":
		c = strings.Compare(a.Region, b.Region)
	default:
		c = strings.Compare(nodeTagSortKeyForService(a), nodeTagSortKeyForService(b))
	}
	if c != 0 {
		return c
	}
	return strings.Compare(a.NodeHash, b.NodeHash)
}

func nodeTagSortKeyForService(n NodeSummary) string {
	if n.DisplayTag != "" {
		return n.DisplayTag
	}
	bestCreated := int64(1<<63 - 1)
	bestTag := ""
	for _, tag := range n.Tags {
		if tag.SubscriptionCreatedAtNs < bestCreated || (tag.SubscriptionCreatedAtNs == bestCreated && (bestTag == "" || tag.Tag < bestTag)) {
			bestCreated = tag.SubscriptionCreatedAtNs
			bestTag = tag.Tag
		}
	}
	return bestTag
}

func paginateNodeSummaries(nodes []NodeSummary, limit, offset int) []NodeSummary {
	if limit <= 0 {
		limit = lenAllNodePage
	}
	if offset < 0 {
		offset = 0
	}
	if offset >= len(nodes) {
		return []NodeSummary{}
	}
	end := offset + limit
	if end > len(nodes) {
		end = len(nodes)
	}
	return append([]NodeSummary(nil), nodes[offset:end]...)
}

func countNodeSummaryUniqueEgressIPs(nodes []NodeSummary, exclude map[string]struct{}) int {
	seen := make(map[string]struct{})
	for _, n := range nodes {
		if n.EgressIP == "" {
			continue
		}
		if exclude != nil {
			if _, ok := exclude[n.EgressIP]; ok {
				continue
			}
		}
		seen[n.EgressIP] = struct{}{}
	}
	return len(seen)
}

func countNodeSummaryUniqueHealthyEgressIPs(nodes []NodeSummary) int {
	seen := make(map[string]struct{})
	for _, n := range nodes {
		if n.EgressIP == "" || !n.IsHealthyAndEnabled() {
			continue
		}
		seen[n.EgressIP] = struct{}{}
	}
	return len(seen)
}

func (s *ControlPlaneService) nodeEntryMatchesFilters(
	entry *node.NodeEntry,
	filters NodeFilters,
	subLookup node.SubLookupFunc,
) bool {
	// Enabled/disabled filter.
	if filters.Enabled != nil {
		enabled := true
		if subLookup != nil {
			enabled = entry.HasEnabledSubscription(subLookup)
		}
		if enabled != *filters.Enabled {
			return false
		}
	}

	// Node tag fuzzy search filter.
	if filters.TagKeyword != nil {
		keyword := strings.ToLower(strings.TrimSpace(*filters.TagKeyword))
		if keyword != "" {
			matched := false
			for _, subID := range entry.SubscriptionIDs() {
				sub := s.SubMgr.Lookup(subID)
				if sub == nil {
					continue
				}
				managed, ok := sub.ManagedNodes().LoadNode(entry.Hash)
				if !ok {
					continue
				}
				tags := managed.Tags
				for _, tag := range tags {
					displayTag := sub.Name() + "/" + tag
					if strings.Contains(strings.ToLower(displayTag), keyword) {
						matched = true
						break
					}
				}
				if matched {
					break
				}
			}
			if !matched {
				return false
			}
		}
	}

	// Region filter.
	if filters.Region != nil {
		region := entry.GetRegion(nil)
		if s.GeoIP != nil {
			region = entry.GetRegion(s.GeoIP.Lookup)
		}
		if region == "" || region != *filters.Region {
			return false
		}
	}
	// Circuit open filter.
	if filters.CircuitOpen != nil {
		if entry.IsCircuitOpen() != *filters.CircuitOpen {
			return false
		}
	}
	// Has outbound filter.
	if filters.HasOutbound != nil {
		if entry.HasOutbound() != *filters.HasOutbound {
			return false
		}
	}
	// Egress IP filter.
	if filters.EgressIP != nil {
		egressIP := entry.GetEgressIP()
		if !egressIP.IsValid() || egressIP.String() != *filters.EgressIP {
			return false
		}
	}
	// Probed since filter.
	if filters.ProbedSince != nil {
		lastUpdate := entry.LastLatencyProbeAttempt.Load()
		if lastUpdate < filters.ProbedSince.UnixNano() {
			return false
		}
	}
	return true
}

// GetNode returns a single node by hash.
func (s *ControlPlaneService) GetNode(hashStr string) (*NodeSummary, error) {
	h, err := node.ParseHex(hashStr)
	if err != nil {
		return nil, invalidArg("node_hash: invalid format")
	}
	entry, ok := s.Pool.GetEntry(h)
	if !ok {
		return nil, notFound("node not found")
	}
	ns := s.nodeEntryToSummary(h, entry)
	return &ns, nil
}

// ProbeEgress triggers a synchronous egress probe and returns results.
func (s *ControlPlaneService) ProbeEgress(hashStr string) (*probe.EgressProbeResult, error) {
	h, err := node.ParseHex(hashStr)
	if err != nil {
		return nil, invalidArg("node_hash: invalid format")
	}
	entry, ok := s.Pool.GetEntry(h)
	if !ok {
		return nil, notFound("node not found")
	}
	result, err := s.ProbeMgr.ProbeEgressSync(h)
	if err != nil {
		return nil, internal("egress probe failed", err)
	}
	result.Region = entry.GetRegion(nil)
	if s.GeoIP != nil {
		result.Region = entry.GetRegion(s.GeoIP.Lookup)
	}
	return result, nil
}

// ProbeLatency triggers a synchronous latency probe and returns results.
func (s *ControlPlaneService) ProbeLatency(hashStr string) (*probe.LatencyProbeResult, error) {
	h, err := node.ParseHex(hashStr)
	if err != nil {
		return nil, invalidArg("node_hash: invalid format")
	}
	if _, ok := s.Pool.GetEntry(h); !ok {
		return nil, notFound("node not found")
	}
	result, err := s.ProbeMgr.ProbeLatencySync(h)
	if err != nil {
		return nil, internal("latency probe failed", err)
	}
	return result, nil
}
