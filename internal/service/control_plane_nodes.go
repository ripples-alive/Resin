package service

import (
	"net/netip"
	"strings"
	"time"

	"github.com/Resinat/Resin/internal/model"
	"github.com/Resinat/Resin/internal/node"
	"github.com/Resinat/Resin/internal/probe"
	"github.com/Resinat/Resin/internal/subscription"
)

// ------------------------------------------------------------------
// Nodes
// ------------------------------------------------------------------

// NodeFilters holds query filters for listing nodes.
type NodeFilters struct {
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

// NodeListScope selects which inventory backs node list queries.
type NodeListScope string

const (
	NodeListScopeActive  NodeListScope = "active"
	NodeListScopeCatalog NodeListScope = "catalog"
)

// ListNodes returns nodes from the pool with optional filters.
func (s *ControlPlaneService) ListNodes(filters NodeFilters) ([]NodeSummary, error) {
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

// ListCatalogNodes returns nodes from the persistent catalog without promoting
// cold nodes into the hot routing pool. It is intended for admin inventory APIs
// when active-only runtime keeps memory limited to routable nodes.
func (s *ControlPlaneService) ListCatalogNodes(filters NodeFilters) ([]NodeSummary, error) {
	if s == nil || s.Engine == nil {
		return nil, internal("catalog store unavailable", nil)
	}
	if filters.SubscriptionID != nil && s.SubMgr.Lookup(*filters.SubscriptionID) == nil {
		return nil, notFound("subscription not found")
	}

	var platformView map[string]struct{}
	if filters.PlatformID != nil {
		if s.Pool == nil {
			return nil, notFound("platform not found")
		}
		plat, ok := s.Pool.GetPlatform(*filters.PlatformID)
		if !ok {
			return nil, notFound("platform not found")
		}
		platformView = make(map[string]struct{}, plat.View().Size())
		plat.View().Range(func(h node.Hash) bool {
			platformView[h.Hex()] = struct{}{}
			return true
		})
	}

	statics, err := s.Engine.LoadAllNodesStatic()
	if err != nil {
		return nil, internal("load catalog nodes", err)
	}
	dynamics, err := s.Engine.LoadAllNodesDynamic()
	if err != nil {
		return nil, internal("load catalog node state", err)
	}
	latencies, err := s.Engine.LoadAllNodeLatency()
	if err != nil {
		return nil, internal("load catalog node latency", err)
	}
	subNodes, err := s.Engine.LoadAllSubscriptionNodes()
	if err != nil {
		return nil, internal("load catalog subscription nodes", err)
	}

	dynamicByHash := make(map[string]model.NodeDynamic, len(dynamics))
	for _, dyn := range dynamics {
		dynamicByHash[dyn.Hash] = dyn
	}
	latencyByHash := make(map[string][]model.NodeLatency)
	for _, lat := range latencies {
		latencyByHash[lat.NodeHash] = append(latencyByHash[lat.NodeHash], lat)
	}
	relationsByHash := make(map[string][]model.SubscriptionNode)
	subFilterHashes := make(map[string]struct{})
	for _, sn := range subNodes {
		if sn.Evicted {
			continue
		}
		relationsByHash[sn.NodeHash] = append(relationsByHash[sn.NodeHash], sn)
		if filters.SubscriptionID != nil && sn.SubscriptionID == *filters.SubscriptionID {
			subFilterHashes[sn.NodeHash] = struct{}{}
		}
	}

	result := make([]NodeSummary, 0, len(statics))
	for _, st := range statics {
		if platformView != nil {
			if _, ok := platformView[st.Hash]; !ok {
				continue
			}
		}
		if filters.SubscriptionID != nil {
			if _, ok := subFilterHashes[st.Hash]; !ok {
				continue
			}
		}
		relations := relationsByHash[st.Hash]
		if len(relations) == 0 {
			continue
		}
		dyn := dynamicByHash[st.Hash]
		ns, lastProbeNs, ok := s.catalogNodeToSummary(st, dyn, latencyByHash[st.Hash], relations)
		if !ok {
			continue
		}
		if !catalogNodeMatchesFilters(ns, lastProbeNs, filters) {
			continue
		}
		result = append(result, ns)
	}
	if result == nil {
		result = []NodeSummary{}
	}
	return result, nil
}

func (s *ControlPlaneService) catalogNodeToSummary(
	st model.NodeStatic,
	dyn model.NodeDynamic,
	latencies []model.NodeLatency,
	relations []model.SubscriptionNode,
) (NodeSummary, int64, bool) {
	h, err := node.ParseHex(st.Hash)
	if err != nil {
		return NodeSummary{}, 0, false
	}

	var ns NodeSummary
	if s != nil && s.Pool != nil {
		if entry, ok := s.Pool.GetEntry(h); ok {
			ns = s.nodeEntryToSummary(h, entry)
		}
	}
	if ns.NodeHash == "" {
		ns = NodeSummary{
			NodeHash:     st.Hash,
			CreatedAt:    time.Unix(0, st.CreatedAtNs).UTC().Format(time.RFC3339Nano),
			Enabled:      false,
			HasOutbound:  false,
			FailureCount: dyn.FailureCount,
		}
		if dyn.CircuitOpenSince > 0 {
			t := time.Unix(0, dyn.CircuitOpenSince).UTC().Format(time.RFC3339Nano)
			ns.CircuitOpenSince = &t
		}
		if dyn.EgressIP != "" {
			ns.EgressIP = dyn.EgressIP
			ns.EgressIPs = append([]string(nil), dyn.EgressIPs...)
			if len(ns.EgressIPs) == 0 {
				ns.EgressIPs = []string{dyn.EgressIP}
			}
			ns.Region = dyn.EgressRegion
			if s != nil && s.GeoIP != nil {
				if addr, err := netip.ParseAddr(dyn.EgressIP); err == nil {
					if region := s.GeoIP.Lookup(addr); region != "" {
						ns.Region = region
					}
				}
			}
		}
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

	tags, enabled, displayTag := s.catalogNodeTags(relations)
	ns.CreatedAt = time.Unix(0, st.CreatedAtNs).UTC().Format(time.RFC3339Nano)
	ns.Enabled = enabled
	ns.DisplayTag = displayTag
	ns.Tags = tags
	if ns.Tags == nil {
		ns.Tags = []NodeTag{}
	}
	if ns.ReferenceLatencyMs == nil && s != nil && s.RuntimeCfg != nil {
		if cfg := s.RuntimeCfg.Load(); cfg != nil {
			if avgMs, ok := averageCatalogLatencyMs(latencies, cfg.LatencyAuthorities); ok {
				ns.ReferenceLatencyMs = &avgMs
			}
		}
	}
	return ns, dyn.LastLatencyProbeAttemptNs, true
}

func (s *ControlPlaneService) catalogNodeTags(relations []model.SubscriptionNode) ([]NodeTag, bool, string) {
	tags := make([]NodeTag, 0)
	enabled := false
	type displayCandidate struct {
		tag       string
		enabled   bool
		createdNs int64
		subID     string
	}
	var candidates []displayCandidate
	for _, rel := range relations {
		if rel.Evicted {
			continue
		}
		sub := s.SubMgr.Lookup(rel.SubscriptionID)
		if sub == nil {
			continue
		}
		if sub.Enabled() {
			enabled = true
		}
		for _, tag := range rel.Tags {
			display := sub.Name() + "/" + tag
			tags = append(tags, NodeTag{
				SubscriptionID:          rel.SubscriptionID,
				SubscriptionName:        sub.Name(),
				Tag:                     display,
				SubscriptionCreatedAtNs: sub.CreatedAtNs,
			})
			candidates = append(candidates, displayCandidate{tag: display, enabled: sub.Enabled(), createdNs: sub.CreatedAtNs, subID: rel.SubscriptionID})
		}
	}
	pick := func(enabledOnly bool) string {
		bestSet := false
		best := displayCandidate{}
		for _, c := range candidates {
			if enabledOnly && !c.enabled {
				continue
			}
			if !bestSet || c.createdNs < best.createdNs || (c.createdNs == best.createdNs && (c.subID < best.subID || (c.subID == best.subID && c.tag < best.tag))) {
				bestSet = true
				best = c
			}
		}
		if !bestSet {
			return ""
		}
		return best.tag
	}
	displayTag := pick(true)
	if displayTag == "" {
		displayTag = pick(false)
	}
	return tags, enabled, displayTag
}

func averageCatalogLatencyMs(latencies []model.NodeLatency, authorities []string) (float64, bool) {
	if len(latencies) == 0 || len(authorities) == 0 {
		return 0, false
	}
	byDomain := make(map[string]int64, len(latencies))
	for _, lat := range latencies {
		if lat.EwmaNs > 0 {
			byDomain[strings.ToLower(lat.Domain)] = lat.EwmaNs
		}
	}
	var total float64
	count := 0
	for _, authority := range authorities {
		if ewma, ok := byDomain[strings.ToLower(authority)]; ok {
			total += float64(ewma) / float64(time.Millisecond)
			count++
		}
	}
	if count == 0 {
		return 0, false
	}
	return total / float64(count), true
}

func catalogNodeMatchesFilters(ns NodeSummary, lastProbeNs int64, filters NodeFilters) bool {
	if filters.Enabled != nil && ns.Enabled != *filters.Enabled {
		return false
	}
	if filters.TagKeyword != nil {
		keyword := strings.ToLower(strings.TrimSpace(*filters.TagKeyword))
		if keyword != "" {
			matched := strings.Contains(strings.ToLower(ns.DisplayTag), keyword)
			for _, tag := range ns.Tags {
				if strings.Contains(strings.ToLower(tag.Tag), keyword) {
					matched = true
					break
				}
			}
			if !matched {
				return false
			}
		}
	}
	if filters.Region != nil && (ns.Region == "" || ns.Region != *filters.Region) {
		return false
	}
	if filters.CircuitOpen != nil {
		if (ns.CircuitOpenSince != nil) != *filters.CircuitOpen {
			return false
		}
	}
	if filters.HasOutbound != nil && ns.HasOutbound != *filters.HasOutbound {
		return false
	}
	if filters.EgressIP != nil && ns.EgressIP != *filters.EgressIP {
		return false
	}
	if filters.ProbedSince != nil && lastProbeNs < filters.ProbedSince.UnixNano() {
		return false
	}
	return true
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
