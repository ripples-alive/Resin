package service

import (
	"net/netip"
	"sort"
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

// ListNodes returns active runtime nodes from the in-memory pool with optional filters.
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

// ListInventoryNodes returns the persisted DB inventory with optional filters.
// It overlays runtime state for nodes that are also present in the active pool,
// but it does not require a node to be active in memory.
func (s *ControlPlaneService) ListInventoryNodes(filters NodeFilters) ([]NodeSummary, error) {
	if s == nil || s.Engine == nil {
		return nil, internal("inventory store not configured", nil)
	}

	var platformView map[node.Hash]struct{}
	if filters.PlatformID != nil {
		if s.Pool == nil {
			return nil, notFound("platform not found")
		}
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

	if filters.SubscriptionID != nil && (s.SubMgr == nil || s.SubMgr.Lookup(*filters.SubscriptionID) == nil) {
		return nil, notFound("subscription not found")
	}

	statics, err := s.Engine.LoadAllNodesStatic()
	if err != nil {
		return nil, internal("load node inventory", err)
	}
	dynamics, err := s.Engine.LoadAllNodesDynamic()
	if err != nil {
		return nil, internal("load node dynamics", err)
	}
	relations, err := s.Engine.LoadAllSubscriptionNodes()
	if err != nil {
		return nil, internal("load subscription node inventory", err)
	}
	latencies, err := s.Engine.LoadAllNodeLatency()
	if err != nil {
		return nil, internal("load node latency inventory", err)
	}

	dynamicByHash := make(map[string]model.NodeDynamic, len(dynamics))
	for _, dynamic := range dynamics {
		dynamicByHash[dynamic.Hash] = dynamic
	}
	relationsByHash := make(map[string][]model.SubscriptionNode)
	for _, relation := range relations {
		if relation.Evicted {
			continue
		}
		relationsByHash[relation.NodeHash] = append(relationsByHash[relation.NodeHash], relation)
	}
	latenciesByHash := make(map[string]map[string]model.NodeLatency)
	for _, latency := range latencies {
		byDomain := latenciesByHash[latency.NodeHash]
		if byDomain == nil {
			byDomain = make(map[string]model.NodeLatency)
			latenciesByHash[latency.NodeHash] = byDomain
		}
		byDomain[latency.Domain] = latency
	}

	result := make([]NodeSummary, 0, len(statics))
	for _, static := range statics {
		rels := relationsByHash[static.Hash]
		if len(rels) == 0 {
			continue
		}

		h, hashErr := node.ParseHex(static.Hash)
		hashOK := hashErr == nil
		if platformView != nil {
			if !hashOK {
				continue
			}
			if _, ok := platformView[h]; !ok {
				continue
			}
		}

		if filters.SubscriptionID != nil {
			matched := false
			for _, rel := range rels {
				if rel.SubscriptionID == *filters.SubscriptionID {
					matched = true
					break
				}
			}
			if !matched {
				continue
			}
		}

		summary := s.inventoryNodeSummary(static, dynamicByHash[static.Hash], rels, latenciesByHash[static.Hash], h, hashOK)
		if !s.nodeSummaryMatchesFilters(summary, filters) {
			continue
		}
		result = append(result, summary)
	}

	if result == nil {
		result = []NodeSummary{}
	}
	return result, nil
}

func (s *ControlPlaneService) inventoryNodeSummary(
	static model.NodeStatic,
	dynamic model.NodeDynamic,
	relations []model.SubscriptionNode,
	latencies map[string]model.NodeLatency,
	h node.Hash,
	hashOK bool,
) NodeSummary {
	ns := NodeSummary{
		NodeHash:    static.Hash,
		CreatedAt:   time.Unix(0, static.CreatedAtNs).UTC().Format(time.RFC3339Nano),
		Enabled:     inventoryRelationsEnabled(s, relations),
		HasOutbound: false,
		Tags:        inventoryRelationTags(s, relations),
	}

	if dynamic.CircuitOpenSince > 0 {
		t := time.Unix(0, dynamic.CircuitOpenSince).UTC().Format(time.RFC3339Nano)
		ns.CircuitOpenSince = &t
	}
	ns.FailureCount = dynamic.FailureCount
	if ip, err := netip.ParseAddr(dynamic.EgressIP); err == nil && ip.IsValid() {
		ns.EgressIP = ip.String()
		if len(dynamic.EgressIPs) > 0 {
			ns.EgressIPs = append([]string(nil), dynamic.EgressIPs...)
		} else {
			ns.EgressIPs = []string{ns.EgressIP}
		}
		ns.Region = strings.ToLower(strings.TrimSpace(dynamic.EgressRegion))
		if ns.Region == "" && s != nil && s.GeoIP != nil {
			ns.Region = s.GeoIP.Lookup(ip)
		}
	}
	if dynamic.EgressUpdatedAtNs > 0 {
		ns.LastEgressUpdate = time.Unix(0, dynamic.EgressUpdatedAtNs).UTC().Format(time.RFC3339Nano)
	}
	if dynamic.LastLatencyProbeAttemptNs > 0 {
		ns.LastLatencyProbeAttempt = time.Unix(0, dynamic.LastLatencyProbeAttemptNs).UTC().Format(time.RFC3339Nano)
	}
	if dynamic.LastAuthorityLatencyProbeAttemptNs > 0 {
		ns.LastAuthorityLatencyProbeAttempt = time.Unix(0, dynamic.LastAuthorityLatencyProbeAttemptNs).UTC().Format(time.RFC3339Nano)
	}
	if dynamic.LastEgressUpdateAttemptNs > 0 {
		ns.LastEgressUpdateAttempt = time.Unix(0, dynamic.LastEgressUpdateAttemptNs).UTC().Format(time.RFC3339Nano)
	}
	if avgMs, ok := s.inventoryReferenceLatencyMs(latencies); ok {
		ns.ReferenceLatencyMs = &avgMs
	}

	if hashOK && s != nil && s.Pool != nil {
		if entry, ok := s.Pool.GetEntry(h); ok {
			runtime := s.nodeEntryToSummary(h, entry)
			ns.HasOutbound = runtime.HasOutbound
			ns.LastError = runtime.LastError
			ns.CircuitOpenSince = runtime.CircuitOpenSince
			ns.FailureCount = runtime.FailureCount
			ns.EgressIP = runtime.EgressIP
			ns.EgressIPs = runtime.EgressIPs
			ns.Region = runtime.Region
			ns.LastEgressUpdate = runtime.LastEgressUpdate
			ns.LastLatencyProbeAttempt = runtime.LastLatencyProbeAttempt
			ns.LastAuthorityLatencyProbeAttempt = runtime.LastAuthorityLatencyProbeAttempt
			ns.LastEgressUpdateAttempt = runtime.LastEgressUpdateAttempt
			ns.ReferenceLatencyMs = runtime.ReferenceLatencyMs
			if runtime.DisplayTag != "" {
				ns.DisplayTag = runtime.DisplayTag
			}
		}
	}

	if ns.Tags == nil {
		ns.Tags = []NodeTag{}
	}
	return ns
}

func inventoryRelationsEnabled(s *ControlPlaneService, relations []model.SubscriptionNode) bool {
	if s == nil || s.SubMgr == nil {
		return true
	}
	for _, rel := range relations {
		sub := s.SubMgr.Lookup(rel.SubscriptionID)
		if sub != nil && sub.Enabled() {
			return true
		}
	}
	return false
}

func inventoryRelationTags(s *ControlPlaneService, relations []model.SubscriptionNode) []NodeTag {
	tags := []NodeTag{}
	for _, rel := range relations {
		subName := rel.SubscriptionID
		createdAtNs := int64(0)
		if s != nil && s.SubMgr != nil {
			if sub := s.SubMgr.Lookup(rel.SubscriptionID); sub != nil {
				subName = sub.Name()
				createdAtNs = sub.CreatedAtNs
			}
		}
		for _, tag := range rel.Tags {
			tags = append(tags, NodeTag{
				SubscriptionID:          rel.SubscriptionID,
				SubscriptionName:        subName,
				Tag:                     subName + "/" + tag,
				SubscriptionCreatedAtNs: createdAtNs,
			})
		}
	}
	sort.SliceStable(tags, func(i, j int) bool {
		if tags[i].SubscriptionCreatedAtNs != tags[j].SubscriptionCreatedAtNs {
			return tags[i].SubscriptionCreatedAtNs < tags[j].SubscriptionCreatedAtNs
		}
		if tags[i].SubscriptionID != tags[j].SubscriptionID {
			return tags[i].SubscriptionID < tags[j].SubscriptionID
		}
		return tags[i].Tag < tags[j].Tag
	})
	return tags
}

func (s *ControlPlaneService) inventoryReferenceLatencyMs(latencies map[string]model.NodeLatency) (float64, bool) {
	if len(latencies) == 0 || s == nil || s.RuntimeCfg == nil {
		return 0, false
	}
	cfg := s.RuntimeCfg.Load()
	if cfg == nil || len(cfg.LatencyAuthorities) == 0 {
		return 0, false
	}
	var sumMs float64
	count := 0
	for _, domain := range cfg.LatencyAuthorities {
		domain = strings.TrimSpace(domain)
		if domain == "" {
			continue
		}
		latency, ok := latencies[domain]
		if !ok {
			continue
		}
		sumMs += float64(time.Duration(latency.EwmaNs).Milliseconds())
		count++
	}
	if count == 0 {
		return 0, false
	}
	return sumMs / float64(count), true
}

func (s *ControlPlaneService) nodeSummaryMatchesFilters(summary NodeSummary, filters NodeFilters) bool {
	if filters.Enabled != nil && summary.Enabled != *filters.Enabled {
		return false
	}
	if filters.TagKeyword != nil {
		keyword := strings.ToLower(strings.TrimSpace(*filters.TagKeyword))
		if keyword != "" {
			matched := strings.Contains(strings.ToLower(summary.DisplayTag), keyword)
			for _, tag := range summary.Tags {
				if matched {
					break
				}
				matched = strings.Contains(strings.ToLower(tag.Tag), keyword) ||
					strings.Contains(strings.ToLower(tag.SubscriptionName), keyword)
			}
			if !matched {
				return false
			}
		}
	}
	if filters.Region != nil {
		if summary.Region == "" || summary.Region != *filters.Region {
			return false
		}
	}
	if filters.CircuitOpen != nil {
		if (summary.CircuitOpenSince != nil) != *filters.CircuitOpen {
			return false
		}
	}
	if filters.HasOutbound != nil && summary.HasOutbound != *filters.HasOutbound {
		return false
	}
	if filters.EgressIP != nil {
		if summary.EgressIP == "" || summary.EgressIP != *filters.EgressIP {
			return false
		}
	}
	if filters.ProbedSince != nil {
		if summary.LastLatencyProbeAttempt == "" {
			return false
		}
		lastUpdate, err := time.Parse(time.RFC3339Nano, summary.LastLatencyProbeAttempt)
		if err != nil || lastUpdate.Before(*filters.ProbedSince) {
			return false
		}
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
