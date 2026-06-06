package service

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/Resinat/Resin/internal/model"
	"github.com/Resinat/Resin/internal/node"
	"github.com/Resinat/Resin/internal/state"
	"github.com/Resinat/Resin/internal/subscription"
	"github.com/Resinat/Resin/internal/topology"
)

// ------------------------------------------------------------------
// Subscription
// ------------------------------------------------------------------

// SubscriptionResponse is the API response for a subscription.
type SubscriptionResponse struct {
	ID                      string `json:"id"`
	Name                    string `json:"name"`
	SourceType              string `json:"source_type"`
	URL                     string `json:"url"`
	Content                 string `json:"content"`
	UpdateInterval          string `json:"update_interval"`
	NodeCount               int    `json:"node_count"`
	HealthyNodeCount        int    `json:"healthy_node_count"`
	Ephemeral               bool   `json:"ephemeral"`
	EphemeralNodeEvictDelay string `json:"ephemeral_node_evict_delay"`
	Enabled                 bool   `json:"enabled"`
	CreatedAt               string `json:"created_at"`
	LastChecked             string `json:"last_checked,omitempty"`
	LastUpdated             string `json:"last_updated,omitempty"`
	LastError               string `json:"last_error,omitempty"`
}

func subscriptionRuntimeNodeCount(sub *subscription.Subscription) int {
	if sub == nil {
		return 0
	}
	count := 0
	if managed := sub.ManagedNodes(); managed != nil {
		managed.RangeNodes(func(_ node.Hash, n subscription.ManagedNode) bool {
			if !n.Evicted {
				count++
			}
			return true
		})
	}
	return count
}

type SubscriptionListFilters struct {
	// Active controls the data source: nil/true lists the hot runtime manager,
	// while false lists the persisted DB inventory. Enabled filters only the
	// subscription's enable/disable switch.
	Active  *bool
	Enabled *bool
	Keyword string
}

type SubscriptionListOptions struct {
	SortBy    string
	SortOrder string
	Limit     int
	Offset    int
}

type SubscriptionListResult struct {
	Items  []SubscriptionResponse
	Total  int
	Limit  int
	Offset int
}

type subscriptionListRow struct {
	ID                        string
	Name                      string
	SourceType                string
	URL                       string
	Content                   string
	UpdateIntervalNs          int64
	Enabled                   bool
	Ephemeral                 bool
	EphemeralNodeEvictDelayNs int64
	CreatedAtNs               int64
	LastCheckedNs             int64
	LastUpdatedNs             int64
	LastError                 string
	runtime                   *subscription.Subscription
}

func subscriptionInventoryRow(sub model.Subscription, runtime *subscription.Subscription) subscriptionListRow {
	return subscriptionListRow{
		ID:                        sub.ID,
		Name:                      sub.Name,
		SourceType:                sub.SourceType,
		URL:                       sub.URL,
		Content:                   sub.Content,
		UpdateIntervalNs:          sub.UpdateIntervalNs,
		Enabled:                   sub.Enabled,
		Ephemeral:                 sub.Ephemeral,
		EphemeralNodeEvictDelayNs: sub.EphemeralNodeEvictDelayNs,
		CreatedAtNs:               sub.CreatedAtNs,
		LastCheckedNs:             sub.LastCheckedNs,
		LastUpdatedNs:             sub.LastUpdatedNs,
		LastError:                 sub.LastError,
		runtime:                   runtime,
	}
}

func subscriptionRuntimeRow(sub *subscription.Subscription) subscriptionListRow {
	if sub == nil {
		return subscriptionListRow{}
	}
	return subscriptionListRow{
		ID:                        sub.ID,
		Name:                      sub.Name(),
		SourceType:                sub.SourceType(),
		URL:                       sub.URL(),
		Content:                   sub.Content(),
		UpdateIntervalNs:          sub.UpdateIntervalNs(),
		Enabled:                   sub.Enabled(),
		Ephemeral:                 sub.Ephemeral(),
		EphemeralNodeEvictDelayNs: sub.EphemeralNodeEvictDelayNs(),
		CreatedAtNs:               sub.CreatedAtNs,
		LastCheckedNs:             sub.LastCheckedNs.Load(),
		LastUpdatedNs:             sub.LastUpdatedNs.Load(),
		LastError:                 sub.GetLastError(),
		runtime:                   sub,
	}
}

func subscriptionRowMatchesKeyword(row subscriptionListRow, rawKeyword string) bool {
	keyword := strings.ToLower(strings.TrimSpace(rawKeyword))
	if keyword == "" {
		return true
	}
	contains := func(v string) bool {
		return strings.Contains(strings.ToLower(v), keyword)
	}
	return contains(row.ID) || contains(row.Name) || contains(row.URL) || contains(row.SourceType)
}

func subscriptionRowSortKey(row subscriptionListRow, sortBy string) string {
	switch sortBy {
	case "created_at":
		return fmt.Sprintf("%020d", row.CreatedAtNs)
	case "last_checked":
		return fmt.Sprintf("%020d", row.LastCheckedNs)
	case "last_updated":
		return fmt.Sprintf("%020d", row.LastUpdatedNs)
	default:
		return strings.ToLower(row.Name)
	}
}

func sortSubscriptionRows(rows []subscriptionListRow, opts SubscriptionListOptions) {
	sort.SliceStable(rows, func(i, j int) bool {
		left := subscriptionRowSortKey(rows[i], opts.SortBy)
		right := subscriptionRowSortKey(rows[j], opts.SortBy)
		if left == right {
			if opts.SortOrder == "desc" {
				return rows[i].ID > rows[j].ID
			}
			return rows[i].ID < rows[j].ID
		}
		if opts.SortOrder == "desc" {
			return left > right
		}
		return left < right
	})
}

func paginateSubscriptionRows(rows []subscriptionListRow, opts SubscriptionListOptions) []subscriptionListRow {
	if opts.Offset >= len(rows) {
		return []subscriptionListRow{}
	}
	end := opts.Offset + opts.Limit
	if end > len(rows) {
		end = len(rows)
	}
	return rows[opts.Offset:end]
}

func (s *ControlPlaneService) subscriptionInventoryNodeCount(sub *subscription.Subscription) (int, error) {
	fallback := subscriptionRuntimeNodeCount(sub)
	if s == nil || s.Engine == nil || sub == nil {
		return fallback, nil
	}
	relations, err := s.Engine.LoadSubscriptionNodes(sub.ID)
	if err != nil {
		return 0, err
	}
	if len(relations) == 0 {
		return fallback, nil
	}
	count := 0
	for _, rel := range relations {
		if !rel.Evicted {
			count++
		}
	}
	return count, nil
}

func (s *ControlPlaneService) subscriptionHealthyNodeCount(sub *subscription.Subscription) int {
	if sub == nil {
		return 0
	}
	healthyNodeCount := 0
	var isHealthyAndEnabled func(*node.NodeEntry) bool
	if sub.Enabled() && s != nil && s.Pool != nil {
		isHealthyAndEnabled = s.Pool.MakeHealthyAndEnabledEvaluator()
	}
	if managed := sub.ManagedNodes(); managed != nil {
		managed.RangeNodes(func(h node.Hash, n subscription.ManagedNode) bool {
			if n.Evicted {
				return true
			}
			if isHealthyAndEnabled != nil {
				entry, ok := s.Pool.GetEntry(h)
				if ok && isHealthyAndEnabled(entry) {
					healthyNodeCount++
				}
			}
			return true
		})
	}
	return healthyNodeCount
}

func (s *ControlPlaneService) subToResponseWithCounts(sub *subscription.Subscription, nodeCount, healthyNodeCount int) SubscriptionResponse {
	resp := SubscriptionResponse{
		ID:                      sub.ID,
		Name:                    sub.Name(),
		SourceType:              sub.SourceType(),
		URL:                     sub.URL(),
		Content:                 sub.Content(),
		UpdateInterval:          time.Duration(sub.UpdateIntervalNs()).String(),
		NodeCount:               nodeCount,
		HealthyNodeCount:        healthyNodeCount,
		Ephemeral:               sub.Ephemeral(),
		EphemeralNodeEvictDelay: time.Duration(sub.EphemeralNodeEvictDelayNs()).String(),
		Enabled:                 sub.Enabled(),
		CreatedAt:               time.Unix(0, sub.CreatedAtNs).UTC().Format(time.RFC3339Nano),
	}
	if lc := sub.LastCheckedNs.Load(); lc > 0 {
		resp.LastChecked = time.Unix(0, lc).UTC().Format(time.RFC3339Nano)
	}
	if lu := sub.LastUpdatedNs.Load(); lu > 0 {
		resp.LastUpdated = time.Unix(0, lu).UTC().Format(time.RFC3339Nano)
	}
	resp.LastError = sub.GetLastError()
	return resp
}

func (s *ControlPlaneService) subToResponseWithNodeCount(sub *subscription.Subscription, nodeCount int) SubscriptionResponse {
	return s.subToResponseWithCounts(sub, nodeCount, s.subscriptionHealthyNodeCount(sub))
}

func (s *ControlPlaneService) subscriptionRowToResponse(row subscriptionListRow, nodeCount int) SubscriptionResponse {
	healthyNodeCount := 0
	if row.runtime != nil {
		healthyNodeCount = s.subscriptionHealthyNodeCount(row.runtime)
	}
	resp := SubscriptionResponse{
		ID:                      row.ID,
		Name:                    row.Name,
		SourceType:              row.SourceType,
		URL:                     row.URL,
		Content:                 row.Content,
		UpdateInterval:          time.Duration(row.UpdateIntervalNs).String(),
		NodeCount:               nodeCount,
		HealthyNodeCount:        healthyNodeCount,
		Ephemeral:               row.Ephemeral,
		EphemeralNodeEvictDelay: time.Duration(row.EphemeralNodeEvictDelayNs).String(),
		Enabled:                 row.Enabled,
		CreatedAt:               time.Unix(0, row.CreatedAtNs).UTC().Format(time.RFC3339Nano),
	}
	if row.LastCheckedNs > 0 {
		resp.LastChecked = time.Unix(0, row.LastCheckedNs).UTC().Format(time.RFC3339Nano)
	}
	if row.LastUpdatedNs > 0 {
		resp.LastUpdated = time.Unix(0, row.LastUpdatedNs).UTC().Format(time.RFC3339Nano)
	}
	resp.LastError = row.LastError
	return resp
}

func (s *ControlPlaneService) subToResponse(sub *subscription.Subscription) SubscriptionResponse {
	nodeCount, err := s.subscriptionInventoryNodeCount(sub)
	if err != nil {
		// Single-object responses predate DB-backed inventory counts. Keep them
		// available if cache.db is temporarily unreadable; bulk list endpoints
		// return the error instead of silently falling back.
		nodeCount = subscriptionRuntimeNodeCount(sub)
	}
	return s.subToResponseWithNodeCount(sub, nodeCount)
}

// ListSubscriptions returns subscriptions from the selected source. By default
// it lists the hot runtime manager (Active); callers can set Active=false to
// inspect the full persisted DB inventory. Enabled filters only the
// subscription enable/disable flag. Expensive per-row counts are computed after
// pagination so small pages do not scan every subscription's node set.
func (s *ControlPlaneService) ListSubscriptions(filters SubscriptionListFilters, opts SubscriptionListOptions) (SubscriptionListResult, error) {
	if opts.Limit <= 0 {
		opts.Limit = 50
	}
	if opts.Offset < 0 {
		opts.Offset = 0
	}
	if filters.Active == nil || *filters.Active {
		return s.listRuntimeSubscriptions(filters, opts)
	}
	return s.listInventorySubscriptions(filters, opts)
}

func (s *ControlPlaneService) listRuntimeSubscriptions(filters SubscriptionListFilters, opts SubscriptionListOptions) (SubscriptionListResult, error) {
	rows := make([]subscriptionListRow, 0)
	if s == nil || s.SubMgr == nil {
		return SubscriptionListResult{Items: []SubscriptionResponse{}, Total: 0, Limit: opts.Limit, Offset: opts.Offset}, nil
	}
	s.SubMgr.Range(func(_ string, sub *subscription.Subscription) bool {
		row := subscriptionRuntimeRow(sub)
		if row.ID == "" {
			return true
		}
		if filters.Enabled != nil && row.Enabled != *filters.Enabled {
			return true
		}
		if !subscriptionRowMatchesKeyword(row, filters.Keyword) {
			return true
		}
		rows = append(rows, row)
		return true
	})
	sortSubscriptionRows(rows, opts)
	total := len(rows)
	pageRows := paginateSubscriptionRows(rows, opts)
	items := make([]SubscriptionResponse, 0, len(pageRows))
	for _, row := range pageRows {
		items = append(items, s.subscriptionRowToResponse(row, subscriptionRuntimeNodeCount(row.runtime)))
	}
	return SubscriptionListResult{Items: items, Total: total, Limit: opts.Limit, Offset: opts.Offset}, nil
}

func (s *ControlPlaneService) listInventorySubscriptions(filters SubscriptionListFilters, opts SubscriptionListOptions) (SubscriptionListResult, error) {
	rows := make([]subscriptionListRow, 0)
	if s == nil || s.Engine == nil {
		return SubscriptionListResult{}, internal("load subscription inventory", errors.New("state engine unavailable"))
	}

	subs, err := s.Engine.ListSubscriptions()
	if err != nil {
		return SubscriptionListResult{}, internal("load subscription inventory", err)
	}
	for _, sub := range subs {
		var runtime *subscription.Subscription
		if s.SubMgr != nil {
			runtime = s.SubMgr.Lookup(sub.ID)
		}
		row := subscriptionInventoryRow(sub, runtime)
		if filters.Enabled != nil && row.Enabled != *filters.Enabled {
			continue
		}
		if !subscriptionRowMatchesKeyword(row, filters.Keyword) {
			continue
		}
		rows = append(rows, row)
	}

	sortSubscriptionRows(rows, opts)
	total := len(rows)
	pageRows := paginateSubscriptionRows(rows, opts)
	items := make([]SubscriptionResponse, 0, len(pageRows))

	ids := make([]string, 0, len(pageRows))
	for _, row := range pageRows {
		ids = append(ids, row.ID)
	}
	counts, err := s.Engine.CountSubscriptionNodes(ids)
	if err != nil {
		return SubscriptionListResult{}, internal("count subscription inventory nodes", err)
	}
	for _, row := range pageRows {
		items = append(items, s.subscriptionRowToResponse(row, counts[row.ID]))
	}

	return SubscriptionListResult{
		Items:  items,
		Total:  total,
		Limit:  opts.Limit,
		Offset: opts.Offset,
	}, nil
}

// GetSubscription returns a single subscription by ID.
func (s *ControlPlaneService) GetSubscription(id string) (*SubscriptionResponse, error) {
	sub := s.SubMgr.Lookup(id)
	if sub == nil {
		return nil, notFound("subscription not found")
	}
	r := s.subToResponse(sub)
	return &r, nil
}

// CreateSubscriptionRequest holds create subscription parameters.
type CreateSubscriptionRequest struct {
	Name                    *string `json:"name"`
	SourceType              *string `json:"source_type"`
	URL                     *string `json:"url"`
	Content                 *string `json:"content"`
	UpdateInterval          *string `json:"update_interval"`
	Enabled                 *bool   `json:"enabled"`
	Ephemeral               *bool   `json:"ephemeral"`
	EphemeralNodeEvictDelay *string `json:"ephemeral_node_evict_delay"`
}

const minSubscriptionUpdateInterval = 30 * time.Second
const defaultSubscriptionEphemeralNodeEvictDelay = 72 * time.Hour

func parseSubscriptionSourceType(raw *string) (string, *ServiceError) {
	if raw == nil {
		return subscription.SourceTypeRemote, nil
	}
	value := strings.ToLower(strings.TrimSpace(*raw))
	switch value {
	case subscription.SourceTypeRemote, subscription.SourceTypeLocal:
		return value, nil
	default:
		return "", invalidArg("source_type: must be remote or local")
	}
}

// CreateSubscription creates a new subscription.
func (s *ControlPlaneService) CreateSubscription(req CreateSubscriptionRequest) (*SubscriptionResponse, error) {
	if req.Name == nil || strings.TrimSpace(*req.Name) == "" {
		return nil, invalidArg("name is required")
	}
	name := strings.TrimSpace(*req.Name)

	sourceType, verr := parseSubscriptionSourceType(req.SourceType)
	if verr != nil {
		return nil, verr
	}

	subURL := ""
	content := ""
	switch sourceType {
	case subscription.SourceTypeRemote:
		if req.URL == nil || strings.TrimSpace(*req.URL) == "" {
			return nil, invalidArg("url is required for remote subscription")
		}
		subURL = strings.TrimSpace(*req.URL)
		if _, verr := parseHTTPAbsoluteURL("url", subURL); verr != nil {
			return nil, verr
		}
		if req.Content != nil && strings.TrimSpace(*req.Content) != "" {
			return nil, invalidArg("content is not allowed for remote subscription")
		}
	case subscription.SourceTypeLocal:
		if req.Content == nil || strings.TrimSpace(*req.Content) == "" {
			return nil, invalidArg("content is required for local subscription")
		}
		content = *req.Content
		if req.URL != nil && strings.TrimSpace(*req.URL) != "" {
			return nil, invalidArg("url is not allowed for local subscription")
		}
	default:
		return nil, invalidArg("source_type: must be remote or local")
	}

	updateInterval := 6 * time.Hour
	if req.UpdateInterval != nil {
		d, err := time.ParseDuration(*req.UpdateInterval)
		if err != nil {
			return nil, invalidArg("update_interval: " + err.Error())
		}
		if d < minSubscriptionUpdateInterval {
			return nil, invalidArg("update_interval: must be >= 30s")
		}
		updateInterval = d
	}

	enabled := true
	if req.Enabled != nil {
		enabled = *req.Enabled
	}
	ephemeral := false
	if req.Ephemeral != nil {
		ephemeral = *req.Ephemeral
	}
	ephemeralNodeEvictDelay := defaultSubscriptionEphemeralNodeEvictDelay
	if req.EphemeralNodeEvictDelay != nil {
		d, err := time.ParseDuration(*req.EphemeralNodeEvictDelay)
		if err != nil {
			return nil, invalidArg("ephemeral_node_evict_delay: " + err.Error())
		}
		if d < 0 {
			return nil, invalidArg("ephemeral_node_evict_delay: must be non-negative")
		}
		ephemeralNodeEvictDelay = d
	}

	id := uuid.New().String()
	now := time.Now().UnixNano()

	ms := model.Subscription{
		ID:                        id,
		Name:                      name,
		SourceType:                sourceType,
		URL:                       subURL,
		Content:                   content,
		UpdateIntervalNs:          int64(updateInterval),
		Enabled:                   enabled,
		Ephemeral:                 ephemeral,
		EphemeralNodeEvictDelayNs: int64(ephemeralNodeEvictDelay),
		CreatedAtNs:               now,
		UpdatedAtNs:               now,
	}
	if err := s.Engine.UpsertSubscription(ms); err != nil {
		return nil, internal("persist subscription", err)
	}

	sub := subscription.NewSubscription(id, name, subURL, enabled, ephemeral)
	sub.SetFetchConfig(subURL, int64(updateInterval))
	sub.SetSourceType(sourceType)
	sub.SetContent(content)
	sub.SetEphemeralNodeEvictDelayNs(int64(ephemeralNodeEvictDelay))
	sub.CreatedAtNs = now
	sub.UpdatedAtNs = now
	s.SubMgr.Register(sub)

	r := s.subToResponse(sub)
	return &r, nil
}

// UpdateSubscription applies a constrained partial patch to a subscription.
// This is not RFC 7396 JSON Merge Patch: patch must be a non-empty object and
// null values are rejected.
func (s *ControlPlaneService) UpdateSubscription(id string, patchJSON json.RawMessage) (*SubscriptionResponse, error) {
	patch, verr := parseMergePatch(patchJSON)
	if verr != nil {
		return nil, verr
	}
	if err := patch.validateFields(subscriptionPatchAllowedFields, func(key string) string {
		return fmt.Sprintf("field %q is read-only or unknown", key)
	}); err != nil {
		return nil, err
	}

	sub := s.SubMgr.Lookup(id)
	if sub == nil {
		return nil, notFound("subscription not found")
	}

	// Track what changed for side-effects.
	nameChanged := false
	enabledChanged := false
	urlChanged := false
	contentChanged := false
	sourceType := sub.SourceType()

	newName := sub.Name()
	if nameStr, ok, err := patch.optionalNonEmptyString("name"); err != nil {
		return nil, err
	} else if ok {
		newName = nameStr
		if newName != sub.Name() {
			nameChanged = true
		}
	}

	newURL := sub.URL()
	if urlStr, ok, err := patch.optionalString("url"); err != nil {
		return nil, err
	} else if ok {
		if sourceType != subscription.SourceTypeRemote {
			return nil, invalidArg("url: field is not allowed for local subscription")
		}
		if _, verr := parseHTTPAbsoluteURL("url", urlStr); verr != nil {
			return nil, verr
		}
		newURL = urlStr
		if newURL != sub.URL() {
			urlChanged = true
		}
	}

	newContent := sub.Content()
	if contentStr, ok, err := patch.optionalString("content"); err != nil {
		return nil, err
	} else if ok {
		if sourceType != subscription.SourceTypeLocal {
			return nil, invalidArg("content: field is not allowed for remote subscription")
		}
		if strings.TrimSpace(contentStr) == "" {
			return nil, invalidArg("content: must be a non-empty string")
		}
		newContent = contentStr
		if newContent != sub.Content() {
			contentChanged = true
		}
	}

	newInterval := sub.UpdateIntervalNs()
	if d, ok, err := patch.optionalDurationString("update_interval"); err != nil {
		return nil, err
	} else if ok {
		if d < minSubscriptionUpdateInterval {
			return nil, invalidArg("update_interval: must be >= 30s")
		}
		newInterval = int64(d)
	}

	newEnabled := sub.Enabled()
	if b, ok, err := patch.optionalBool("enabled"); err != nil {
		return nil, err
	} else if ok {
		if b != newEnabled {
			enabledChanged = true
		}
		newEnabled = b
	}

	newEphemeral := sub.Ephemeral()
	if b, ok, err := patch.optionalBool("ephemeral"); err != nil {
		return nil, err
	} else if ok {
		newEphemeral = b
	}

	newEphemeralNodeEvictDelay := sub.EphemeralNodeEvictDelayNs()
	if d, ok, err := patch.optionalDurationString("ephemeral_node_evict_delay"); err != nil {
		return nil, err
	} else if ok {
		if d < 0 {
			return nil, invalidArg("ephemeral_node_evict_delay: must be non-negative")
		}
		newEphemeralNodeEvictDelay = int64(d)
	}

	now := time.Now().UnixNano()
	ms := model.Subscription{
		ID:                        id,
		Name:                      newName,
		SourceType:                sourceType,
		URL:                       newURL,
		Content:                   newContent,
		UpdateIntervalNs:          newInterval,
		Enabled:                   newEnabled,
		Ephemeral:                 newEphemeral,
		EphemeralNodeEvictDelayNs: newEphemeralNodeEvictDelay,
		CreatedAtNs:               sub.CreatedAtNs,
		UpdatedAtNs:               now,
	}
	if err := s.Engine.UpsertSubscription(ms); err != nil {
		return nil, internal("persist subscription", err)
	}

	// Apply side-effects via scheduler.
	sub.SetFetchConfig(newURL, newInterval)
	sub.SetContent(newContent)
	sub.SetEphemeral(newEphemeral)
	sub.SetEphemeralNodeEvictDelayNs(newEphemeralNodeEvictDelay)
	sub.UpdatedAtNs = now

	if nameChanged {
		s.Scheduler.RenameSubscription(sub, newName)
	}
	if enabledChanged {
		s.Scheduler.SetSubscriptionEnabled(sub, newEnabled)
	}
	if urlChanged || contentChanged {
		go s.Scheduler.UpdateSubscription(sub)
	}

	r := s.subToResponse(sub)
	return &r, nil
}

// DeleteSubscription deletes a subscription and evicts its nodes.
func (s *ControlPlaneService) DeleteSubscription(id string) error {
	sub := s.SubMgr.Lookup(id)
	if sub == nil {
		return notFound("subscription not found")
	}

	var (
		managedHashes []node.Hash
		deleteErr     error
	)

	// Keep delete atomic across persistence + in-memory runtime state:
	// if DB delete fails, do not mutate runtime subscription/node state.
	sub.WithOpLock(func() {
		// Re-check under lock in case another goroutine removed it between
		// the initial Lookup and lock acquisition.
		lockedSub := s.SubMgr.Lookup(id)
		if lockedSub == nil {
			deleteErr = notFound("subscription not found")
			return
		}

		lockedSub.ManagedNodes().RangeNodes(func(h node.Hash, _ subscription.ManagedNode) bool {
			managedHashes = append(managedHashes, h)
			return true
		})

		if err := s.Engine.DeleteSubscription(id); err != nil {
			if errors.Is(err, state.ErrNotFound) {
				deleteErr = notFound("subscription not found")
			} else {
				deleteErr = internal("delete subscription", err)
			}
			return
		}

		// Persist succeeded; now apply in-memory cleanup.
		for _, h := range managedHashes {
			s.Pool.RemoveNodeFromSub(h, id)
		}
		s.SubMgr.Unregister(id)
	})

	return deleteErr
}

// RefreshSubscription triggers an immediate subscription refresh (blocks).
func (s *ControlPlaneService) RefreshSubscription(id string) error {
	sub := s.SubMgr.Lookup(id)
	if sub == nil {
		return notFound("subscription not found")
	}
	s.Scheduler.UpdateSubscription(sub)
	return nil
}

// CleanupSubscriptionCircuitOpenNodes removes problematic nodes from a subscription.
// It marks nodes as evicted (while keeping managed hashes) for nodes currently
// circuit-open, and nodes with no outbound while carrying a non-empty last error.
func (s *ControlPlaneService) CleanupSubscriptionCircuitOpenNodes(id string) (int, error) {
	return s.cleanupSubscriptionCircuitOpenNodesWithHook(id, nil)
}

// cleanupSubscriptionCircuitOpenNodesWithHook performs cleanup with an optional
// hook between first scan and second confirmation scan. The hook is only used
// by tests to simulate TOCTOU recovery.
func (s *ControlPlaneService) cleanupSubscriptionCircuitOpenNodesWithHook(
	id string,
	betweenScans func(),
) (int, error) {
	sub := s.SubMgr.Lookup(id)
	if sub == nil {
		return 0, notFound("subscription not found")
	}

	var (
		cleanedCount int
		evicted      []node.Hash
		cleanupErr   error
	)

	sub.WithOpLock(func() {
		// Re-check under lock in case another goroutine deleted the subscription
		// between lookup and lock acquisition.
		lockedSub := s.SubMgr.Lookup(id)
		if lockedSub == nil {
			cleanupErr = notFound("subscription not found")
			return
		}

		cleanedCount, evicted = topology.CleanupSubscriptionNodesWithConfirmNoLock(
			lockedSub,
			s.Pool,
			shouldCleanupSubscriptionNode,
			betweenScans,
		)
	})
	if cleanupErr != nil {
		return 0, cleanupErr
	}

	if s.Engine != nil {
		for _, h := range evicted {
			s.Engine.MarkSubscriptionNode(id, h.Hex())
		}
	}

	return cleanedCount, nil
}

func shouldCleanupSubscriptionNode(entry *node.NodeEntry) bool {
	if entry == nil {
		return false
	}
	return entry.IsCircuitOpen() || (!entry.HasOutbound() && entry.GetLastError() != "")
}
