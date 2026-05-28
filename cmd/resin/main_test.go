package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/netip"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Resinat/Resin/internal/config"
	"github.com/Resinat/Resin/internal/geoip"
	"github.com/Resinat/Resin/internal/model"
	"github.com/Resinat/Resin/internal/node"
	"github.com/Resinat/Resin/internal/outbound"
	"github.com/Resinat/Resin/internal/platform"
	"github.com/Resinat/Resin/internal/state"
	"github.com/Resinat/Resin/internal/subscription"
	"github.com/Resinat/Resin/internal/testutil"
	"github.com/Resinat/Resin/internal/topology"
	"github.com/sagernet/sing-box/adapter"
)

func newBootstrapTestRuntime(runtimeCfg *config.RuntimeConfig) (*topology.SubscriptionManager, *topology.GlobalNodePool) {
	subManager := topology.NewSubscriptionManager()
	pool := topology.NewGlobalNodePool(topology.PoolConfig{
		SubLookup:              subManager.Lookup,
		GeoLookup:              func(netip.Addr) string { return "" },
		MaxLatencyTableEntries: 16,
		MaxConsecutiveFailures: func() int { return runtimeCfg.MaxConsecutiveFailures },
		LatencyDecayWindow: func() time.Duration {
			return time.Duration(runtimeCfg.LatencyDecayWindow)
		},
	})
	return subManager, pool
}

func newDefaultPlatformEnvConfig() *config.EnvConfig {
	return &config.EnvConfig{
		AuthVersion:                                     config.AuthVersionLegacyV0,
		DefaultPlatformStickyTTL:                        7 * 24 * time.Hour,
		DefaultPlatformRegexFilters:                     []string{},
		DefaultPlatformRegionFilters:                    []string{},
		DefaultPlatformReverseProxyMissAction:           "TREAT_AS_EMPTY",
		DefaultPlatformReverseProxyEmptyAccountBehavior: "ACCOUNT_HEADER_RULE",
		DefaultPlatformReverseProxyFixedAccountHeader:   "Authorization",
		DefaultPlatformAllocationPolicy:                 "BALANCED",
		ActiveOnlyRuntime:                               true,
	}
}

type trackingBootstrapBuilder struct {
	failRaw map[string]bool
	built   []string
}

func sameStringSet(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	counts := make(map[string]int, len(want))
	for _, item := range want {
		counts[item]++
	}
	for _, item := range got {
		if counts[item] == 0 {
			return false
		}
		counts[item]--
	}
	return true
}

func (b *trackingBootstrapBuilder) Build(raw json.RawMessage) (adapter.Outbound, error) {
	b.built = append(b.built, string(raw))
	if b.failRaw[string(raw)] {
		return nil, errors.New("bootstrap build failed")
	}
	return testutil.NewNoopOutbound(), nil
}

type staticSubscriptionDownloader struct {
	body []byte
	err  error
}

func (d staticSubscriptionDownloader) Download(_ context.Context, _ string) ([]byte, error) {
	if d.err != nil {
		return nil, d.err
	}
	return append([]byte(nil), d.body...), nil
}

func newColdCheckTestRuntime(
	engine *state.StateEngine,
	runtimeCfg *config.RuntimeConfig,
) (*topology.SubscriptionManager, *topology.GlobalNodePool) {
	subManager := topology.NewSubscriptionManager()
	pool := topology.NewGlobalNodePool(topology.PoolConfig{
		SubLookup:              subManager.Lookup,
		GeoLookup:              func(netip.Addr) string { return "us" },
		MaxLatencyTableEntries: 16,
		MaxConsecutiveFailures: func() int { return runtimeCfg.MaxConsecutiveFailures },
		LatencyDecayWindow: func() time.Duration {
			return time.Duration(runtimeCfg.LatencyDecayWindow)
		},
		OnNodeAdded: func(hash node.Hash) {
			engine.MarkNodeStatic(hash.Hex())
		},
		OnSubNodeChanged: func(subID string, hash node.Hash, added bool) {
			if added {
				engine.MarkSubscriptionNode(subID, hash.Hex())
			} else {
				engine.MarkSubscriptionNodeDelete(subID, hash.Hex())
			}
		},
		OnNodeDynamicChanged: func(hash node.Hash) {
			engine.MarkNodeDynamic(hash.Hex())
		},
		OnNodeLatencyChanged: func(hash node.Hash, domain string) {
			engine.MarkNodeLatency(hash.Hex(), domain)
		},
	})
	return subManager, pool
}

func TestAuthVersionStartupWarning_LegacyV0(t *testing.T) {
	msg := authVersionStartupWarning(config.AuthVersionLegacyV0)
	if msg == "" {
		t.Fatal("expected warning message for LEGACY_V0")
	}
	if !strings.Contains(msg, "RESIN_AUTH_VERSION=LEGACY_V0") {
		t.Fatalf("warning message should mention LEGACY_V0, got: %q", msg)
	}
	if !strings.Contains(msg, config.AuthMigrationGuideURL) {
		t.Fatalf("warning message should mention migration guide, got: %q", msg)
	}
}

func TestAuthVersionStartupWarning_V1(t *testing.T) {
	msg := authVersionStartupWarning(config.AuthVersionV1)
	if msg != "" {
		t.Fatalf("expected empty warning for V1, got: %q", msg)
	}
}

func TestBootstrapTopology_CreatesDefaultPlatformWhenMissing(t *testing.T) {
	engine, closer, err := state.PersistenceBootstrap(t.TempDir(), t.TempDir())
	if err != nil {
		t.Fatalf("PersistenceBootstrap: %v", err)
	}
	t.Cleanup(func() { _ = closer.Close() })

	runtimeCfg := config.NewDefaultRuntimeConfig()
	envCfg := newDefaultPlatformEnvConfig()
	envCfg.DefaultPlatformStickyTTL = 2 * time.Hour
	envCfg.DefaultPlatformRegexFilters = []string{`^Provider/.*`}
	envCfg.DefaultPlatformRegionFilters = []string{"us", "hk"}
	envCfg.DefaultPlatformReverseProxyMissAction = "REJECT"
	envCfg.DefaultPlatformReverseProxyEmptyAccountBehavior = "FIXED_HEADER"
	envCfg.DefaultPlatformReverseProxyFixedAccountHeader = "X-Account-Id"
	envCfg.DefaultPlatformAllocationPolicy = "PREFER_LOW_LATENCY"

	subManager, pool := newBootstrapTestRuntime(runtimeCfg)
	if err := bootstrapTopology(engine, subManager, pool, envCfg); err != nil {
		t.Fatalf("bootstrapTopology: %v", err)
	}

	platforms, err := engine.ListPlatforms()
	if err != nil {
		t.Fatalf("ListPlatforms: %v", err)
	}
	if len(platforms) != 1 {
		t.Fatalf("expected 1 platform, got %d", len(platforms))
	}

	defaultPlat := platforms[0]
	if defaultPlat.ID != platform.DefaultPlatformID {
		t.Fatalf("default id: got %q, want %q", defaultPlat.ID, platform.DefaultPlatformID)
	}
	if defaultPlat.Name != platform.DefaultPlatformName {
		t.Fatalf("default name: got %q, want %q", defaultPlat.Name, platform.DefaultPlatformName)
	}
	if defaultPlat.StickyTTLNs != int64(2*time.Hour) {
		t.Fatalf("sticky_ttl_ns: got %d, want %d", defaultPlat.StickyTTLNs, int64(2*time.Hour))
	}
	if defaultPlat.ReverseProxyMissAction != "REJECT" {
		t.Fatalf("reverse_proxy_miss_action: got %q, want %q", defaultPlat.ReverseProxyMissAction, "REJECT")
	}
	if defaultPlat.ReverseProxyEmptyAccountBehavior != "FIXED_HEADER" {
		t.Fatalf(
			"reverse_proxy_empty_account_behavior: got %q, want %q",
			defaultPlat.ReverseProxyEmptyAccountBehavior,
			"FIXED_HEADER",
		)
	}
	if defaultPlat.ReverseProxyFixedAccountHeader != "X-Account-Id" {
		t.Fatalf(
			"reverse_proxy_fixed_account_header: got %q, want %q",
			defaultPlat.ReverseProxyFixedAccountHeader,
			"X-Account-Id",
		)
	}
	if defaultPlat.AllocationPolicy != "PREFER_LOW_LATENCY" {
		t.Fatalf("allocation_policy: got %q, want %q", defaultPlat.AllocationPolicy, "PREFER_LOW_LATENCY")
	}

	if !reflect.DeepEqual(defaultPlat.RegexFilters, []string{`^Provider/.*`}) {
		t.Fatalf("regex_filters: got %v", defaultPlat.RegexFilters)
	}
	if !reflect.DeepEqual(defaultPlat.RegionFilters, []string{"us", "hk"}) {
		t.Fatalf("region_filters: got %v", defaultPlat.RegionFilters)
	}

	if _, ok := pool.GetPlatform(platform.DefaultPlatformID); !ok {
		t.Fatal("default platform should be registered in pool by ID")
	}
	if _, ok := pool.GetPlatformByName(platform.DefaultPlatformName); !ok {
		t.Fatal("default platform should be registered in pool by name")
	}
}

func TestBootstrapTopology_DefaultPlatformCreationIsIdempotent(t *testing.T) {
	engine, closer, err := state.PersistenceBootstrap(t.TempDir(), t.TempDir())
	if err != nil {
		t.Fatalf("PersistenceBootstrap: %v", err)
	}
	t.Cleanup(func() { _ = closer.Close() })

	runtimeCfg := config.NewDefaultRuntimeConfig()
	envCfg := newDefaultPlatformEnvConfig()
	subManager, pool := newBootstrapTestRuntime(runtimeCfg)

	if err := bootstrapTopology(engine, subManager, pool, envCfg); err != nil {
		t.Fatalf("first bootstrapTopology: %v", err)
	}
	if err := bootstrapTopology(engine, subManager, pool, envCfg); err != nil {
		t.Fatalf("second bootstrapTopology: %v", err)
	}

	platforms, err := engine.ListPlatforms()
	if err != nil {
		t.Fatalf("ListPlatforms: %v", err)
	}
	if len(platforms) != 1 {
		t.Fatalf("expected exactly 1 platform after repeated bootstrap, got %d", len(platforms))
	}
	if platforms[0].ID != platform.DefaultPlatformID {
		t.Fatalf("unexpected platform id after repeated bootstrap: %q", platforms[0].ID)
	}
}

func TestEnsureDefaultAccountHeaderRule_CreatesFallbackWhenMissing(t *testing.T) {
	engine, closer, err := state.PersistenceBootstrap(t.TempDir(), t.TempDir())
	if err != nil {
		t.Fatalf("PersistenceBootstrap: %v", err)
	}
	t.Cleanup(func() { _ = closer.Close() })

	if err := ensureDefaultAccountHeaderRule(engine); err != nil {
		t.Fatalf("ensureDefaultAccountHeaderRule: %v", err)
	}
	if err := ensureDefaultAccountHeaderRule(engine); err != nil {
		t.Fatalf("ensureDefaultAccountHeaderRule second call: %v", err)
	}

	rules, err := engine.ListAccountHeaderRules()
	if err != nil {
		t.Fatalf("ListAccountHeaderRules: %v", err)
	}
	if len(rules) != 1 {
		t.Fatalf("expected 1 fallback rule, got %d", len(rules))
	}
	if rules[0].URLPrefix != "*" {
		t.Fatalf("fallback url_prefix = %q, want %q", rules[0].URLPrefix, "*")
	}
	if !reflect.DeepEqual(rules[0].Headers, []string{"Authorization", "x-api-key"}) {
		t.Fatalf("fallback headers = %v, want %v", rules[0].Headers, []string{"Authorization", "x-api-key"})
	}
}

func TestEnsureDefaultAccountHeaderRule_DoesNotOverwriteExistingFallback(t *testing.T) {
	engine, closer, err := state.PersistenceBootstrap(t.TempDir(), t.TempDir())
	if err != nil {
		t.Fatalf("PersistenceBootstrap: %v", err)
	}
	t.Cleanup(func() { _ = closer.Close() })

	custom := model.AccountHeaderRule{
		URLPrefix:   "*",
		Headers:     []string{"X-Custom-Account"},
		UpdatedAtNs: time.Now().UnixNano(),
	}
	if _, err := engine.UpsertAccountHeaderRuleWithCreated(custom); err != nil {
		t.Fatalf("seed fallback rule: %v", err)
	}

	if err := ensureDefaultAccountHeaderRule(engine); err != nil {
		t.Fatalf("ensureDefaultAccountHeaderRule: %v", err)
	}

	rules, err := engine.ListAccountHeaderRules()
	if err != nil {
		t.Fatalf("ListAccountHeaderRules: %v", err)
	}
	if len(rules) != 1 {
		t.Fatalf("expected 1 fallback rule, got %d", len(rules))
	}
	if !reflect.DeepEqual(rules[0].Headers, custom.Headers) {
		t.Fatalf("fallback headers should stay custom, got %v, want %v", rules[0].Headers, custom.Headers)
	}
}

func TestBootstrapTopology_DefaultPlatformByNameDoesNotSatisfyDefaultID(t *testing.T) {
	engine, closer, err := state.PersistenceBootstrap(t.TempDir(), t.TempDir())
	if err != nil {
		t.Fatalf("PersistenceBootstrap: %v", err)
	}
	t.Cleanup(func() { _ = closer.Close() })

	now := time.Now().UnixNano()
	if err := engine.UpsertPlatform(model.Platform{
		ID:                     "legacy-default-id",
		Name:                   platform.DefaultPlatformName,
		StickyTTLNs:            int64(time.Hour),
		RegexFilters:           []string{},
		RegionFilters:          []string{},
		ReverseProxyMissAction: "TREAT_AS_EMPTY",
		AllocationPolicy:       "BALANCED",
		UpdatedAtNs:            now,
	}); err != nil {
		t.Fatalf("seed legacy default-by-name platform: %v", err)
	}

	subManager, pool := newBootstrapTestRuntime(config.NewDefaultRuntimeConfig())
	err = bootstrapTopology(engine, subManager, pool, newDefaultPlatformEnvConfig())
	if err == nil {
		t.Fatal("expected bootstrapTopology to fail when default ID is missing but default name is occupied")
	}
	if !strings.Contains(err.Error(), "ensure default platform") {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(err.Error(), "platform name already exists") {
		t.Fatalf("unexpected error detail: %v", err)
	}
}

func TestBootstrapTopology_FailsFastOnCorruptPlatformFilters(t *testing.T) {
	root := t.TempDir()
	stateDir := filepath.Join(root, "state")
	cacheDir := filepath.Join(root, "cache")

	engine, closer, err := state.PersistenceBootstrap(stateDir, cacheDir)
	if err != nil {
		t.Fatalf("PersistenceBootstrap: %v", err)
	}
	t.Cleanup(func() { _ = closer.Close() })

	now := time.Now().UnixNano()
	if err := engine.UpsertPlatform(model.Platform{
		ID:                     "plat-1",
		Name:                   "BrokenOnRead",
		StickyTTLNs:            int64(time.Hour),
		RegexFilters:           []string{`^ok$`},
		RegionFilters:          []string{"us"},
		ReverseProxyMissAction: "TREAT_AS_EMPTY",
		AllocationPolicy:       "BALANCED",
		UpdatedAtNs:            now,
	}); err != nil {
		t.Fatalf("UpsertPlatform: %v", err)
	}

	db, err := state.OpenDB(filepath.Join(stateDir, "state.db"))
	if err != nil {
		t.Fatalf("OpenDB(state.db): %v", err)
	}
	defer db.Close()
	if _, err := db.Exec(
		`UPDATE platforms SET regex_filters_json = ? WHERE id = ?`,
		`["(broken"]`,
		"plat-1",
	); err != nil {
		t.Fatalf("corrupt platform row: %v", err)
	}

	subManager, pool := newBootstrapTestRuntime(config.NewDefaultRuntimeConfig())
	err = bootstrapTopology(engine, subManager, pool, newDefaultPlatformEnvConfig())
	if err == nil {
		t.Fatal("expected bootstrapTopology to fail on corrupt platform filters")
	}
	if !strings.Contains(err.Error(), "regex_filters") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestBootstrapTopology_V1RejectsPersistedInvalidPlatformName(t *testing.T) {
	root := t.TempDir()
	stateDir := filepath.Join(root, "state")
	cacheDir := filepath.Join(root, "cache")

	engine, closer, err := state.PersistenceBootstrap(stateDir, cacheDir)
	if err != nil {
		t.Fatalf("PersistenceBootstrap: %v", err)
	}
	t.Cleanup(func() { _ = closer.Close() })

	now := time.Now().UnixNano()
	if err := engine.UpsertPlatform(model.Platform{
		ID:                     "plat-1",
		Name:                   "LegacyPlatform",
		StickyTTLNs:            int64(time.Hour),
		RegexFilters:           []string{},
		RegionFilters:          []string{},
		ReverseProxyMissAction: "TREAT_AS_EMPTY",
		AllocationPolicy:       "BALANCED",
		UpdatedAtNs:            now,
	}); err != nil {
		t.Fatalf("UpsertPlatform: %v", err)
	}

	db, err := state.OpenDB(filepath.Join(stateDir, "state.db"))
	if err != nil {
		t.Fatalf("OpenDB(state.db): %v", err)
	}
	defer db.Close()
	if _, err := db.Exec(`UPDATE platforms SET name = ? WHERE id = ?`, "legacy:bad", "plat-1"); err != nil {
		t.Fatalf("corrupt platform name row: %v", err)
	}

	subManager, pool := newBootstrapTestRuntime(config.NewDefaultRuntimeConfig())
	envCfg := newDefaultPlatformEnvConfig()
	envCfg.AuthVersion = config.AuthVersionV1

	err = bootstrapTopology(engine, subManager, pool, envCfg)
	if err == nil {
		t.Fatal("expected bootstrapTopology to fail when V1 detects invalid persisted platform name")
	}
	if !strings.Contains(err.Error(), "RESIN_AUTH_VERSION=V1") {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(err.Error(), config.AuthMigrationGuideURL) {
		t.Fatalf("expected migration guide link in error, got: %v", err)
	}
}

func TestBootstrapTopology_V1RejectsPersistedPlatformNameWithLeadingSpace(t *testing.T) {
	root := t.TempDir()
	stateDir := filepath.Join(root, "state")
	cacheDir := filepath.Join(root, "cache")

	engine, closer, err := state.PersistenceBootstrap(stateDir, cacheDir)
	if err != nil {
		t.Fatalf("PersistenceBootstrap: %v", err)
	}
	t.Cleanup(func() { _ = closer.Close() })

	now := time.Now().UnixNano()
	if err := engine.UpsertPlatform(model.Platform{
		ID:                     "plat-1",
		Name:                   "LegacyPlatform",
		StickyTTLNs:            int64(time.Hour),
		RegexFilters:           []string{},
		RegionFilters:          []string{},
		ReverseProxyMissAction: "TREAT_AS_EMPTY",
		AllocationPolicy:       "BALANCED",
		UpdatedAtNs:            now,
	}); err != nil {
		t.Fatalf("UpsertPlatform: %v", err)
	}

	db, err := state.OpenDB(filepath.Join(stateDir, "state.db"))
	if err != nil {
		t.Fatalf("OpenDB(state.db): %v", err)
	}
	defer db.Close()
	if _, err := db.Exec(`UPDATE platforms SET name = ? WHERE id = ?`, " legacy-space", "plat-1"); err != nil {
		t.Fatalf("corrupt platform name row: %v", err)
	}

	subManager, pool := newBootstrapTestRuntime(config.NewDefaultRuntimeConfig())
	envCfg := newDefaultPlatformEnvConfig()
	envCfg.AuthVersion = config.AuthVersionV1

	err = bootstrapTopology(engine, subManager, pool, envCfg)
	if err == nil {
		t.Fatal("expected bootstrapTopology to fail when V1 detects leading space in persisted platform name")
	}
	if !strings.Contains(err.Error(), "RESIN_AUTH_VERSION=V1") {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(err.Error(), config.AuthMigrationGuideURL) {
		t.Fatalf("expected migration guide link in error, got: %v", err)
	}
}

func TestBootstrapTopology_V1RejectsPersistedReservedPlatformNameAPI(t *testing.T) {
	root := t.TempDir()
	stateDir := filepath.Join(root, "state")
	cacheDir := filepath.Join(root, "cache")

	engine, closer, err := state.PersistenceBootstrap(stateDir, cacheDir)
	if err != nil {
		t.Fatalf("PersistenceBootstrap: %v", err)
	}
	t.Cleanup(func() { _ = closer.Close() })

	now := time.Now().UnixNano()
	if err := engine.UpsertPlatform(model.Platform{
		ID:                     "plat-1",
		Name:                   "LegacyPlatform",
		StickyTTLNs:            int64(time.Hour),
		RegexFilters:           []string{},
		RegionFilters:          []string{},
		ReverseProxyMissAction: "TREAT_AS_EMPTY",
		AllocationPolicy:       "BALANCED",
		UpdatedAtNs:            now,
	}); err != nil {
		t.Fatalf("UpsertPlatform: %v", err)
	}

	db, err := state.OpenDB(filepath.Join(stateDir, "state.db"))
	if err != nil {
		t.Fatalf("OpenDB(state.db): %v", err)
	}
	defer db.Close()
	if _, err := db.Exec(`UPDATE platforms SET name = ? WHERE id = ?`, "api", "plat-1"); err != nil {
		t.Fatalf("corrupt platform name row: %v", err)
	}

	subManager, pool := newBootstrapTestRuntime(config.NewDefaultRuntimeConfig())
	envCfg := newDefaultPlatformEnvConfig()
	envCfg.AuthVersion = config.AuthVersionV1

	err = bootstrapTopology(engine, subManager, pool, envCfg)
	if err == nil {
		t.Fatal("expected bootstrapTopology to fail when V1 detects reserved platform name api")
	}
	if !strings.Contains(err.Error(), "RESIN_AUTH_VERSION=V1") {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(err.Error(), config.AuthMigrationGuideURL) {
		t.Fatalf("expected migration guide link in error, got: %v", err)
	}
}

func TestBootstrapTopology_V1RejectsAllPersistedInvalidPlatformNames(t *testing.T) {
	root := t.TempDir()
	stateDir := filepath.Join(root, "state")
	cacheDir := filepath.Join(root, "cache")

	engine, closer, err := state.PersistenceBootstrap(stateDir, cacheDir)
	if err != nil {
		t.Fatalf("PersistenceBootstrap: %v", err)
	}
	t.Cleanup(func() { _ = closer.Close() })

	now := time.Now().UnixNano()
	for _, p := range []model.Platform{
		{
			ID:                     "plat-1",
			Name:                   "LegacyPlatformOne",
			StickyTTLNs:            int64(time.Hour),
			RegexFilters:           []string{},
			RegionFilters:          []string{},
			ReverseProxyMissAction: "TREAT_AS_EMPTY",
			AllocationPolicy:       "BALANCED",
			UpdatedAtNs:            now,
		},
		{
			ID:                     "plat-2",
			Name:                   "LegacyPlatformTwo",
			StickyTTLNs:            int64(time.Hour),
			RegexFilters:           []string{},
			RegionFilters:          []string{},
			ReverseProxyMissAction: "TREAT_AS_EMPTY",
			AllocationPolicy:       "BALANCED",
			UpdatedAtNs:            now,
		},
	} {
		if err := engine.UpsertPlatform(p); err != nil {
			t.Fatalf("UpsertPlatform(%s): %v", p.ID, err)
		}
	}

	db, err := state.OpenDB(filepath.Join(stateDir, "state.db"))
	if err != nil {
		t.Fatalf("OpenDB(state.db): %v", err)
	}
	defer db.Close()
	if _, err := db.Exec(`UPDATE platforms SET name = ? WHERE id = ?`, "legacy:bad-one", "plat-1"); err != nil {
		t.Fatalf("corrupt platform name row (plat-1): %v", err)
	}
	if _, err := db.Exec(`UPDATE platforms SET name = ? WHERE id = ?`, "api", "plat-2"); err != nil {
		t.Fatalf("corrupt platform name row (plat-2): %v", err)
	}

	subManager, pool := newBootstrapTestRuntime(config.NewDefaultRuntimeConfig())
	envCfg := newDefaultPlatformEnvConfig()
	envCfg.AuthVersion = config.AuthVersionV1

	err = bootstrapTopology(engine, subManager, pool, envCfg)
	if err == nil {
		t.Fatal("expected bootstrapTopology to fail when V1 detects multiple invalid persisted platform names")
	}
	if !strings.Contains(err.Error(), "2 platform(s) are incompatible with RESIN_AUTH_VERSION=V1") {
		t.Fatalf("unexpected error summary: %v", err)
	}
	if !strings.Contains(err.Error(), "\"legacy:bad-one\"") || !strings.Contains(err.Error(), "\"api\"") {
		t.Fatalf("expected all invalid platform names in error, got: %v", err)
	}
	if !strings.Contains(err.Error(), "Platform name rules:") {
		t.Fatalf("expected platform-name rules in error, got: %v", err)
	}
	if strings.Contains(err.Error(), "\"legacy:bad-one\":") || strings.Contains(err.Error(), "\"api\":") {
		t.Fatalf("error should list invalid platform names without per-platform reason details, got: %v", err)
	}
	if !strings.Contains(err.Error(), config.AuthMigrationGuideURL) {
		t.Fatalf("expected migration guide link in error, got: %v", err)
	}
}

func TestBootstrapNodes_MissingDynamicStaysColdInCatalog(t *testing.T) {
	engine, closer, err := state.PersistenceBootstrap(t.TempDir(), t.TempDir())
	if err != nil {
		t.Fatalf("PersistenceBootstrap: %v", err)
	}
	t.Cleanup(func() { _ = closer.Close() })

	const subID = "sub-bootstrap-missing-dynamic"
	now := time.Now().UnixNano()
	if err := engine.UpsertSubscription(model.Subscription{
		ID:               subID,
		Name:             "BootstrapSub",
		URL:              "https://example.com/sub",
		UpdateIntervalNs: int64(30 * time.Minute),
		Enabled:          true,
		Ephemeral:        false,
		CreatedAtNs:      now,
		UpdatedAtNs:      now,
	}); err != nil {
		t.Fatalf("UpsertSubscription: %v", err)
	}

	raw := json.RawMessage(`{"type":"stub","server":"198.51.100.77","server_port":443}`)
	hash := node.HashFromRawOptions(raw)
	if err := engine.BulkUpsertNodesStatic([]model.NodeStatic{{
		Hash:        hash.Hex(),
		RawOptions:  raw,
		CreatedAtNs: now - int64(time.Hour),
	}}); err != nil {
		t.Fatalf("BulkUpsertNodesStatic: %v", err)
	}
	if err := engine.BulkUpsertSubscriptionNodes([]model.SubscriptionNode{{
		SubscriptionID: subID,
		NodeHash:       hash.Hex(),
		Tags:           []string{"bootstrap-tag"},
	}}); err != nil {
		t.Fatalf("BulkUpsertSubscriptionNodes: %v", err)
	}

	runtimeCfg := config.NewDefaultRuntimeConfig()
	envCfg := newDefaultPlatformEnvConfig()
	envCfg.MaxLatencyTableEntries = 16
	subManager, pool := newBootstrapTestRuntime(runtimeCfg)

	if err := bootstrapTopology(engine, subManager, pool, envCfg); err != nil {
		t.Fatalf("bootstrapTopology: %v", err)
	}

	outboundMgr := outbound.NewOutboundManager(pool, &testutil.StubOutboundBuilder{})
	if err := bootstrapNodes(engine, pool, subManager, outboundMgr, envCfg, runtimeCfg.LatencyAuthorities); err != nil {
		t.Fatalf("bootstrapNodes: %v", err)
	}

	if _, ok := pool.GetEntry(hash); ok {
		t.Fatalf("node %s without dynamic/latency evidence should stay cold after bootstrapNodes", hash.Hex())
	}
	rows, err := engine.LoadSubscriptionNodes(subID)
	if err != nil {
		t.Fatalf("LoadSubscriptionNodes: %v", err)
	}
	if len(rows) != 1 || rows[0].NodeHash != hash.Hex() {
		t.Fatalf("cold catalog relation missing after bootstrapNodes, got %+v want %s", rows, hash.Hex())
	}
}

func TestBootstrapNodes_CircuitClosedWithoutLatencyStaysColdInCatalog(t *testing.T) {
	engine, closer, err := state.PersistenceBootstrap(t.TempDir(), t.TempDir())
	if err != nil {
		t.Fatalf("PersistenceBootstrap: %v", err)
	}
	t.Cleanup(func() { _ = closer.Close() })

	const subID = "sub-bootstrap-with-dynamic"
	now := time.Now().UnixNano()
	if err := engine.UpsertSubscription(model.Subscription{
		ID:               subID,
		Name:             "BootstrapSub",
		URL:              "https://example.com/sub",
		UpdateIntervalNs: int64(30 * time.Minute),
		Enabled:          true,
		Ephemeral:        false,
		CreatedAtNs:      now,
		UpdatedAtNs:      now,
	}); err != nil {
		t.Fatalf("UpsertSubscription: %v", err)
	}

	raw := json.RawMessage(`{"type":"stub","server":"198.51.100.88","server_port":443}`)
	hash := node.HashFromRawOptions(raw)
	if err := engine.BulkUpsertNodesStatic([]model.NodeStatic{{
		Hash:        hash.Hex(),
		RawOptions:  raw,
		CreatedAtNs: now - int64(time.Hour),
	}}); err != nil {
		t.Fatalf("BulkUpsertNodesStatic: %v", err)
	}
	if err := engine.BulkUpsertSubscriptionNodes([]model.SubscriptionNode{{
		SubscriptionID: subID,
		NodeHash:       hash.Hex(),
		Tags:           []string{"bootstrap-tag"},
	}}); err != nil {
		t.Fatalf("BulkUpsertSubscriptionNodes: %v", err)
	}
	if err := engine.BulkUpsertNodesDynamic([]model.NodeDynamic{{
		Hash:             hash.Hex(),
		FailureCount:     0,
		CircuitOpenSince: 0,
	}}); err != nil {
		t.Fatalf("BulkUpsertNodesDynamic: %v", err)
	}

	runtimeCfg := config.NewDefaultRuntimeConfig()
	envCfg := newDefaultPlatformEnvConfig()
	envCfg.MaxLatencyTableEntries = 16
	subManager, pool := newBootstrapTestRuntime(runtimeCfg)

	if err := bootstrapTopology(engine, subManager, pool, envCfg); err != nil {
		t.Fatalf("bootstrapTopology: %v", err)
	}

	outboundMgr := outbound.NewOutboundManager(pool, &testutil.StubOutboundBuilder{})
	if err := bootstrapNodes(engine, pool, subManager, outboundMgr, envCfg, runtimeCfg.LatencyAuthorities); err != nil {
		t.Fatalf("bootstrapNodes: %v", err)
	}

	if _, ok := pool.GetEntry(hash); ok {
		t.Fatalf("node %s without durable latency evidence should stay cold after bootstrapNodes", hash.Hex())
	}
	rows, err := engine.LoadSubscriptionNodes(subID)
	if err != nil {
		t.Fatalf("LoadSubscriptionNodes: %v", err)
	}
	if len(rows) != 1 || rows[0].NodeHash != hash.Hex() {
		t.Fatalf("cold catalog relation missing after bootstrapNodes, got %+v want %s", rows, hash.Hex())
	}
}

func TestBootstrapNodes_RestoreEvictedSubscriptionNodeWithoutPoolRef(t *testing.T) {
	engine, closer, err := state.PersistenceBootstrap(t.TempDir(), t.TempDir())
	if err != nil {
		t.Fatalf("PersistenceBootstrap: %v", err)
	}
	t.Cleanup(func() { _ = closer.Close() })

	const subID = "sub-bootstrap-evicted"
	now := time.Now().UnixNano()
	if err := engine.UpsertSubscription(model.Subscription{
		ID:               subID,
		Name:             "BootstrapSub",
		URL:              "https://example.com/sub",
		UpdateIntervalNs: int64(30 * time.Minute),
		Enabled:          true,
		Ephemeral:        false,
		CreatedAtNs:      now,
		UpdatedAtNs:      now,
	}); err != nil {
		t.Fatalf("UpsertSubscription: %v", err)
	}

	raw := []byte(`{"type":"stub","server":"198.51.100.199","server_port":443}`)
	hash := node.HashFromRawOptions(raw)
	if err := engine.BulkUpsertSubscriptionNodes([]model.SubscriptionNode{{
		SubscriptionID: subID,
		NodeHash:       hash.Hex(),
		Tags:           []string{"evicted-tag"},
		Evicted:        true,
	}}); err != nil {
		t.Fatalf("BulkUpsertSubscriptionNodes: %v", err)
	}

	runtimeCfg := config.NewDefaultRuntimeConfig()
	envCfg := newDefaultPlatformEnvConfig()
	envCfg.MaxLatencyTableEntries = 16
	subManager, pool := newBootstrapTestRuntime(runtimeCfg)

	if err := bootstrapTopology(engine, subManager, pool, envCfg); err != nil {
		t.Fatalf("bootstrapTopology: %v", err)
	}

	outboundMgr := outbound.NewOutboundManager(pool, &testutil.StubOutboundBuilder{})
	if err := bootstrapNodes(engine, pool, subManager, outboundMgr, envCfg, runtimeCfg.LatencyAuthorities); err != nil {
		t.Fatalf("bootstrapNodes: %v", err)
	}

	sub, ok := subManager.Get(subID)
	if !ok {
		t.Fatalf("subscription %q missing after bootstrap", subID)
	}
	if _, ok := sub.ManagedNodes().LoadNode(hash); ok {
		t.Fatalf("evicted subscription node %s should stay out of active managed view", hash.Hex())
	}
	rows, err := engine.LoadSubscriptionNodes(subID)
	if err != nil {
		t.Fatalf("LoadSubscriptionNodes: %v", err)
	}
	if len(rows) != 1 || rows[0].NodeHash != hash.Hex() || !rows[0].Evicted {
		t.Fatalf("evicted catalog relation not preserved, got %+v want evicted %s", rows, hash.Hex())
	}
	if _, ok := pool.GetEntry(hash); ok {
		t.Fatal("evicted subscription node should not restore subscription hold in pool")
	}
}

func TestBootstrapNodes_ActiveOnlyRuntimeSelectsEnabledNonEvictedCircuitClosedWithLatency(t *testing.T) {
	engine, closer, err := state.PersistenceBootstrap(t.TempDir(), t.TempDir())
	if err != nil {
		t.Fatalf("PersistenceBootstrap: %v", err)
	}
	t.Cleanup(func() { _ = closer.Close() })

	const (
		enabledSubID  = "sub-active-enabled"
		disabledSubID = "sub-active-disabled"
	)
	now := time.Now().UnixNano()
	for _, sub := range []model.Subscription{
		{
			ID:               enabledSubID,
			Name:             "BootstrapActive",
			URL:              "https://example.com/enabled",
			UpdateIntervalNs: int64(30 * time.Minute),
			Enabled:          true,
			LastError:        "previous refresh failure must not filter active bootstrap",
			CreatedAtNs:      now,
			UpdatedAtNs:      now,
		},
		{
			ID:               disabledSubID,
			Name:             "BootstrapDisabled",
			URL:              "https://example.com/disabled",
			UpdateIntervalNs: int64(30 * time.Minute),
			Enabled:          false,
			CreatedAtNs:      now,
			UpdatedAtNs:      now,
		},
	} {
		if err := engine.UpsertSubscription(sub); err != nil {
			t.Fatalf("UpsertSubscription(%s): %v", sub.ID, err)
		}
	}

	rawActive := json.RawMessage(`{"type":"stub","server":"198.51.100.10","server_port":443}`)
	rawDisabled := json.RawMessage(`{"type":"stub","server":"198.51.100.11","server_port":443}`)
	rawEvicted := json.RawMessage(`{"type":"stub","server":"198.51.100.12","server_port":443}`)
	rawCircuitOpen := json.RawMessage(`{"type":"stub","server":"198.51.100.13","server_port":443}`)
	rawAttemptOnly := json.RawMessage(`{"type":"stub","server":"198.51.100.14","server_port":443}`)
	rawBuildFail := json.RawMessage(`{"type":"stub","server":"198.51.100.15","server_port":443}`)
	hashByRaw := map[string]node.Hash{
		string(rawActive):      node.HashFromRawOptions(rawActive),
		string(rawDisabled):    node.HashFromRawOptions(rawDisabled),
		string(rawEvicted):     node.HashFromRawOptions(rawEvicted),
		string(rawCircuitOpen): node.HashFromRawOptions(rawCircuitOpen),
		string(rawAttemptOnly): node.HashFromRawOptions(rawAttemptOnly),
		string(rawBuildFail):   node.HashFromRawOptions(rawBuildFail),
	}

	var statics []model.NodeStatic
	for rawString, hash := range hashByRaw {
		statics = append(statics, model.NodeStatic{
			Hash:        hash.Hex(),
			RawOptions:  json.RawMessage(rawString),
			CreatedAtNs: now - int64(time.Hour),
		})
	}
	if err := engine.BulkUpsertNodesStatic(statics); err != nil {
		t.Fatalf("BulkUpsertNodesStatic: %v", err)
	}
	if err := engine.BulkUpsertSubscriptionNodes([]model.SubscriptionNode{
		{SubscriptionID: enabledSubID, NodeHash: hashByRaw[string(rawActive)].Hex(), Tags: []string{"active"}},
		{SubscriptionID: disabledSubID, NodeHash: hashByRaw[string(rawDisabled)].Hex(), Tags: []string{"disabled"}},
		{SubscriptionID: enabledSubID, NodeHash: hashByRaw[string(rawEvicted)].Hex(), Tags: []string{"evicted"}, Evicted: true},
		{SubscriptionID: enabledSubID, NodeHash: hashByRaw[string(rawCircuitOpen)].Hex(), Tags: []string{"circuit-open"}},
		{SubscriptionID: enabledSubID, NodeHash: hashByRaw[string(rawAttemptOnly)].Hex(), Tags: []string{"attempt-only"}},
		{SubscriptionID: enabledSubID, NodeHash: hashByRaw[string(rawBuildFail)].Hex(), Tags: []string{"build-fail"}},
	}); err != nil {
		t.Fatalf("BulkUpsertSubscriptionNodes: %v", err)
	}
	if err := engine.BulkUpsertNodesDynamic([]model.NodeDynamic{
		{Hash: hashByRaw[string(rawActive)].Hex(), CircuitOpenSince: 0, EgressIP: ""},
		{Hash: hashByRaw[string(rawDisabled)].Hex(), CircuitOpenSince: 0},
		{Hash: hashByRaw[string(rawEvicted)].Hex(), CircuitOpenSince: 0},
		{Hash: hashByRaw[string(rawCircuitOpen)].Hex(), CircuitOpenSince: now - int64(time.Minute)},
		{Hash: hashByRaw[string(rawAttemptOnly)].Hex(), CircuitOpenSince: 0, LastLatencyProbeAttemptNs: now - int64(time.Second)},
		{Hash: hashByRaw[string(rawBuildFail)].Hex(), CircuitOpenSince: 0},
	}); err != nil {
		t.Fatalf("BulkUpsertNodesDynamic: %v", err)
	}
	if err := engine.BulkUpsertNodeLatency([]model.NodeLatency{
		{NodeHash: hashByRaw[string(rawActive)].Hex(), Domain: "example.com", EwmaNs: int64(42 * time.Millisecond), LastUpdatedNs: now},
		{NodeHash: hashByRaw[string(rawDisabled)].Hex(), Domain: "example.com", EwmaNs: int64(43 * time.Millisecond), LastUpdatedNs: now},
		{NodeHash: hashByRaw[string(rawEvicted)].Hex(), Domain: "example.com", EwmaNs: int64(44 * time.Millisecond), LastUpdatedNs: now},
		{NodeHash: hashByRaw[string(rawCircuitOpen)].Hex(), Domain: "example.com", EwmaNs: int64(45 * time.Millisecond), LastUpdatedNs: now},
		{NodeHash: hashByRaw[string(rawBuildFail)].Hex(), Domain: "example.com", EwmaNs: int64(46 * time.Millisecond), LastUpdatedNs: now},
	}); err != nil {
		t.Fatalf("BulkUpsertNodeLatency: %v", err)
	}

	runtimeCfg := config.NewDefaultRuntimeConfig()
	envCfg := newDefaultPlatformEnvConfig()
	envCfg.MaxLatencyTableEntries = 16
	subManager, pool := newBootstrapTestRuntime(runtimeCfg)

	if err := bootstrapTopology(engine, subManager, pool, envCfg); err != nil {
		t.Fatalf("bootstrapTopology: %v", err)
	}

	builder := &trackingBootstrapBuilder{
		failRaw: map[string]bool{string(rawBuildFail): true},
	}
	outboundMgr := outbound.NewOutboundManager(pool, builder)
	if err := bootstrapNodes(engine, pool, subManager, outboundMgr, envCfg, runtimeCfg.LatencyAuthorities); err != nil {
		t.Fatalf("bootstrapNodes: %v", err)
	}

	activeHash := hashByRaw[string(rawActive)]
	entry, ok := pool.GetEntry(activeHash)
	if !ok {
		t.Fatalf("active node %s missing after active-only bootstrap", activeHash.Hex())
	}
	if !entry.HasOutbound() {
		t.Fatal("active node should have outbound after active-only bootstrap")
	}
	if entry.IsCircuitOpen() {
		t.Fatal("persisted circuit-closed dynamic state should be restored")
	}
	if entry.GetEgressIP().IsValid() {
		t.Fatal("test setup should prove empty egress_ip did not prevent active bootstrap")
	}
	if !entry.HasLatency() {
		t.Fatal("active node should restore durable latency sample")
	}

	for rawString, hash := range hashByRaw {
		if hash == activeHash {
			continue
		}
		if _, ok := pool.GetEntry(hash); ok {
			t.Fatalf("node %s (%s) should not be active after active-only bootstrap", rawString, hash.Hex())
		}
	}
	if got := builder.built; !sameStringSet(got, []string{string(rawActive), string(rawBuildFail)}) {
		t.Fatalf("outbound build candidates: got %v, want active and build-fail only", got)
	}

	sub, ok := subManager.Get(enabledSubID)
	if !ok {
		t.Fatalf("subscription %s missing", enabledSubID)
	}
	if _, ok := sub.ManagedNodes().LoadNode(activeHash); !ok {
		t.Fatal("active node relation should be restored into subscription managed nodes")
	}
	for _, raw := range []json.RawMessage{rawEvicted, rawCircuitOpen, rawAttemptOnly, rawBuildFail} {
		hash := hashByRaw[string(raw)]
		if _, ok := sub.ManagedNodes().LoadNode(hash); ok {
			t.Fatalf("inactive node %s should not be restored into active managed nodes", hash.Hex())
		}
	}
}

func TestBootstrapNodes_ActiveOnlyRuntimeExcludesAttemptOnlyWithoutLatencySample(t *testing.T) {
	engine, closer, err := state.PersistenceBootstrap(t.TempDir(), t.TempDir())
	if err != nil {
		t.Fatalf("PersistenceBootstrap: %v", err)
	}
	t.Cleanup(func() { _ = closer.Close() })

	const subID = "sub-active-attempt-only"
	now := time.Now().UnixNano()
	if err := engine.UpsertSubscription(model.Subscription{
		ID:               subID,
		Name:             "BootstrapAttemptOnly",
		URL:              "https://example.com/sub",
		UpdateIntervalNs: int64(30 * time.Minute),
		Enabled:          true,
		CreatedAtNs:      now,
		UpdatedAtNs:      now,
	}); err != nil {
		t.Fatalf("UpsertSubscription: %v", err)
	}

	raw := json.RawMessage(`{"type":"stub","server":"198.51.100.16","server_port":443}`)
	hash := node.HashFromRawOptions(raw)
	if err := engine.BulkUpsertNodesStatic([]model.NodeStatic{{
		Hash:        hash.Hex(),
		RawOptions:  raw,
		CreatedAtNs: now - int64(time.Hour),
	}}); err != nil {
		t.Fatalf("BulkUpsertNodesStatic: %v", err)
	}
	if err := engine.BulkUpsertSubscriptionNodes([]model.SubscriptionNode{{
		SubscriptionID: subID,
		NodeHash:       hash.Hex(),
		Tags:           []string{"attempt-only"},
	}}); err != nil {
		t.Fatalf("BulkUpsertSubscriptionNodes: %v", err)
	}
	if err := engine.BulkUpsertNodesDynamic([]model.NodeDynamic{{
		Hash:                      hash.Hex(),
		CircuitOpenSince:          0,
		LastLatencyProbeAttemptNs: now - int64(time.Second),
	}}); err != nil {
		t.Fatalf("BulkUpsertNodesDynamic: %v", err)
	}

	runtimeCfg := config.NewDefaultRuntimeConfig()
	envCfg := newDefaultPlatformEnvConfig()
	envCfg.MaxLatencyTableEntries = 16
	subManager, pool := newBootstrapTestRuntime(runtimeCfg)
	if err := bootstrapTopology(engine, subManager, pool, envCfg); err != nil {
		t.Fatalf("bootstrapTopology: %v", err)
	}

	builder := &trackingBootstrapBuilder{}
	outboundMgr := outbound.NewOutboundManager(pool, builder)
	if err := bootstrapNodes(engine, pool, subManager, outboundMgr, envCfg, runtimeCfg.LatencyAuthorities); err != nil {
		t.Fatalf("bootstrapNodes: %v", err)
	}
	if _, ok := pool.GetEntry(hash); ok {
		t.Fatal("attempt-only node should not enter active-only runtime memory")
	}
	if len(builder.built) != 0 {
		t.Fatalf("attempt-only node should not get outbound build, got builds=%v", builder.built)
	}
}

func TestColdSubscriptionNodeCheck_PromotesOnLatencySuccessAndPersists(t *testing.T) {
	engine, closer, err := state.PersistenceBootstrap(t.TempDir(), t.TempDir())
	if err != nil {
		t.Fatalf("PersistenceBootstrap: %v", err)
	}
	t.Cleanup(func() { _ = closer.Close() })

	const subID = "sub-cold-success"
	now := time.Now().UnixNano()
	if err := engine.UpsertSubscription(model.Subscription{
		ID:               subID,
		Name:             "ColdSuccess",
		URL:              "https://example.com/sub",
		UpdateIntervalNs: int64(30 * time.Minute),
		Enabled:          true,
		CreatedAtNs:      now,
		UpdatedAtNs:      now,
	}); err != nil {
		t.Fatalf("UpsertSubscription: %v", err)
	}

	runtimeCfg := config.NewDefaultRuntimeConfig()
	subManager, pool := newColdCheckTestRuntime(engine, runtimeCfg)
	if err := bootstrapTopology(engine, subManager, pool, newDefaultPlatformEnvConfig()); err != nil {
		t.Fatalf("bootstrapTopology: %v", err)
	}

	raw := json.RawMessage(`{"type":"stub","server":"198.51.100.70","server_port":443}`)
	hash := node.HashFromRawOptions(raw)
	candidate := topology.ColdNodeCandidate{
		SubscriptionID: subID,
		Hash:           hash,
		RawOptions:     raw,
		Tags:           []string{"cold-success"},
	}
	checker := newColdSubscriptionNodeChecker(engine, pool, subManager, &testutil.StubOutboundBuilder{}, func(hash node.Hash) error {
		pool.RecordResult(hash, true)
		latency := 25 * time.Millisecond
		pool.RecordLatency(hash, "example.com", &latency)
		return nil
	})

	checker.Check(candidate)

	entry, ok := pool.GetEntry(hash)
	if !ok {
		t.Fatal("successful cold check should promote node into memory")
	}
	if !entry.HasOutbound() {
		t.Fatal("promoted cold node should have outbound")
	}
	if entry.IsCircuitOpen() {
		t.Fatal("successful cold check should close circuit")
	}
	if !entry.HasLatency() {
		t.Fatal("successful cold check should record latency")
	}
	sub, ok := subManager.Get(subID)
	if !ok {
		t.Fatalf("subscription %s missing", subID)
	}
	managed, ok := sub.ManagedNodes().LoadNode(hash)
	if !ok {
		t.Fatal("successful cold check should add active subscription relation")
	}
	if !reflect.DeepEqual(managed.Tags, []string{"cold-success"}) {
		t.Fatalf("managed tags: got %v, want [cold-success]", managed.Tags)
	}

	if err := engine.FlushDirtySets(newFlushReaders(pool, subManager, nil)); err != nil {
		t.Fatalf("FlushDirtySets: %v", err)
	}
	statics, err := engine.LoadAllNodesStatic()
	if err != nil {
		t.Fatalf("LoadAllNodesStatic: %v", err)
	}
	if len(statics) != 1 || statics[0].Hash != hash.Hex() {
		t.Fatalf("persisted statics: got %+v, want %s", statics, hash.Hex())
	}
	dynamics, err := engine.LoadAllNodesDynamic()
	if err != nil {
		t.Fatalf("LoadAllNodesDynamic: %v", err)
	}
	if len(dynamics) != 1 || dynamics[0].Hash != hash.Hex() || dynamics[0].CircuitOpenSince != 0 {
		t.Fatalf("persisted dynamics: got %+v, want circuit closed row for %s", dynamics, hash.Hex())
	}
	latencies, err := engine.LoadAllNodeLatency()
	if err != nil {
		t.Fatalf("LoadAllNodeLatency: %v", err)
	}
	if len(latencies) != 1 || latencies[0].NodeHash != hash.Hex() || latencies[0].EwmaNs <= 0 {
		t.Fatalf("persisted latency: got %+v, want positive EWMA for %s", latencies, hash.Hex())
	}
	subNodes, err := engine.LoadAllSubscriptionNodes()
	if err != nil {
		t.Fatalf("LoadAllSubscriptionNodes: %v", err)
	}
	if len(subNodes) != 1 || subNodes[0].SubscriptionID != subID || subNodes[0].NodeHash != hash.Hex() || subNodes[0].Evicted {
		t.Fatalf("persisted relation: got %+v, want active relation for %s", subNodes, hash.Hex())
	}
}

func TestColdSubscriptionNodeCheck_FailurePersistsFailureAndDoesNotRetainMemory(t *testing.T) {
	engine, closer, err := state.PersistenceBootstrap(t.TempDir(), t.TempDir())
	if err != nil {
		t.Fatalf("PersistenceBootstrap: %v", err)
	}
	t.Cleanup(func() { _ = closer.Close() })

	const subID = "sub-cold-failure"
	now := time.Now().UnixNano()
	if err := engine.UpsertSubscription(model.Subscription{
		ID:               subID,
		Name:             "ColdFailure",
		URL:              "https://example.com/sub",
		UpdateIntervalNs: int64(30 * time.Minute),
		Enabled:          true,
		CreatedAtNs:      now,
		UpdatedAtNs:      now,
	}); err != nil {
		t.Fatalf("UpsertSubscription: %v", err)
	}

	runtimeCfg := config.NewDefaultRuntimeConfig()
	runtimeCfg.MaxConsecutiveFailures = 1
	subManager, pool := newColdCheckTestRuntime(engine, runtimeCfg)
	if err := bootstrapTopology(engine, subManager, pool, newDefaultPlatformEnvConfig()); err != nil {
		t.Fatalf("bootstrapTopology: %v", err)
	}

	raw := json.RawMessage(`{"type":"stub","server":"198.51.100.71","server_port":443}`)
	hash := node.HashFromRawOptions(raw)
	if err := engine.BulkUpsertNodesStatic([]model.NodeStatic{{
		Hash:        hash.Hex(),
		RawOptions:  raw,
		CreatedAtNs: now,
	}}); err != nil {
		t.Fatalf("BulkUpsertNodesStatic: %v", err)
	}
	if err := engine.BulkUpsertSubscriptionNodes([]model.SubscriptionNode{{
		SubscriptionID: subID,
		NodeHash:       hash.Hex(),
		Tags:           []string{"cold-failure"},
	}}); err != nil {
		t.Fatalf("BulkUpsertSubscriptionNodes: %v", err)
	}

	candidate := topology.ColdNodeCandidate{
		SubscriptionID: subID,
		Hash:           hash,
		RawOptions:     raw,
		Tags:           []string{"cold-failure"},
	}
	checker := newColdSubscriptionNodeChecker(engine, pool, subManager, &testutil.StubOutboundBuilder{}, func(hash node.Hash) error {
		pool.RecordResult(hash, false)
		pool.RecordLatency(hash, "example.com", nil)
		return errors.New("cold latency failed")
	})

	checker.Check(candidate)

	if _, ok := pool.GetEntry(hash); ok {
		t.Fatal("failed cold check should not retain node in memory")
	}
	sub, ok := subManager.Get(subID)
	if !ok {
		t.Fatalf("subscription %s missing", subID)
	}
	if _, ok := sub.ManagedNodes().LoadNode(hash); ok {
		t.Fatal("failed cold check should not retain active managed relation")
	}
	if err := engine.FlushDirtySets(newFlushReaders(pool, subManager, nil)); err != nil {
		t.Fatalf("FlushDirtySets: %v", err)
	}
	statics, err := engine.LoadAllNodesStatic()
	if err != nil {
		t.Fatalf("LoadAllNodesStatic: %v", err)
	}
	if len(statics) != 1 || statics[0].Hash != hash.Hex() {
		t.Fatalf("cold catalog static row should remain after failed check, got %+v", statics)
	}
	dynamics, err := engine.LoadAllNodesDynamic()
	if err != nil {
		t.Fatalf("LoadAllNodesDynamic: %v", err)
	}
	if len(dynamics) != 1 || dynamics[0].Hash != hash.Hex() || dynamics[0].CircuitOpenSince == 0 || dynamics[0].FailureCount == 0 {
		t.Fatalf("failed cold check should persist circuit/failure dynamic row, got %+v", dynamics)
	}
	subNodes, err := engine.LoadAllSubscriptionNodes()
	if err != nil {
		t.Fatalf("LoadAllSubscriptionNodes: %v", err)
	}
	if len(subNodes) != 1 || subNodes[0].NodeHash != hash.Hex() || subNodes[0].Evicted {
		t.Fatalf("failed cold check should keep non-evicted catalog relation, got %+v", subNodes)
	}
}

func TestBootstrapNodes_TrimRegularLatencyKeepsAuthorities(t *testing.T) {
	engine, closer, err := state.PersistenceBootstrap(t.TempDir(), t.TempDir())
	if err != nil {
		t.Fatalf("PersistenceBootstrap: %v", err)
	}
	t.Cleanup(func() { _ = closer.Close() })

	const subID = "sub-bootstrap-trim-latency"
	now := time.Now().UnixNano()
	if err := engine.UpsertSubscription(model.Subscription{
		ID:               subID,
		Name:             "BootstrapSub",
		URL:              "https://example.com/sub",
		UpdateIntervalNs: int64(30 * time.Minute),
		Enabled:          true,
		Ephemeral:        false,
		CreatedAtNs:      now,
		UpdatedAtNs:      now,
	}); err != nil {
		t.Fatalf("UpsertSubscription: %v", err)
	}

	raw := json.RawMessage(`{"type":"stub","server":"198.51.100.120","server_port":443}`)
	hash := node.HashFromRawOptions(raw)
	hashHex := hash.Hex()
	if err := engine.BulkUpsertNodesStatic([]model.NodeStatic{{
		Hash:        hashHex,
		RawOptions:  raw,
		CreatedAtNs: now - int64(time.Hour),
	}}); err != nil {
		t.Fatalf("BulkUpsertNodesStatic: %v", err)
	}
	if err := engine.BulkUpsertSubscriptionNodes([]model.SubscriptionNode{{
		SubscriptionID: subID,
		NodeHash:       hashHex,
		Tags:           []string{"bootstrap-tag"},
	}}); err != nil {
		t.Fatalf("BulkUpsertSubscriptionNodes: %v", err)
	}
	if err := engine.BulkUpsertNodesDynamic([]model.NodeDynamic{{
		Hash:             hashHex,
		CircuitOpenSince: 0,
	}}); err != nil {
		t.Fatalf("BulkUpsertNodesDynamic: %v", err)
	}

	// 2 authority domains + 3 regular domains (capacity=2, one regular should be trimmed).
	if err := engine.BulkUpsertNodeLatency([]model.NodeLatency{
		{NodeHash: hashHex, Domain: "gstatic.com", EwmaNs: int64(10 * time.Millisecond), LastUpdatedNs: now - int64(1*time.Second)},
		{NodeHash: hashHex, Domain: "github.com", EwmaNs: int64(20 * time.Millisecond), LastUpdatedNs: now - int64(2*time.Second)},
		{NodeHash: hashHex, Domain: "recent-a.com", EwmaNs: int64(30 * time.Millisecond), LastUpdatedNs: now - int64(3*time.Second)},
		{NodeHash: hashHex, Domain: "recent-b.com", EwmaNs: int64(40 * time.Millisecond), LastUpdatedNs: now - int64(4*time.Second)},
		{NodeHash: hashHex, Domain: "old-c.com", EwmaNs: int64(50 * time.Millisecond), LastUpdatedNs: now - int64(5*time.Second)},
	}); err != nil {
		t.Fatalf("BulkUpsertNodeLatency: %v", err)
	}

	runtimeCfg := config.NewDefaultRuntimeConfig()
	runtimeCfg.LatencyAuthorities = []string{"gstatic.com", "github.com"}
	envCfg := newDefaultPlatformEnvConfig()
	envCfg.MaxLatencyTableEntries = 2
	subManager, pool := newBootstrapTestRuntime(runtimeCfg)

	if err := bootstrapTopology(engine, subManager, pool, envCfg); err != nil {
		t.Fatalf("bootstrapTopology: %v", err)
	}

	outboundMgr := outbound.NewOutboundManager(pool, &testutil.StubOutboundBuilder{})
	if err := bootstrapNodes(engine, pool, subManager, outboundMgr, envCfg, runtimeCfg.LatencyAuthorities); err != nil {
		t.Fatalf("bootstrapNodes: %v", err)
	}

	entry, ok := pool.GetEntry(hash)
	if !ok || entry.LatencyTable == nil {
		t.Fatalf("node %s missing or no latency table after bootstrap", hashHex)
	}
	restored := make(map[string]bool)
	entry.LatencyTable.Range(func(domain string, _ node.DomainLatencyStats) bool {
		restored[domain] = true
		return true
	})
	for _, domain := range []string{"gstatic.com", "github.com", "recent-a.com", "recent-b.com"} {
		if !restored[domain] {
			t.Fatalf("expected domain %q to be restored", domain)
		}
	}
	if restored["old-c.com"] {
		t.Fatal("old regular domain should be trimmed at bootstrap")
	}
	// Post-bootstrap first regular insert should evict the oldest kept regular
	// entry (recent-b.com), not the newest one (recent-a.com).
	entry.LatencyTable.Update("fresh-d.com", 60*time.Millisecond, 30*time.Second)
	if _, ok := entry.LatencyTable.GetDomainStats("recent-a.com"); !ok {
		t.Fatal("recent-a.com should remain as the newer regular entry")
	}
	if _, ok := entry.LatencyTable.GetDomainStats("recent-b.com"); ok {
		t.Fatal("recent-b.com should be evicted as the oldest regular entry")
	}
	if _, ok := entry.LatencyTable.GetDomainStats("fresh-d.com"); !ok {
		t.Fatal("fresh-d.com should be inserted into regular LRU")
	}

	if err := engine.FlushDirtySets(newFlushReaders(pool, subManager, nil)); err != nil {
		t.Fatalf("FlushDirtySets: %v", err)
	}
	latencies, err := engine.LoadAllNodeLatency()
	if err != nil {
		t.Fatalf("LoadAllNodeLatency: %v", err)
	}
	domains := make(map[string]bool)
	for _, row := range latencies {
		if row.NodeHash == hashHex {
			domains[row.Domain] = true
		}
	}
	for _, domain := range []string{"gstatic.com", "github.com", "recent-a.com", "recent-b.com"} {
		if !domains[domain] {
			t.Fatalf("expected persisted domain %q after trim flush", domain)
		}
	}
	if domains["old-c.com"] {
		t.Fatal("trimmed regular domain should be deleted from persistence")
	}
}

func TestMarkNodeRemovedDirty_DeletesStaticDynamicAndLatency(t *testing.T) {
	engine, closer, err := state.PersistenceBootstrap(t.TempDir(), t.TempDir())
	if err != nil {
		t.Fatalf("PersistenceBootstrap: %v", err)
	}
	t.Cleanup(func() { _ = closer.Close() })

	raw := json.RawMessage(`{"type":"stub","server":"198.51.100.42","server_port":443}`)
	hash := node.HashFromRawOptions(raw)
	hashHex := hash.Hex()

	entry := node.NewNodeEntry(hash, raw, time.Now(), 16)
	entry.FailureCount.Store(2)
	entry.CircuitOpenSince.Store(time.Now().Add(-time.Minute).UnixNano())
	entry.SetEgressIP(netip.MustParseAddr("203.0.113.50"))
	entry.LastEgressUpdate.Store(time.Now().UnixNano())
	entry.LastEgressUpdateAttempt.Store(time.Now().UnixNano())
	entry.LastLatencyProbeAttempt.Store(time.Now().UnixNano())
	entry.LastAuthorityLatencyProbeAttempt.Store(time.Now().UnixNano())
	entry.LatencyTable.Update("example.com", 55*time.Millisecond, 5*time.Minute)
	entry.LatencyTable.Update("cloudflare.com", 65*time.Millisecond, 5*time.Minute)

	readers := state.CacheReaders{
		ReadNodeStatic: func(h string) *model.NodeStatic {
			if h != hashHex {
				return nil
			}
			return &model.NodeStatic{
				Hash:        hashHex,
				RawOptions:  append(json.RawMessage(nil), raw...),
				CreatedAtNs: entry.CreatedAt.UnixNano(),
			}
		},
		ReadNodeDynamic: func(h string) *model.NodeDynamic {
			if h != hashHex {
				return nil
			}
			return &model.NodeDynamic{
				Hash:                               hashHex,
				FailureCount:                       int(entry.FailureCount.Load()),
				CircuitOpenSince:                   entry.CircuitOpenSince.Load(),
				EgressIP:                           entry.GetEgressIP().String(),
				EgressUpdatedAtNs:                  entry.LastEgressUpdate.Load(),
				LastLatencyProbeAttemptNs:          entry.LastLatencyProbeAttempt.Load(),
				LastAuthorityLatencyProbeAttemptNs: entry.LastAuthorityLatencyProbeAttempt.Load(),
				LastEgressUpdateAttemptNs:          entry.LastEgressUpdateAttempt.Load(),
			}
		},
		ReadNodeLatency: func(key model.NodeLatencyKey) *model.NodeLatency {
			if key.NodeHash != hashHex {
				return nil
			}
			stats, ok := entry.LatencyTable.GetDomainStats(key.Domain)
			if !ok {
				return nil
			}
			return &model.NodeLatency{
				NodeHash:      hashHex,
				Domain:        key.Domain,
				EwmaNs:        int64(stats.Ewma),
				LastUpdatedNs: stats.LastUpdated.UnixNano(),
			}
		},
	}

	// Seed cache rows for this node.
	engine.MarkNodeStatic(hashHex)
	engine.MarkNodeDynamic(hashHex)
	engine.MarkNodeLatency(hashHex, "example.com")
	engine.MarkNodeLatency(hashHex, "cloudflare.com")
	if err := engine.FlushDirtySets(readers); err != nil {
		t.Fatalf("seed FlushDirtySets: %v", err)
	}

	// Simulate node removed callback and flush deletes.
	markNodeRemovedDirty(engine, hash, entry)
	if err := engine.FlushDirtySets(state.CacheReaders{}); err != nil {
		t.Fatalf("delete FlushDirtySets: %v", err)
	}

	nodesStatic, err := engine.LoadAllNodesStatic()
	if err != nil {
		t.Fatalf("LoadAllNodesStatic: %v", err)
	}
	if len(nodesStatic) != 0 {
		t.Fatalf("nodes_static not deleted: %+v", nodesStatic)
	}

	nodesDynamic, err := engine.LoadAllNodesDynamic()
	if err != nil {
		t.Fatalf("LoadAllNodesDynamic: %v", err)
	}
	if len(nodesDynamic) != 0 {
		t.Fatalf("nodes_dynamic not deleted: %+v", nodesDynamic)
	}

	latencies, err := engine.LoadAllNodeLatency()
	if err != nil {
		t.Fatalf("LoadAllNodeLatency: %v", err)
	}
	if len(latencies) != 0 {
		t.Fatalf("node_latency not deleted: %+v", latencies)
	}
}

func TestNewTopologyRuntime_WiresActiveOnlyRefreshAndColdQueue(t *testing.T) {
	engine, closer, err := state.PersistenceBootstrap(t.TempDir(), t.TempDir())
	if err != nil {
		t.Fatalf("PersistenceBootstrap: %v", err)
	}
	t.Cleanup(func() { _ = closer.Close() })

	const subID = "sub-runtime-catalog"
	now := time.Now().UnixNano()
	if err := engine.UpsertSubscription(model.Subscription{
		ID:               subID,
		Name:             "RuntimeCatalog",
		SourceType:       subscription.SourceTypeRemote,
		URL:              "https://example.com/sub",
		UpdateIntervalNs: int64(30 * time.Minute),
		Enabled:          true,
		CreatedAtNs:      now,
		UpdatedAtNs:      now,
	}); err != nil {
		t.Fatalf("UpsertSubscription: %v", err)
	}

	raw := `{"type":"shadowsocks","tag":"runtime-catalog","server":"198.51.100.90","server_port":443}`
	hash := node.HashFromRawOptions([]byte(raw))
	body := []byte(`{"outbounds":[` + raw + `]}`)
	envCfg := newDefaultPlatformEnvConfig()
	runtimeCfg := config.NewDefaultRuntimeConfig()
	var runtimePtr atomic.Pointer[config.RuntimeConfig]
	runtimePtr.Store(runtimeCfg)
	geoSvc := geoip.NewService(geoip.ServiceConfig{OpenDB: geoip.NoOpOpen})

	rt, err := newTopologyRuntime(
		engine,
		envCfg,
		&runtimePtr,
		geoSvc,
		staticSubscriptionDownloader{body: body},
		nil,
		nil,
	)
	if err != nil {
		t.Fatalf("newTopologyRuntime: %v", err)
	}
	if rt.scheduler == nil {
		t.Fatal("runtime scheduler should be initialized")
	}
	if rt.coldNodeQueue == nil {
		t.Fatal("runtime should wire a cold-node queue for catalog refresh")
	}

	if err := bootstrapTopology(engine, rt.subManager, rt.pool, envCfg); err != nil {
		t.Fatalf("bootstrapTopology: %v", err)
	}
	sub := rt.subManager.Lookup(subID)
	if sub == nil {
		t.Fatalf("subscription %s not bootstrapped", subID)
	}

	rt.scheduler.UpdateSubscription(sub)

	statics, err := engine.LoadAllNodesStatic()
	if err != nil {
		t.Fatalf("LoadAllNodesStatic: %v", err)
	}
	if len(statics) != 1 || statics[0].Hash != hash.Hex() {
		t.Fatalf("catalog refresh should persist parsed node static catalog, got %+v want %s", statics, hash.Hex())
	}
	subNodes, err := engine.LoadSubscriptionNodes(subID)
	if err != nil {
		t.Fatalf("LoadSubscriptionNodes: %v", err)
	}
	if len(subNodes) != 1 || subNodes[0].NodeHash != hash.Hex() || subNodes[0].Evicted {
		t.Fatalf("catalog refresh should persist subscription relation before promotion, got %+v", subNodes)
	}
	if rt.pool.Size() != 0 {
		t.Fatalf("new catalog nodes should stay cold until check promotion, pool size=%d", rt.pool.Size())
	}
}

func TestNewTopologyRuntime_LegacyRuntimeDoesNotWireCatalogQueue(t *testing.T) {
	engine, closer, err := state.PersistenceBootstrap(t.TempDir(), t.TempDir())
	if err != nil {
		t.Fatalf("PersistenceBootstrap: %v", err)
	}
	t.Cleanup(func() { _ = closer.Close() })

	envCfg := newDefaultPlatformEnvConfig()
	envCfg.ActiveOnlyRuntime = false
	runtimeCfg := config.NewDefaultRuntimeConfig()
	var runtimePtr atomic.Pointer[config.RuntimeConfig]
	runtimePtr.Store(runtimeCfg)
	geoSvc := geoip.NewService(geoip.ServiceConfig{OpenDB: geoip.NoOpOpen})

	rt, err := newTopologyRuntime(
		engine,
		envCfg,
		&runtimePtr,
		geoSvc,
		staticSubscriptionDownloader{body: []byte(`{"outbounds":[]}`)},
		nil,
		nil,
	)
	if err != nil {
		t.Fatalf("newTopologyRuntime: %v", err)
	}
	if rt.coldNodeQueue != nil {
		t.Fatal("legacy runtime should not allocate cold-node queue")
	}
}
