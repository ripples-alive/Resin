package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/netip"
	"os"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Resinat/Resin/internal/config"
	"github.com/Resinat/Resin/internal/geoip"
	"github.com/Resinat/Resin/internal/metrics"
	"github.com/Resinat/Resin/internal/model"
	"github.com/Resinat/Resin/internal/netutil"
	"github.com/Resinat/Resin/internal/node"
	"github.com/Resinat/Resin/internal/outbound"
	"github.com/Resinat/Resin/internal/platform"
	"github.com/Resinat/Resin/internal/probe"
	"github.com/Resinat/Resin/internal/proxy"
	"github.com/Resinat/Resin/internal/requestlog"
	"github.com/Resinat/Resin/internal/routing"
	"github.com/Resinat/Resin/internal/state"
	"github.com/Resinat/Resin/internal/subscription"
	"github.com/Resinat/Resin/internal/topology"
)

type topologyRuntime struct {
	subManager       *topology.SubscriptionManager
	pool             *topology.GlobalNodePool
	probeMgr         *probe.ProbeManager
	scheduler        *topology.SubscriptionScheduler
	ephemeralCleaner *topology.EphemeralCleaner
	coldNodeQueue    *coldSubscriptionNodeCheckQueue
	coldSweepRunner  *coldSubscriptionNodeSweepRunner
	router           *routing.Router
	leaseCleaner     *routing.LeaseCleaner
	outboundMgr      *outbound.OutboundManager
	singboxBuilder   *outbound.SingboxBuilder // for Close on shutdown
}

const downloadUserAgent = "clash.meta"

const (
	coldSubscriptionNodeQueueCapacity = 1024
	coldSubscriptionNodeWorkerCount   = 4

	coldSubscriptionNodeSweepInterval  = 5 * time.Minute
	coldSubscriptionNodeSweepBatchSize = 256

	coldSubscriptionNodeTransientSubscriptionID = "__cold_check_transient__"
)

func main() {
	if err := run(); err != nil {
		fatalf("%v", err)
	}
}

func fatalf(format string, args ...any) {
	message := fmt.Sprintf(format, args...)
	if supportsANSIColorOnStderr() {
		fmt.Fprintf(os.Stderr, "\x1b[31mfatal:\x1b[0m %s\n", message)
	} else {
		fmt.Fprintf(os.Stderr, "fatal: %s\n", message)
	}
	os.Exit(1)
}

func supportsANSIColorOnStderr() bool {
	if os.Getenv("NO_COLOR") != "" {
		return false
	}
	term := os.Getenv("TERM")
	if term == "" || term == "dumb" {
		return false
	}
	stat, err := os.Stderr.Stat()
	if err != nil {
		return false
	}
	return (stat.Mode() & os.ModeCharDevice) != 0
}

func startupWarnf(format string, args ...any) {
	message := fmt.Sprintf(format, args...)
	if supportsANSIColorOnStderr() {
		fmt.Fprintf(os.Stderr, "\x1b[33mwarning:\x1b[0m %s\n", message)
	} else {
		fmt.Fprintf(os.Stderr, "warning: %s\n", message)
	}
}

func authVersionStartupWarning(authVersion config.AuthVersion) string {
	if authVersion != config.AuthVersionLegacyV0 {
		return ""
	}
	return fmt.Sprintf(
		"RESIN_AUTH_VERSION=LEGACY_V0 enables legacy auth compatibility and will be removed in a future release. Migrate to V1. Migration guide: %s",
		config.AuthMigrationGuideURL,
	)
}

func inboundProtocolsStartupLabel(authVersion config.AuthVersion) string {
	switch authVersion {
	case config.AuthVersionV1:
		return "HTTP + SOCKS5"
	case config.AuthVersionLegacyV0:
		return "HTTP; SOCKS5 disabled under RESIN_AUTH_VERSION=LEGACY_V0"
	default:
		return "HTTP"
	}
}

func loadRuntimeConfig(engine *state.StateEngine) *config.RuntimeConfig {
	runtimeCfg, ver, err := engine.GetSystemConfig()
	if err != nil {
		fatalf("load system config: %v", err)
	}
	if runtimeCfg == nil {
		log.Println("No persisted runtime config found, using defaults")
		return config.NewDefaultRuntimeConfig()
	}
	log.Printf("Loaded persisted runtime config (version %d)", ver)
	return runtimeCfg
}

func newDirectDownloader(
	envCfg *config.EnvConfig,
) *netutil.DirectDownloader {
	return netutil.NewDirectDownloader(
		func() time.Duration {
			return envCfg.ResourceFetchTimeout
		},
		func() string {
			return currentDownloadUserAgent()
		},
	)
}

func currentDownloadUserAgent() string {
	return downloadUserAgent
}

func runtimeConfigSnapshot(runtimeCfg *atomic.Pointer[config.RuntimeConfig]) *config.RuntimeConfig {
	if runtimeCfg == nil {
		return config.NewDefaultRuntimeConfig()
	}
	cfg := runtimeCfg.Load()
	if cfg == nil {
		return config.NewDefaultRuntimeConfig()
	}
	return cfg
}

type requestLogRuntimeSettings struct {
	DBMaxBytes    int64
	DBRetainCount int
	QueueSize     int
	FlushBatch    int
	FlushInterval time.Duration
}

func deriveRequestLogRuntimeSettings(envCfg *config.EnvConfig) requestLogRuntimeSettings {
	return requestLogRuntimeSettings{
		DBMaxBytes:    int64(envCfg.RequestLogDBMaxMB) * 1024 * 1024,
		DBRetainCount: envCfg.RequestLogDBRetainCount,
		QueueSize:     envCfg.RequestLogQueueSize,
		FlushBatch:    envCfg.RequestLogQueueFlushBatchSize,
		FlushInterval: envCfg.RequestLogQueueFlushInterval,
	}
}

type metricsManagerSettings struct {
	LatencyBinMs                int
	LatencyOverflowMs           int
	BucketSeconds               int
	ThroughputIntervalSec       int
	ThroughputRealtimeCapacity  int
	ConnectionsIntervalSec      int
	ConnectionsRealtimeCapacity int
	LeasesIntervalSec           int
	LeasesRealtimeCapacity      int
}

func deriveMetricsManagerSettings(envCfg *config.EnvConfig) metricsManagerSettings {
	return metricsManagerSettings{
		LatencyBinMs:                envCfg.MetricLatencyBinWidthMS,
		LatencyOverflowMs:           envCfg.MetricLatencyBinOverflowMS,
		BucketSeconds:               envCfg.MetricBucketSeconds,
		ThroughputIntervalSec:       envCfg.MetricThroughputIntervalSeconds,
		ThroughputRealtimeCapacity:  realtimeCapacity(envCfg.MetricThroughputRetentionSeconds, envCfg.MetricThroughputIntervalSeconds),
		ConnectionsIntervalSec:      envCfg.MetricConnectionsIntervalSeconds,
		ConnectionsRealtimeCapacity: realtimeCapacity(envCfg.MetricConnectionsRetentionSeconds, envCfg.MetricConnectionsIntervalSeconds),
		LeasesIntervalSec:           envCfg.MetricLeasesIntervalSeconds,
		LeasesRealtimeCapacity:      realtimeCapacity(envCfg.MetricLeasesRetentionSeconds, envCfg.MetricLeasesIntervalSeconds),
	}
}

func realtimeCapacity(retentionSec, intervalSec int) int {
	if intervalSec <= 0 {
		intervalSec = 1
	}
	if retentionSec <= 0 {
		retentionSec = intervalSec
	}
	capacity := retentionSec / intervalSec
	if retentionSec%intervalSec != 0 {
		capacity++
	}
	if capacity <= 0 {
		capacity = 1
	}
	return capacity
}

func newGeoIPService(
	cacheDir string,
	updateSchedule string,
	downloader netutil.Downloader,
) *geoip.Service {
	geoSvc := geoip.NewService(geoip.ServiceConfig{
		CacheDir:       cacheDir,
		UpdateSchedule: updateSchedule,
		Downloader:     downloader,
		OpenDB:         geoip.MMDBOpen,
	})
	return geoSvc
}

func startGeoIPService(geoSvc *geoip.Service) {
	if err := geoSvc.Start(); err != nil {
		log.Printf("GeoIP service start (non-fatal): %v", err)
	}
	log.Println("GeoIP service initialized")
}

func newTopologyRuntime(
	engine *state.StateEngine,
	envCfg *config.EnvConfig,
	runtimeCfg *atomic.Pointer[config.RuntimeConfig],
	geoSvc *geoip.Service,
	downloader netutil.Downloader,
	onProbeConnLifecycle func(netutil.ConnLifecycleOp),
	onNodeRemoved func(node.Hash),
) (*topologyRuntime, error) {
	subManager := topology.NewSubscriptionManager()

	pool := topology.NewGlobalNodePool(topology.PoolConfig{
		SubLookup: subManager.Lookup,
		GeoLookup: geoSvc.Lookup,
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
		MaxLatencyTableEntries: envCfg.MaxLatencyTableEntries,
		MaxConsecutiveFailures: func() int {
			return runtimeConfigSnapshot(runtimeCfg).MaxConsecutiveFailures
		},
		LatencyDecayWindow: func() time.Duration {
			return time.Duration(runtimeConfigSnapshot(runtimeCfg).LatencyDecayWindow)
		},
		LatencyAuthorities: func() []string {
			return runtimeConfigSnapshot(runtimeCfg).LatencyAuthorities
		},
	})
	log.Println("Topology: GlobalNodePool initialized")

	singboxBuilder, err := outbound.NewSingboxBuilderWithSecureDNS(envCfg.EnableEmbeddedSecureDNS)
	if err != nil {
		return nil, fmt.Errorf("singbox builder: %w", err)
	}
	outboundMgr := outbound.NewOutboundManager(pool, singboxBuilder)

	probeMgr := probe.NewProbeManager(probe.ProbeConfig{
		Pool:        pool,
		Concurrency: envCfg.ProbeConcurrency,
		Fetcher: func(hash node.Hash, url string) ([]byte, time.Duration, error) {
			ctx, cancel := context.WithTimeout(context.Background(), envCfg.ProbeTimeout)
			defer cancel()
			entry, ok := pool.GetEntry(hash)
			if !ok {
				return nil, 0, fmt.Errorf("node not found")
			}
			outboundPtr := entry.Outbound.Load()
			if outboundPtr == nil {
				return nil, 0, outbound.ErrOutboundNotReady
			}
			return netutil.HTTPGetViaOutbound(ctx, *outboundPtr, url, netutil.OutboundHTTPOptions{
				RequireStatusOK: false,
				OnConnLifecycle: func(op netutil.ConnLifecycleOp) {
					if onProbeConnLifecycle != nil {
						onProbeConnLifecycle(op)
					}
				},
			})
		},
		MaxEgressTestInterval: func() time.Duration {
			return time.Duration(runtimeConfigSnapshot(runtimeCfg).MaxEgressTestInterval)
		},
		MaxLatencyTestInterval: func() time.Duration {
			return time.Duration(runtimeConfigSnapshot(runtimeCfg).MaxLatencyTestInterval)
		},
		MaxAuthorityLatencyTestInterval: func() time.Duration {
			return time.Duration(runtimeConfigSnapshot(runtimeCfg).MaxAuthorityLatencyTestInterval)
		},
		LatencyTestURL: func() string {
			return runtimeConfigSnapshot(runtimeCfg).LatencyTestURL
		},
		LatencyAuthorities: func() []string {
			return runtimeConfigSnapshot(runtimeCfg).LatencyAuthorities
		},
	})

	pool.SetOnNodeAdded(func(hash node.Hash) {
		engine.MarkNodeStatic(hash.Hex())
		outboundMgr.EnsureNodeOutbound(hash)
		// No NotifyNodeDirty here — AddNodeFromSub already notifies all platforms.
		probeMgr.TriggerImmediateEgressProbe(hash)
	})
	pool.SetOnNodeRemoved(func(hash node.Hash, entry *node.NodeEntry) {
		markNodeRemovedDirty(engine, hash, entry)
		outboundMgr.RemoveNodeOutbound(entry)
		if entry != nil && entry.LatencyTable != nil {
			entry.LatencyTable.Close()
		}
		if onNodeRemoved != nil {
			onNodeRemoved(hash)
		}
	})
	log.Println("ProbeManager initialized")

	var coldNodeQueue *coldSubscriptionNodeCheckQueue
	var coldSweepRunner *coldSubscriptionNodeSweepRunner
	var inventoryStore topology.SubscriptionInventoryStore
	if envCfg.ActiveOnlyRuntime {
		coldChecker := newColdSubscriptionNodeChecker(engine, pool, subManager, singboxBuilder, func(hash node.Hash) error {
			_, err := probeMgr.ProbeLatencySync(hash)
			return err
		})
		coldNodeQueue = newColdSubscriptionNodeCheckQueue(
			coldChecker,
			coldSubscriptionNodeWorkerCount,
			coldSubscriptionNodeQueueCapacity,
		)
		inventoryStore = engine
		coldSweepRunner = newColdSubscriptionNodeSweepRunner(coldSubscriptionNodeSweepRunnerConfig{
			store:         engine,
			checker:       coldNodeQueue,
			pool:          pool,
			subManager:    subManager,
			sweepInterval: coldSubscriptionNodeSweepInterval,
			batchSize:     coldSubscriptionNodeSweepBatchSize,
		})
	}

	scheduler := topology.NewSubscriptionScheduler(topology.SchedulerConfig{
		SubManager:           subManager,
		Pool:                 pool,
		Downloader:           downloader,
		InventoryStore:       inventoryStore,
		ColdNodeSweepTrigger: coldSweepRunner,
		OnSubRefreshState: func(subID string, checkedNs int64, updatedNs *int64, lastError string) {
			if err := engine.UpdateSubscriptionRefreshState(subID, checkedNs, updatedNs, lastError); err != nil {
				log.Printf("[scheduler] persist subscription refresh state %s: %v", subID, err)
			}
		},
		OnSubReenabledNode: func(hash node.Hash) {
			outboundMgr.EnsureNodeOutbound(hash)
			probeMgr.TriggerImmediateEgressProbe(hash)
			probeMgr.TriggerImmediateLatencyProbe(hash)
		},
	})
	ephemeralCleaner := topology.NewEphemeralCleaner(
		subManager,
		pool,
	)
	ephemeralCleaner.SetOnNodeEvicted(func(subID string, hash node.Hash) {
		engine.MarkSubscriptionNode(subID, hash.Hex())
	})

	return &topologyRuntime{
		subManager:       subManager,
		pool:             pool,
		probeMgr:         probeMgr,
		scheduler:        scheduler,
		ephemeralCleaner: ephemeralCleaner,
		coldNodeQueue:    coldNodeQueue,
		coldSweepRunner:  coldSweepRunner,
		outboundMgr:      outboundMgr,
		singboxBuilder:   singboxBuilder,
	}, nil
}

func markNodeRemovedDirty(engine *state.StateEngine, hash node.Hash, entry *node.NodeEntry) {
	hashHex := hash.Hex()
	engine.MarkNodeStaticDelete(hashHex)
	engine.MarkNodeDynamicDelete(hashHex)

	if entry == nil || entry.LatencyTable == nil {
		return
	}
	entry.LatencyTable.Range(func(domain string, _ node.DomainLatencyStats) bool {
		engine.MarkNodeLatencyDelete(hashHex, domain)
		return true
	})
}

func bootstrapTopology(
	engine *state.StateEngine,
	subManager *topology.SubscriptionManager,
	pool *topology.GlobalNodePool,
	envCfg *config.EnvConfig,
) error {
	dbSubs, err := engine.ListSubscriptions()
	if err != nil {
		return fmt.Errorf("load subscriptions: %w", err)
	}
	for _, ms := range dbSubs {
		sub := subscription.NewSubscription(ms.ID, ms.Name, ms.URL, ms.Enabled, ms.Ephemeral)
		sub.SetFetchConfig(ms.URL, ms.UpdateIntervalNs)
		sub.SetSourceType(ms.SourceType)
		sub.SetContent(ms.Content)
		sub.SetEphemeralNodeEvictDelayNs(ms.EphemeralNodeEvictDelayNs)
		sub.LastCheckedNs.Store(ms.LastCheckedNs)
		sub.LastUpdatedNs.Store(ms.LastUpdatedNs)
		sub.SetLastError(ms.LastError)
		sub.CreatedAtNs = ms.CreatedAtNs
		sub.UpdatedAtNs = ms.UpdatedAtNs
		subManager.Register(sub)
	}
	log.Printf("Loaded %d subscriptions from state.db", len(dbSubs))

	dbPlats, err := engine.ListPlatforms()
	if err != nil {
		return fmt.Errorf("load platforms: %w", err)
	}
	if envCfg != nil && envCfg.AuthVersion == config.AuthVersionV1 {
		if err := validatePersistedPlatformNamesForV1(dbPlats); err != nil {
			return fmt.Errorf("validate platform names for V1: %w", err)
		}
	}
	if err := ensureDefaultPlatform(engine, envCfg, dbPlats); err != nil {
		return fmt.Errorf("ensure default platform: %w", err)
	}
	dbPlats, err = engine.ListPlatforms()
	if err != nil {
		return fmt.Errorf("reload platforms: %w", err)
	}
	for _, mp := range dbPlats {
		plat, err := platform.BuildFromModel(mp)
		if err != nil {
			return err
		}
		pool.RegisterPlatform(plat)
	}
	log.Printf("Loaded %d platforms from state.db", len(dbPlats))
	return nil
}

func validatePersistedPlatformNamesForV1(platformsInDB []model.Platform) error {
	var invalidPlatformNames []string
	for _, p := range platformsInDB {
		if err := platform.ValidatePlatformName(p.Name); err != nil {
			invalidPlatformNames = append(invalidPlatformNames, fmt.Sprintf("%q", p.Name))
		}
	}

	if len(invalidPlatformNames) > 0 {
		return fmt.Errorf(
			"%d platform(s) are incompatible with RESIN_AUTH_VERSION=V1: %s. Platform name rules: must be non-empty; must not be reserved name; must not contain any of \".:|/\\\\@?#%%~\"; must not contain spaces, tabs, newlines, or carriage returns. Please rename these platforms; you can temporarily start with RESIN_AUTH_VERSION=LEGACY_V0, rename them, then switch back to V1. Migration guide: %s",
			len(invalidPlatformNames),
			strings.Join(invalidPlatformNames, ", "),
			config.AuthMigrationGuideURL,
		)
	}
	return nil
}

func ensureDefaultPlatform(
	engine *state.StateEngine,
	envCfg *config.EnvConfig,
	platformsInDB []model.Platform,
) error {
	hasDefaultID := false
	for _, p := range platformsInDB {
		if p.ID == platform.DefaultPlatformID {
			hasDefaultID = true
		}
	}
	if hasDefaultID {
		return nil
	}

	defaultPlatform := model.Platform{
		ID:                               platform.DefaultPlatformID,
		Name:                             platform.DefaultPlatformName,
		StickyTTLNs:                      int64(envCfg.DefaultPlatformStickyTTL),
		RegexFilters:                     append([]string(nil), envCfg.DefaultPlatformRegexFilters...),
		RegionFilters:                    append([]string(nil), envCfg.DefaultPlatformRegionFilters...),
		ReverseProxyMissAction:           envCfg.DefaultPlatformReverseProxyMissAction,
		ReverseProxyEmptyAccountBehavior: envCfg.DefaultPlatformReverseProxyEmptyAccountBehavior,
		ReverseProxyFixedAccountHeader:   envCfg.DefaultPlatformReverseProxyFixedAccountHeader,
		AllocationPolicy:                 envCfg.DefaultPlatformAllocationPolicy,
		UpdatedAtNs:                      time.Now().UnixNano(),
	}
	if err := engine.UpsertPlatform(defaultPlatform); err != nil {
		return err
	}
	log.Println("Created built-in Default platform")
	return nil
}

var defaultFallbackAccountHeaders = []string{"Authorization", "x-api-key"}

func ensureDefaultAccountHeaderRule(engine *state.StateEngine) error {
	created, err := engine.EnsureAccountHeaderRule(model.AccountHeaderRule{
		URLPrefix:   "*",
		Headers:     append([]string(nil), defaultFallbackAccountHeaders...),
		UpdatedAtNs: time.Now().UnixNano(),
	})
	if err != nil {
		return fmt.Errorf("ensure default account header fallback rule: %w", err)
	}
	if created {
		log.Printf("Created built-in account header fallback rule %q", "*")
	}
	return nil
}

type coldSubscriptionNodeChecker struct {
	engine     *state.StateEngine
	pool       *topology.GlobalNodePool
	subManager *topology.SubscriptionManager
	outbound   *outbound.OutboundManager
	probe      func(node.Hash) error
}

type coldNodeDirtyFlusher interface {
	FlushColdNodeDirty() error
}

type coldNodeBatchWorkChecker interface {
	CheckForBatch(topology.ColdNodeCandidate) func()
}

func newColdSubscriptionNodeChecker(
	engine *state.StateEngine,
	pool *topology.GlobalNodePool,
	subManager *topology.SubscriptionManager,
	builder outbound.OutboundBuilder,
	probe func(node.Hash) error,
) *coldSubscriptionNodeChecker {
	return &coldSubscriptionNodeChecker{
		engine:     engine,
		pool:       pool,
		subManager: subManager,
		outbound:   outbound.NewOutboundManager(pool, builder),
		probe:      probe,
	}
}

func (c *coldSubscriptionNodeChecker) Check(candidate topology.ColdNodeCandidate) {
	cleanup := c.CheckForBatch(candidate)
	if err := c.FlushColdNodeDirty(); err != nil {
		log.Printf("cold subscription node check: flush state for %s: %v", candidate.Hash.Hex(), err)
	}
	if cleanup != nil {
		cleanup()
	}
}

func (c *coldSubscriptionNodeChecker) CheckBatch(ctx context.Context, candidates []topology.ColdNodeCandidate) int {
	completed := 0
	cleanups := make([]func(), 0, len(candidates))
	for _, candidate := range candidates {
		select {
		case <-ctx.Done():
			if err := c.FlushColdNodeDirty(); err != nil {
				log.Printf("cold subscription node check: flush partial batch state: %v", err)
			}
			for _, cleanup := range cleanups {
				cleanup()
			}
			return completed
		default:
		}
		if cleanup := c.CheckForBatch(candidate); cleanup != nil {
			cleanups = append(cleanups, cleanup)
		}
		completed++
	}
	if err := c.FlushColdNodeDirty(); err != nil {
		log.Printf("cold subscription node check: flush batch state: %v", err)
	}
	for _, cleanup := range cleanups {
		cleanup()
	}
	return completed
}

func (c *coldSubscriptionNodeChecker) CheckForBatch(candidate topology.ColdNodeCandidate) func() {
	if c == nil || c.pool == nil || c.engine == nil {
		return nil
	}
	createdAt := time.Now()
	checkStartedNs := createdAt.UnixNano()
	entry := node.NewNodeEntry(candidate.Hash, append(json.RawMessage(nil), candidate.RawOptions...), createdAt, 16)
	// The cold sweep needs a pool entry so existing outbound/probe plumbing can
	// operate, but this is not yet an active subscription relation. Keep a
	// probe-only sentinel reference and remove it before returning.
	entry.AddSubscriptionID(coldSubscriptionNodeTransientSubscriptionID)
	entry.CircuitOpenSince.Store(createdAt.UnixNano())
	c.pool.LoadNodeFromBootstrap(entry)

	if c.outbound != nil {
		c.outbound.EnsureNodeOutbound(candidate.Hash)
	}
	probeErr := error(nil)
	if c.probe != nil {
		probeErr = c.probe(candidate.Hash)
	}
	entry, ok := c.pool.GetEntry(candidate.Hash)
	success := probeErr == nil && ok && entry.HasOutbound() && !entry.IsCircuitOpen() && entry.HasLatency()
	if success {
		c.pool.AddNodeFromSub(candidate.Hash, candidate.RawOptions, candidate.SubscriptionID)
		if sub := c.subManager.Lookup(candidate.SubscriptionID); sub != nil {
			sub.ManagedNodes().StoreNode(candidate.Hash, subscription.ManagedNode{Tags: append([]string(nil), candidate.Tags...)})
		}
		c.engine.MarkNodeStatic(candidate.Hash.Hex())
		c.engine.MarkNodeDynamic(candidate.Hash.Hex())
		c.engine.MarkSubscriptionNode(candidate.SubscriptionID, candidate.Hash.Hex())
		c.removeTransientColdCheckEntry(candidate.Hash)
		return nil
	}

	if ok {
		if entry.LastLatencyProbeAttempt.Load() < checkStartedNs {
			entry.LastLatencyProbeAttempt.Store(time.Now().UnixNano())
		}
		c.engine.MarkNodeDynamic(candidate.Hash.Hex())
		return func() { c.removeTransientColdCheckEntry(candidate.Hash) }
	}
	return nil
}

func (c *coldSubscriptionNodeChecker) FlushColdNodeDirty() error {
	if c == nil || c.engine == nil {
		return nil
	}
	return c.engine.FlushNodeDirtySets(newFlushReaders(c.pool, c.subManager, nil))
}

func (c *coldSubscriptionNodeChecker) removeTransientColdCheckEntry(hash node.Hash) {
	entry, ok := c.pool.GetEntry(hash)
	if !ok || entry == nil {
		return
	}
	if entry.SubscriptionCount() == 1 {
		ids := entry.SubscriptionIDs()
		if len(ids) == 1 && ids[0] == coldSubscriptionNodeTransientSubscriptionID {
			c.pool.DeleteNodeFromBootstrap(hash)
			if c.outbound != nil {
				c.outbound.RemoveNodeOutbound(entry)
			}
			if entry.LatencyTable != nil {
				entry.LatencyTable.Close()
			}
			return
		}
	}
	c.pool.RemoveNodeFromSub(hash, coldSubscriptionNodeTransientSubscriptionID)
}

type coldSubscriptionNodeCheckQueue struct {
	checker coldNodeChecker
	ch      chan coldSubscriptionNodeCheckWork
	stopCh  chan struct{}
	workers int
	stopped atomic.Bool
	wg      sync.WaitGroup
}

type coldSubscriptionNodeCheckWork struct {
	candidate topology.ColdNodeCandidate
	done      chan coldSubscriptionNodeCheckResult
}

type coldSubscriptionNodeCheckResult struct {
	cleanup func()
}

func newColdSubscriptionNodeCheckQueue(
	checker coldNodeChecker,
	workers int,
	capacity int,
) *coldSubscriptionNodeCheckQueue {
	if workers <= 0 {
		workers = 1
	}
	if capacity <= 0 {
		capacity = 1
	}
	return &coldSubscriptionNodeCheckQueue{
		checker: checker,
		ch:      make(chan coldSubscriptionNodeCheckWork, capacity),
		stopCh:  make(chan struct{}),
		workers: workers,
	}
}

func (q *coldSubscriptionNodeCheckQueue) Start() {
	if q == nil || q.checker == nil {
		return
	}
	for i := 0; i < q.workers; i++ {
		q.wg.Add(1)
		go func() {
			defer q.wg.Done()
			for {
				select {
				case <-q.stopCh:
					return
				case work := <-q.ch:
					var cleanup func()
					if batchChecker, ok := q.checker.(coldNodeBatchWorkChecker); ok {
						cleanup = batchChecker.CheckForBatch(work.candidate)
					} else {
						q.checker.Check(work.candidate)
					}
					if work.done != nil {
						work.done <- coldSubscriptionNodeCheckResult{cleanup: cleanup}
					}
				}
			}
		}()
	}
}

func (q *coldSubscriptionNodeCheckQueue) Stop() {
	if q == nil || !q.stopped.CompareAndSwap(false, true) {
		return
	}
	close(q.stopCh)
	q.wg.Wait()
}

func (q *coldSubscriptionNodeCheckQueue) Check(candidate topology.ColdNodeCandidate) {
	_ = q.CheckBatch(context.Background(), []topology.ColdNodeCandidate{candidate})
}

func (q *coldSubscriptionNodeCheckQueue) CheckBatch(ctx context.Context, candidates []topology.ColdNodeCandidate) int {
	if q == nil || q.checker == nil || q.stopped.Load() {
		return 0
	}
	done := make(chan coldSubscriptionNodeCheckResult, len(candidates))
	queued := 0
candidateLoop:
	for _, candidate := range candidates {
		work := topology.ColdNodeCandidate{
			SubscriptionID: candidate.SubscriptionID,
			Hash:           candidate.Hash,
			RawOptions:     append([]byte(nil), candidate.RawOptions...),
			Tags:           append([]string(nil), candidate.Tags...),
		}
		select {
		case <-ctx.Done():
			break candidateLoop
		case <-q.stopCh:
			return queued
		case q.ch <- coldSubscriptionNodeCheckWork{candidate: work, done: done}:
			queued++
		}
	}

	completed := 0
	cleanups := make([]func(), 0, queued)
	for completed < queued {
		select {
		case <-q.stopCh:
			q.finishCompletedColdChecks(cleanups)
			return completed
		case result := <-done:
			if result.cleanup != nil {
				cleanups = append(cleanups, result.cleanup)
			}
			completed++
		}
	}
	q.finishCompletedColdChecks(cleanups)
	return completed
}

func (q *coldSubscriptionNodeCheckQueue) finishCompletedColdChecks(cleanups []func()) {
	if q == nil {
		return
	}
	if flusher, ok := q.checker.(coldNodeDirtyFlusher); ok {
		if err := flusher.FlushColdNodeDirty(); err != nil {
			log.Printf("cold subscription node check: flush batch state: %v", err)
		}
	}
	for _, cleanup := range cleanups {
		cleanup()
	}
}

func nodeDynamicModelFromEntry(hash string, entry *node.NodeEntry) model.NodeDynamic {
	egressIP := entry.GetEgressIP()
	egressStr := ""
	if egressIP.IsValid() {
		egressStr = egressIP.String()
	}
	return model.NodeDynamic{
		Hash:                               hash,
		FailureCount:                       int(entry.FailureCount.Load()),
		CircuitOpenSince:                   entry.CircuitOpenSince.Load(),
		EgressIP:                           egressStr,
		EgressIPs:                          entry.GetObservedEgressIPStrings(),
		EgressRegion:                       entry.GetEgressRegion(),
		EgressUpdatedAtNs:                  entry.LastEgressUpdate.Load(),
		LastLatencyProbeAttemptNs:          entry.LastLatencyProbeAttempt.Load(),
		LastAuthorityLatencyProbeAttemptNs: entry.LastAuthorityLatencyProbeAttempt.Load(),
		LastEgressUpdateAttemptNs:          entry.LastEgressUpdateAttempt.Load(),
	}
}

func newFlushReaders(
	pool *topology.GlobalNodePool,
	subManager *topology.SubscriptionManager,
	router *routing.Router,
) state.CacheReaders {
	return state.CacheReaders{
		ReadNodeStatic: func(hash string) *model.NodeStatic {
			h, err := node.ParseHex(hash)
			if err != nil {
				return nil
			}
			entry, ok := pool.GetEntry(h)
			if !ok {
				return nil
			}
			return &model.NodeStatic{
				Hash:        hash,
				RawOptions:  append(json.RawMessage(nil), entry.RawOptions...),
				CreatedAtNs: entry.CreatedAt.UnixNano(),
			}
		},
		ReadNodeDynamic: func(hash string) *model.NodeDynamic {
			h, err := node.ParseHex(hash)
			if err != nil {
				return nil
			}
			entry, ok := pool.GetEntry(h)
			if !ok {
				return nil
			}
			dynamic := nodeDynamicModelFromEntry(hash, entry)
			return &dynamic
		},
		ReadNodeLatency: func(key model.NodeLatencyKey) *model.NodeLatency {
			h, err := node.ParseHex(key.NodeHash)
			if err != nil {
				return nil
			}
			entry, ok := pool.GetEntry(h)
			if !ok || entry.LatencyTable == nil {
				return nil
			}
			stats, ok := entry.LatencyTable.GetDomainStats(key.Domain)
			if !ok {
				return nil
			}
			return &model.NodeLatency{
				NodeHash:      key.NodeHash,
				Domain:        key.Domain,
				EwmaNs:        int64(stats.Ewma),
				LastUpdatedNs: stats.LastUpdated.UnixNano(),
			}
		},
		ReadLease: func(key model.LeaseKey) *model.Lease {
			return router.ReadLease(key)
		},
		ReadSubscriptionNode: func(key model.SubscriptionNodeKey) *model.SubscriptionNode {
			h, err := node.ParseHex(key.NodeHash)
			if err != nil {
				return nil
			}
			sub := subManager.Lookup(key.SubscriptionID)
			if sub == nil {
				return nil
			}
			managed, ok := sub.ManagedNodes().LoadNode(h)
			if !ok {
				return nil
			}
			return &model.SubscriptionNode{
				SubscriptionID: key.SubscriptionID,
				NodeHash:       key.NodeHash,
				Tags:           append([]string(nil), managed.Tags...),
				Evicted:        managed.Evicted,
			}
		},
	}
}

func buildAccountMatcher(engine *state.StateEngine) *proxy.AccountMatcherRuntime {
	rules, err := engine.ListAccountHeaderRules()
	if err != nil {
		log.Printf("Warning: load account header rules: %v", err)
		return proxy.NewAccountMatcherRuntime(proxy.BuildAccountMatcher(nil))
	}
	if len(rules) > 0 {
		log.Printf("Loaded %d account header rules", len(rules))
	}
	return proxy.NewAccountMatcherRuntime(proxy.BuildAccountMatcher(rules))
}

// --- Metrics runtime stats adapter ---

// runtimeStatsAdapter implements metrics.RuntimeStatsProvider using
// GlobalNodePool + Router.
type runtimeStatsAdapter struct {
	pool        *topology.GlobalNodePool
	router      *routing.Router
	authorities func() []string
}

func (a *runtimeStatsAdapter) TotalNodes() int { return a.pool.Size() }

func (a *runtimeStatsAdapter) HealthyNodes() int {
	count := 0
	isHealthyAndEnabled := a.pool.MakeHealthyAndEnabledEvaluator()
	a.pool.RangeNodes(func(_ node.Hash, entry *node.NodeEntry) bool {
		if isHealthyAndEnabled(entry) {
			count++
		}
		return true
	})
	return count
}

func (a *runtimeStatsAdapter) EgressIPCount() int {
	seen := make(map[netip.Addr]struct{})
	a.pool.RangeNodes(func(_ node.Hash, entry *node.NodeEntry) bool {
		if ip := entry.GetEgressIP(); ip.IsValid() {
			seen[ip] = struct{}{}
		}
		return true
	})
	return len(seen)
}

func (a *runtimeStatsAdapter) UniqueHealthyEgressIPCount() int {
	seen := make(map[netip.Addr]struct{})
	isHealthyAndEnabled := a.pool.MakeHealthyAndEnabledEvaluator()
	a.pool.RangeNodes(func(_ node.Hash, entry *node.NodeEntry) bool {
		if !isHealthyAndEnabled(entry) {
			return true
		}
		if ip := entry.GetEgressIP(); ip.IsValid() {
			seen[ip] = struct{}{}
		}
		return true
	})
	return len(seen)
}

func (a *runtimeStatsAdapter) LeaseCountsByPlatform() map[string]int {
	result := make(map[string]int)
	a.pool.RangePlatforms(func(plat *platform.Platform) bool {
		count := 0
		a.router.RangeLeases(plat.ID, func(_ string, _ routing.Lease) bool {
			count++
			return true
		})
		if count > 0 {
			result[plat.ID] = count
		}
		return true
	})
	return result
}

func (a *runtimeStatsAdapter) RoutableNodeCount(platformID string) (int, bool) {
	plat, ok := a.pool.GetPlatform(platformID)
	if !ok {
		return 0, false
	}
	return plat.View().Size(), true
}

func (a *runtimeStatsAdapter) PlatformEgressIPCount(platformID string) (int, bool) {
	plat, ok := a.pool.GetPlatform(platformID)
	if !ok {
		return 0, false
	}
	seen := make(map[netip.Addr]struct{})
	plat.View().Range(func(h node.Hash) bool {
		entry, ok := a.pool.GetEntry(h)
		if ok {
			if ip := entry.GetEgressIP(); ip.IsValid() {
				seen[ip] = struct{}{}
			}
		}
		return true
	})
	return len(seen), true
}

func (a *runtimeStatsAdapter) CollectNodeEWMAs(platformID string) []float64 {
	authorities := a.authorities()
	var ewmas []float64

	if platformID == "" {
		// Global: iterate all nodes.
		a.pool.RangeNodes(func(_ node.Hash, entry *node.NodeEntry) bool {
			if avg, ok := node.AverageEWMAForDomainsMs(entry, authorities); ok {
				ewmas = append(ewmas, avg)
			}
			return true
		})
	} else {
		// Platform-scoped: iterate only nodes routable by this platform.
		plat, ok := a.pool.GetPlatform(platformID)
		if !ok {
			return nil
		}
		plat.View().Range(func(h node.Hash) bool {
			entry, ok := a.pool.GetEntry(h)
			if ok {
				if avg, ok := node.AverageEWMAForDomainsMs(entry, authorities); ok {
					ewmas = append(ewmas, avg)
				}
			}
			return true
		})
	}
	return ewmas
}

// compositeEmitter dispatches proxy events to both requestlog and metrics.
type compositeEmitter struct {
	logSvc     *requestlog.Service
	metricsMgr *metrics.Manager
}

func (c compositeEmitter) EmitRequestFinished(ev proxy.RequestFinishedEvent) {
	c.metricsMgr.OnRequestFinished(ev)
}

func (c compositeEmitter) EmitRequestLog(ev proxy.RequestLogEntry) {
	c.logSvc.EmitRequestLog(ev)
}

func loadBootstrapNodeStatics(
	engine *state.StateEngine,
	pool *topology.GlobalNodePool,
	envCfg *config.EnvConfig,
) ([]node.Hash, error) {
	statics, err := engine.LoadAllNodesStatic()
	if err != nil {
		return nil, fmt.Errorf("load nodes_static: %w", err)
	}

	hashes := make([]node.Hash, 0, len(statics))
	bootstrapNowNs := time.Now().UnixNano()
	for _, ns := range statics {
		hash, err := node.ParseHex(ns.Hash)
		if err != nil {
			log.Printf("[bootstrap] skip node %s: %v", ns.Hash, err)
			continue
		}
		entry := &node.NodeEntry{
			Hash:       hash,
			RawOptions: append(json.RawMessage(nil), ns.RawOptions...),
			CreatedAt:  time.Unix(0, ns.CreatedAtNs),
		}
		// Bootstrap default: treat nodes as circuit-open unless a persisted
		// nodes_dynamic row later overrides this state.
		entry.CircuitOpenSince.Store(bootstrapNowNs)
		entry.LatencyTable = node.NewLatencyTable(envCfg.MaxLatencyTableEntries)
		pool.LoadNodeFromBootstrap(entry)
		hashes = append(hashes, hash)
	}
	log.Printf("Loaded %d static nodes from cache.db", len(statics))
	return hashes, nil
}

func warmupBootstrapOutbounds(
	hashes []node.Hash,
	outboundMgr *outbound.OutboundManager,
) {
	if len(hashes) == 0 {
		return
	}

	workers := runtime.GOMAXPROCS(0)
	if workers < 1 {
		workers = 1
	}
	hashCh := make(chan node.Hash, len(hashes))
	for _, h := range hashes {
		hashCh <- h
	}
	close(hashCh)

	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for h := range hashCh {
				outboundMgr.EnsureNodeOutbound(h)
			}
		}()
	}
	wg.Wait()
	log.Printf("Parallel outbound init complete (%d workers)", workers)
}

func restoreBootstrapSubscriptionBindings(
	engine *state.StateEngine,
	pool *topology.GlobalNodePool,
	subManager *topology.SubscriptionManager,
) error {
	subNodes, err := engine.LoadAllSubscriptionNodes()
	if err != nil {
		return fmt.Errorf("load subscription_nodes: %w", err)
	}

	// Group by subscription ID for batch processing.
	subNodeMap := make(map[string][]model.SubscriptionNode)
	for _, sn := range subNodes {
		subNodeMap[sn.SubscriptionID] = append(subNodeMap[sn.SubscriptionID], sn)
	}
	for subID, nodes := range subNodeMap {
		sub, ok := subManager.Get(subID)
		if !ok {
			log.Printf("[bootstrap] subscription %s not found, skipping %d node bindings", subID, len(nodes))
			continue
		}
		managed := subscription.NewManagedNodes()
		for _, sn := range nodes {
			hash, err := node.ParseHex(sn.NodeHash)
			if err != nil {
				continue
			}
			managed.StoreNode(hash, subscription.ManagedNode{
				Tags:    append([]string(nil), sn.Tags...),
				Evicted: sn.Evicted,
			})
			// Restore runtime hold references only for non-evicted rows.
			if !sn.Evicted {
				if entry, ok := pool.GetEntry(hash); ok {
					entry.AddSubscriptionID(subID)
				}
			}
		}
		sub.SwapManagedNodes(managed)
	}
	log.Printf("Loaded %d subscription-node bindings from cache.db", len(subNodes))
	return nil
}

func restoreBootstrapNodeDynamics(
	engine *state.StateEngine,
	pool *topology.GlobalNodePool,
) error {
	dynamics, err := engine.LoadAllNodesDynamic()
	if err != nil {
		return fmt.Errorf("load nodes_dynamic: %w", err)
	}

	for _, nd := range dynamics {
		hash, err := node.ParseHex(nd.Hash)
		if err != nil {
			continue
		}
		entry, ok := pool.GetEntry(hash)
		if !ok {
			continue
		}
		entry.FailureCount.Store(int32(nd.FailureCount))
		entry.CircuitOpenSince.Store(nd.CircuitOpenSince)
		entry.LastLatencyProbeAttempt.Store(nd.LastLatencyProbeAttemptNs)
		entry.LastAuthorityLatencyProbeAttempt.Store(nd.LastAuthorityLatencyProbeAttemptNs)
		entry.LastEgressUpdateAttempt.Store(nd.LastEgressUpdateAttemptNs)
		if nd.EgressIP != "" {
			if ip, err := netip.ParseAddr(nd.EgressIP); err == nil {
				entry.SetEgressIP(ip)
			}
		}
		if len(nd.EgressIPs) > 0 {
			entry.SetObservedEgressIPStrings(nd.EgressIPs)
		}
		entry.SetEgressRegion(nd.EgressRegion)
		entry.LastEgressUpdate.Store(nd.EgressUpdatedAtNs)
	}
	log.Printf("Loaded %d node dynamic states from cache.db", len(dynamics))
	return nil
}

func restoreBootstrapNodeLatencies(
	engine *state.StateEngine,
	pool *topology.GlobalNodePool,
	maxRegularEntries int,
	latencyAuthorities []string,
	latencies []model.NodeLatency,
) error {
	if latencies == nil {
		var err error
		latencies, err = engine.LoadAllNodeLatency()
		if err != nil {
			return fmt.Errorf("load node_latency: %w", err)
		}
	}

	if maxRegularEntries <= 0 {
		maxRegularEntries = 1
	}
	authoritySet := make(map[string]struct{}, len(latencyAuthorities))
	for _, authority := range latencyAuthorities {
		authority = strings.ToLower(strings.TrimSpace(authority))
		if authority == "" {
			continue
		}
		authoritySet[authority] = struct{}{}
	}
	isAuthority := func(domain string) bool {
		_, ok := authoritySet[strings.ToLower(strings.TrimSpace(domain))]
		return ok
	}

	byNode := make(map[string][]model.NodeLatency)
	for _, nl := range latencies {
		byNode[nl.NodeHash] = append(byNode[nl.NodeHash], nl)
	}

	loadedCount := 0
	trimmedCount := 0
	for nodeHash, rows := range byNode {
		hash, err := node.ParseHex(nodeHash)
		if err != nil {
			continue
		}
		entry, ok := pool.GetEntry(hash)
		if !ok || entry.LatencyTable == nil {
			continue
		}

		authorities := make([]model.NodeLatency, 0, len(rows))
		regular := make([]model.NodeLatency, 0, len(rows))
		for _, row := range rows {
			if isAuthority(row.Domain) {
				authorities = append(authorities, row)
			} else {
				regular = append(regular, row)
			}
		}
		sort.SliceStable(regular, func(i, j int) bool {
			if regular[i].LastUpdatedNs == regular[j].LastUpdatedNs {
				return regular[i].Domain < regular[j].Domain
			}
			return regular[i].LastUpdatedNs > regular[j].LastUpdatedNs
		})
		if len(regular) > maxRegularEntries {
			for _, dropped := range regular[maxRegularEntries:] {
				engine.MarkNodeLatencyDelete(dropped.NodeHash, dropped.Domain)
				trimmedCount++
			}
			regular = regular[:maxRegularEntries]
		}

		for _, row := range authorities {
			entry.LatencyTable.LoadEntryClassified(row.Domain, node.DomainLatencyStats{
				Ewma:        time.Duration(row.EwmaNs),
				LastUpdated: time.Unix(0, row.LastUpdatedNs),
			}, true)
			loadedCount++
		}
		// regular is sorted by LastUpdated desc (newest -> oldest).
		// Load in reverse so in-memory LRU order stays oldest -> newest.
		for i := len(regular) - 1; i >= 0; i-- {
			row := regular[i]
			entry.LatencyTable.LoadEntryClassified(row.Domain, node.DomainLatencyStats{
				Ewma:        time.Duration(row.EwmaNs),
				LastUpdated: time.Unix(0, row.LastUpdatedNs),
			}, false)
			loadedCount++
		}
	}
	log.Printf("Loaded %d latency entries from cache.db (trimmed=%d)", loadedCount, trimmedCount)
	return nil
}

func pruneColdBootstrapNodes(pool *topology.GlobalNodePool, subManager *topology.SubscriptionManager) {
	if pool == nil || subManager == nil {
		return
	}
	active := make(map[node.Hash]bool)
	pool.Range(func(hash node.Hash, entry *node.NodeEntry) bool {
		active[hash] = entry != nil && entry.HasOutbound() && !entry.IsCircuitOpen()
		return true
	})
	subManager.Range(func(_ string, sub *subscription.Subscription) bool {
		managed := subscription.NewManagedNodes()
		if sub != nil && sub.Enabled() {
			sub.ManagedNodes().RangeNodes(func(hash node.Hash, mn subscription.ManagedNode) bool {
				if !mn.Evicted && active[hash] {
					managed.StoreNode(hash, mn)
				}
				return true
			})
		}
		sub.SwapManagedNodes(managed)
		return true
	})
	pool.Range(func(hash node.Hash, entry *node.NodeEntry) bool {
		if entry == nil || !active[hash] {
			pool.DeleteNodeFromBootstrap(hash)
			return true
		}
		keep := false
		for _, subID := range entry.SubscriptionIDs() {
			if sub := subManager.Lookup(subID); sub != nil && sub.Enabled() {
				if mn, ok := sub.ManagedNodes().LoadNode(hash); ok && !mn.Evicted {
					keep = true
					break
				}
			}
		}
		if !keep {
			pool.DeleteNodeFromBootstrap(hash)
		}
		return true
	})
}

func enabledSubscriptionIDs(subManager *topology.SubscriptionManager) []string {
	if subManager == nil {
		return nil
	}
	ids := make([]string, 0)
	subManager.Range(func(id string, sub *subscription.Subscription) bool {
		if sub != nil && sub.Enabled() {
			ids = append(ids, id)
		}
		return true
	})
	sort.Strings(ids)
	return ids
}

func loadActiveOnlyBootstrapNodes(
	engine *state.StateEngine,
	pool *topology.GlobalNodePool,
	subManager *topology.SubscriptionManager,
	envCfg *config.EnvConfig,
	latencyAuthorities []string,
) ([]node.Hash, error) {
	records, err := engine.LoadBootstrapActiveNodes(enabledSubscriptionIDs(subManager))
	if err != nil {
		return nil, fmt.Errorf("load active bootstrap nodes: %w", err)
	}

	hashes := make([]node.Hash, 0, len(records))
	hashHexes := make([]string, 0, len(records))
	for _, record := range records {
		hash, err := node.ParseHex(record.Static.Hash)
		if err != nil {
			log.Printf("[bootstrap] skip active node %s: %v", record.Static.Hash, err)
			continue
		}
		entry := &node.NodeEntry{
			Hash:       hash,
			RawOptions: append(json.RawMessage(nil), record.Static.RawOptions...),
			CreatedAt:  time.Unix(0, record.Static.CreatedAtNs),
		}
		entry.LatencyTable = node.NewLatencyTable(envCfg.MaxLatencyTableEntries)
		entry.FailureCount.Store(int32(record.Dynamic.FailureCount))
		entry.CircuitOpenSince.Store(record.Dynamic.CircuitOpenSince)
		entry.LastLatencyProbeAttempt.Store(record.Dynamic.LastLatencyProbeAttemptNs)
		entry.LastAuthorityLatencyProbeAttempt.Store(record.Dynamic.LastAuthorityLatencyProbeAttemptNs)
		entry.LastEgressUpdateAttempt.Store(record.Dynamic.LastEgressUpdateAttemptNs)
		if record.Dynamic.EgressIP != "" {
			if ip, err := netip.ParseAddr(record.Dynamic.EgressIP); err == nil {
				entry.SetEgressIP(ip)
			}
		}
		if len(record.Dynamic.EgressIPs) > 0 {
			entry.SetObservedEgressIPStrings(record.Dynamic.EgressIPs)
		}
		entry.SetEgressRegion(record.Dynamic.EgressRegion)
		entry.LastEgressUpdate.Store(record.Dynamic.EgressUpdatedAtNs)

		managedBySub := make(map[string]subscription.ManagedNode, len(record.Relations))
		for _, relation := range record.Relations {
			if relation.Evicted {
				continue
			}
			if sub := subManager.Lookup(relation.SubscriptionID); sub != nil && sub.Enabled() {
				entry.AddSubscriptionID(relation.SubscriptionID)
				managedBySub[relation.SubscriptionID] = subscription.ManagedNode{
					Tags: append([]string(nil), relation.Tags...),
				}
			}
		}
		if entry.SubscriptionCount() == 0 {
			continue
		}

		pool.LoadNodeFromBootstrap(entry)
		for subID, managed := range managedBySub {
			if sub := subManager.Lookup(subID); sub != nil {
				sub.ManagedNodes().StoreNode(hash, managed)
			}
		}
		hashes = append(hashes, hash)
		hashHexes = append(hashHexes, record.Static.Hash)
	}

	latencies, err := engine.LoadNodeLatencyForHashes(hashHexes)
	if err != nil {
		return nil, fmt.Errorf("load active bootstrap latencies: %w", err)
	}
	if err := restoreBootstrapNodeLatencies(engine, pool, envCfg.MaxLatencyTableEntries, latencyAuthorities, latencies); err != nil {
		return nil, err
	}
	log.Printf("Loaded %d active bootstrap nodes from cache.db", len(hashes))
	return hashes, nil
}

// bootstrapNodes loads cached node data from persistence for bootstrap recovery.
// In active-only mode, DB filtering restores only persisted active candidates;
// legacy mode restores the full cache inventory before outbound warmup.
func bootstrapNodes(
	engine *state.StateEngine,
	pool *topology.GlobalNodePool,
	subManager *topology.SubscriptionManager,
	outboundMgr *outbound.OutboundManager,
	envCfg *config.EnvConfig,
	latencyAuthorities []string,
) error {
	if envCfg.ActiveOnlyRuntime {
		hashes, err := loadActiveOnlyBootstrapNodes(engine, pool, subManager, envCfg, latencyAuthorities)
		if err != nil {
			return err
		}
		warmupBootstrapOutbounds(hashes, outboundMgr)
		pruneColdBootstrapNodes(pool, subManager)
		return nil
	}

	hashes, err := loadBootstrapNodeStatics(engine, pool, envCfg)
	if err != nil {
		return err
	}

	if err := restoreBootstrapSubscriptionBindings(engine, pool, subManager); err != nil {
		return err
	}
	if err := restoreBootstrapNodeDynamics(engine, pool); err != nil {
		return err
	}
	if err := restoreBootstrapNodeLatencies(
		engine,
		pool,
		envCfg.MaxLatencyTableEntries,
		latencyAuthorities,
		nil,
	); err != nil {
		return err
	}
	warmupBootstrapOutbounds(hashes, outboundMgr)
	return nil
}
