package main

import (
	"testing"
	"time"

	"github.com/Resinat/Resin/internal/config"
	"github.com/Resinat/Resin/internal/model"
	"github.com/Resinat/Resin/internal/state"
)

func TestBootstrapTopology_RestoresSubscriptionRefreshState(t *testing.T) {
	engine, closer, err := state.PersistenceBootstrap(t.TempDir(), t.TempDir())
	if err != nil {
		t.Fatalf("PersistenceBootstrap: %v", err)
	}
	t.Cleanup(func() { _ = closer.Close() })

	now := time.Now().UnixNano()
	if err := engine.UpsertSubscription(model.Subscription{
		ID: "sub-refresh", Name: "RefreshSub", URL: "https://example.com/sub",
		UpdateIntervalNs: int64(time.Hour), Enabled: true,
		Ephemeral: false, EphemeralNodeEvictDelayNs: int64(72 * time.Hour),
		CreatedAtNs: now, UpdatedAtNs: now,
	}); err != nil {
		t.Fatalf("UpsertSubscription: %v", err)
	}
	lastUpdatedNs := int64(456)
	if err := engine.UpdateSubscriptionRefreshState("sub-refresh", 123, &lastUpdatedNs, "previous failure"); err != nil {
		t.Fatalf("UpdateSubscriptionRefreshState: %v", err)
	}

	runtimeCfg := config.NewDefaultRuntimeConfig()
	subManager, pool := newBootstrapTestRuntime(runtimeCfg)
	if err := bootstrapTopology(engine, subManager, pool, newDefaultPlatformEnvConfig()); err != nil {
		t.Fatalf("bootstrapTopology: %v", err)
	}

	sub := subManager.Lookup("sub-refresh")
	if sub == nil {
		t.Fatal("expected subscription restored")
	}
	if sub.LastCheckedNs.Load() != 123 || sub.LastUpdatedNs.Load() != 456 || sub.GetLastError() != "previous failure" {
		t.Fatalf("refresh state not restored: checked=%d updated=%d err=%q", sub.LastCheckedNs.Load(), sub.LastUpdatedNs.Load(), sub.GetLastError())
	}
}
