package main

import (
	"testing"

	"github.com/Resinat/Resin/internal/config"
)

func TestShouldForceRefreshSubscriptionsAtStartup_CatalogFirstSkipsForceRefresh(t *testing.T) {
	envCfg := &config.EnvConfig{CatalogFirstRuntime: true}
	if shouldForceRefreshSubscriptionsAtStartup(envCfg) {
		t.Fatal("catalog-first startup should skip force refresh")
	}
}

func TestShouldForceRefreshSubscriptionsAtStartup_LegacyRuntimeForcesRefresh(t *testing.T) {
	envCfg := &config.EnvConfig{CatalogFirstRuntime: false}
	if !shouldForceRefreshSubscriptionsAtStartup(envCfg) {
		t.Fatal("legacy runtime should force startup refresh")
	}
}
