package main

import (
	"testing"

	"github.com/Resinat/Resin/internal/config"
)

func TestShouldForceRefreshSubscriptionsAtStartup_ActiveOnlySkipsForceRefresh(t *testing.T) {
	envCfg := &config.EnvConfig{ActiveOnlyRuntime: true}
	if shouldForceRefreshSubscriptionsAtStartup(envCfg) {
		t.Fatal("active-only startup should skip force refresh")
	}
}

func TestShouldForceRefreshSubscriptionsAtStartup_LegacyRuntimeForcesRefresh(t *testing.T) {
	envCfg := &config.EnvConfig{ActiveOnlyRuntime: false}
	if !shouldForceRefreshSubscriptionsAtStartup(envCfg) {
		t.Fatal("legacy runtime should force startup refresh")
	}
}
