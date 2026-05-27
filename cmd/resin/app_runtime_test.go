package main

import (
	"testing"

	"github.com/Resinat/Resin/internal/config"
)

func TestShouldForceRefreshSubscriptionsAtStartup(t *testing.T) {
	if !shouldForceRefreshSubscriptionsAtStartup(&config.EnvConfig{}) {
		t.Fatal("default startup should preserve force refresh behavior")
	}
	if shouldForceRefreshSubscriptionsAtStartup(&config.EnvConfig{ActiveOnlyBootstrap: true}) {
		t.Fatal("active-only bootstrap should skip startup force refresh")
	}
}
