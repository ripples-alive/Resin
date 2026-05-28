package main

import "testing"

func TestShouldForceRefreshSubscriptionsAtStartup(t *testing.T) {
	if shouldForceRefreshSubscriptionsAtStartup() {
		t.Fatal("catalog-mode startup should skip force refresh by default")
	}
}
