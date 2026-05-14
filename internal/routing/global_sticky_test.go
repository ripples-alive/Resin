package routing

import (
	"errors"
	"net/netip"
	"testing"
	"time"

	"github.com/Resinat/Resin/internal/platform"
)

func TestRouteRequest_StickyLeaseCreationRejectsOnlyGlobalEgressCandidates(t *testing.T) {
	pool := newRouterTestPool()
	plat := platform.NewPlatform("plat-global", "Plat-Global", nil, nil)
	plat.StickyTTLNs = int64(time.Hour)
	pool.addPlatform(plat)

	h, entry := newRoutableEntry(t, `{"id":"global-only"}`, "203.0.113.10")
	entry.SetObservedEgressIPs(netip.MustParseAddr("203.0.113.10"), []netip.Addr{netip.MustParseAddr("203.0.113.11")})
	pool.addEntry(h, entry)
	pool.rebuildPlatformView(plat)

	_, err := newTestRouter(pool, nil).RouteRequest(plat.Name, "acct", "https://example.com")
	if !errors.Is(err, ErrNoAvailableNodes) {
		t.Fatalf("RouteRequest sticky global-only error = %v, want ErrNoAvailableNodes", err)
	}
}

func TestRouteRequest_RandomCanUseGlobalEgressCandidate(t *testing.T) {
	pool := newRouterTestPool()
	plat := platform.NewPlatform("plat-global-random", "Plat-Global-Random", nil, nil)
	plat.StickyTTLNs = 0
	pool.addPlatform(plat)

	h, entry := newRoutableEntry(t, `{"id":"global-random"}`, "203.0.113.20")
	entry.SetObservedEgressIPs(netip.MustParseAddr("203.0.113.20"), []netip.Addr{netip.MustParseAddr("203.0.113.21")})
	pool.addEntry(h, entry)
	pool.rebuildPlatformView(plat)

	res, err := newTestRouter(pool, nil).RouteRequest(plat.Name, "", "https://example.com")
	if err != nil {
		t.Fatalf("RouteRequest random global: %v", err)
	}
	if res.NodeHash != h {
		t.Fatalf("random node hash = %s, want %s", res.NodeHash.Hex(), h.Hex())
	}
}
