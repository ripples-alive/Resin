package node

import (
	"net/netip"
	"reflect"
	"testing"
	"time"
)

func TestNodeEntryObservedEgressIPsDedupesCapsAndDetectsGlobal(t *testing.T) {
	e := NewNodeEntry(HashFromRawOptions([]byte(`{"id":"observed"}`)), []byte(`{"id":"observed"}`), time.Now(), 0)

	primary := netip.MustParseAddr("203.0.113.1")
	e.SetObservedEgressIPs(primary, []netip.Addr{
		primary,
		netip.MustParseAddr("203.0.113.2"),
		netip.Addr{},
		netip.MustParseAddr("2001:db8::1"),
		netip.MustParseAddr("2001:db8::2"),
	})

	want := []string{"203.0.113.1", "203.0.113.2", "2001:db8::1", "2001:db8::2"}
	if got := e.GetObservedEgressIPStrings(); !reflect.DeepEqual(got, want) {
		t.Fatalf("observed egress IPs: got %v, want %v", got, want)
	}
	if !e.HasGlobalEgress() {
		t.Fatal("multiple distinct IPv4/IPv6 observations should classify as global")
	}
	if got := e.GetRegion(nil); got != "global" {
		t.Fatalf("global region: got %q, want global", got)
	}
}

func TestNodeEntryObservedEgressIPsOneIPv4AndOneIPv6IsNotGlobal(t *testing.T) {
	e := NewNodeEntry(HashFromRawOptions([]byte(`{"id":"dual-stack"}`)), []byte(`{"id":"dual-stack"}`), time.Now(), 0)
	primary := netip.MustParseAddr("203.0.113.1")
	e.SetObservedEgressIPs(primary, []netip.Addr{netip.MustParseAddr("2001:db8::1")})
	if e.HasGlobalEgress() {
		t.Fatal("one IPv4 plus one IPv6 alone must not classify as global")
	}
}
