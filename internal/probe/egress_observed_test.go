package probe

import (
	"errors"
	"net/netip"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/Resinat/Resin/internal/node"
	"github.com/Resinat/Resin/internal/topology"
)

func TestProbeEgress_RecordsBoundedObservedIPsAfterCloudflareSuccess(t *testing.T) {
	pool := topology.NewGlobalNodePool(topology.PoolConfig{MaxConsecutiveFailures: func() int { return 3 }})
	hash := node.HashFromRawOptions([]byte(`{"type":"observed-egress"}`))
	pool.AddNodeFromSub(hash, []byte(`{"type":"observed-egress"}`), "sub1")
	entry, ok := pool.GetEntry(hash)
	if !ok {
		t.Fatal("entry not found")
	}
	storeOutbound(entry)

	var urlsMu sync.Mutex
	var urls []string
	mgr := NewProbeManager(ProbeConfig{
		Pool: pool,
		Fetcher: func(_ node.Hash, url string) ([]byte, time.Duration, error) {
			urlsMu.Lock()
			urls = append(urls, url)
			urlsMu.Unlock()
			switch url {
			case egressTraceURL:
				return []byte("ip=198.51.100.1\nloc=US\n"), time.Millisecond, nil
			case egressExtraIPURLs[0]:
				return []byte("198.51.100.2\n"), time.Millisecond, nil
			case egressExtraIPURLs[1]:
				return []byte("not-an-ip"), time.Millisecond, nil
			case egressExtraIPURLs[2]:
				return []byte("2001:db8::1"), time.Millisecond, nil
			default:
				t.Fatalf("unexpected URL %q", url)
			}
			return nil, 0, nil
		},
	})
	if got, status, err := mgr.performEgressProbe(hash); err != nil || status != egressProbeNoError || got != netip.MustParseAddr("198.51.100.1") {
		t.Fatalf("performEgressProbe got ip=%v status=%v err=%v", got, status, err)
	}
	wantURLs := append([]string{egressTraceURL}, egressExtraIPURLs...)
	if len(urls) != len(wantURLs) {
		t.Fatalf("fetch URLs: got %v, want %v", urls, wantURLs)
	}
	seenURLs := make(map[string]int, len(urls))
	for _, url := range urls {
		seenURLs[url]++
	}
	for _, url := range wantURLs {
		if seenURLs[url] != 1 {
			t.Fatalf("fetch URLs: got %v, want exactly one %q", urls, url)
		}
	}
	wantIPs := []string{"198.51.100.1", "198.51.100.2", "2001:db8::1"}
	if got := entry.GetObservedEgressIPStrings(); !reflect.DeepEqual(got, wantIPs) {
		t.Fatalf("observed IPs: got %v, want %v", got, wantIPs)
	}
}

func TestProbeEgress_DoesNotFetchExtrasWhenCloudflareParseFails(t *testing.T) {
	pool := topology.NewGlobalNodePool(topology.PoolConfig{MaxConsecutiveFailures: func() int { return 3 }})
	hash := node.HashFromRawOptions([]byte(`{"type":"observed-egress-fail"}`))
	pool.AddNodeFromSub(hash, []byte(`{"type":"observed-egress-fail"}`), "sub1")
	entry, ok := pool.GetEntry(hash)
	if !ok {
		t.Fatal("entry not found")
	}
	storeOutbound(entry)

	var urlsMu sync.Mutex
	var urls []string
	mgr := NewProbeManager(ProbeConfig{
		Pool: pool,
		Fetcher: func(_ node.Hash, url string) ([]byte, time.Duration, error) {
			urlsMu.Lock()
			urls = append(urls, url)
			urlsMu.Unlock()
			if url != egressTraceURL {
				return nil, 0, errors.New("extras should not be fetched")
			}
			return []byte("loc=US\n"), time.Millisecond, nil
		},
	})
	_, status, err := mgr.performEgressProbe(hash)
	if err == nil || status != egressProbeParseError {
		t.Fatalf("performEgressProbe status=%v err=%v, want parse error", status, err)
	}
	if !reflect.DeepEqual(urls, []string{egressTraceURL}) {
		t.Fatalf("fetch URLs: got %v, want only Cloudflare", urls)
	}
}
