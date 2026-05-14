package netutil

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/Resinat/Resin/internal/testutil"
	"github.com/sagernet/sing-box/adapter"
	M "github.com/sagernet/sing/common/metadata"
)

func TestHTTPGetViaOutbound_RequireStatusOK(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte("not found"))
	}))
	defer srv.Close()

	ob, err := (&testutil.StubOutboundBuilder{}).Build(nil)
	if err != nil {
		t.Fatalf("build outbound: %v", err)
	}
	_, _, err = HTTPGetViaOutbound(context.Background(), ob, srv.URL, OutboundHTTPOptions{
		RequireStatusOK: true,
	})
	if err == nil {
		t.Fatal("expected non-200 status to return error")
	}
	if !strings.Contains(err.Error(), "unexpected status 404") {
		t.Fatalf("expected status error, got: %v", err)
	}
}

func TestHTTPGetViaOutbound_AllowNon200(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte("probe-body"))
	}))
	defer srv.Close()

	ob, err := (&testutil.StubOutboundBuilder{}).Build(nil)
	if err != nil {
		t.Fatalf("build outbound: %v", err)
	}
	body, _, err := HTTPGetViaOutbound(context.Background(), ob, srv.URL, OutboundHTTPOptions{
		RequireStatusOK: false,
	})
	if err != nil {
		t.Fatalf("expected non-200 response to pass through, got: %v", err)
	}
	if string(body) != "probe-body" {
		t.Fatalf("unexpected body %q", string(body))
	}
}

func TestConnCloseHook_CloseIsIdempotentAndConcurrentSafe(t *testing.T) {
	client, server := net.Pipe()
	defer server.Close()

	var onCloseCount atomic.Int32
	hook := &connCloseHook{
		Conn: client,
		onClose: func() {
			onCloseCount.Add(1)
		},
	}

	const closers = 32
	var wg sync.WaitGroup
	wg.Add(closers)
	for i := 0; i < closers; i++ {
		go func() {
			defer wg.Done()
			_ = hook.Close()
		}()
	}
	wg.Wait()

	if got := onCloseCount.Load(); got != 1 {
		t.Fatalf("onClose called %d times, want 1", got)
	}
}

func TestHTTPGetViaOutbound_ClosesDialedConnOnReturn(t *testing.T) {
	ob := &trackingOutbound{
		serve: func(conn net.Conn) {
			defer conn.Close()
			buf := make([]byte, 4096)
			_, _ = conn.Read(buf)
			_, _ = conn.Write([]byte("HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok"))
		},
	}

	body, _, err := HTTPGetViaOutbound(context.Background(), ob, "http://example.test/", OutboundHTTPOptions{})
	if err != nil {
		t.Fatalf("HTTPGetViaOutbound returned error: %v", err)
	}
	if string(body) != "ok" {
		t.Fatalf("unexpected body %q", string(body))
	}
	if got := ob.closeCount.Load(); got != 1 {
		t.Fatalf("dialed conn closed %d times, want 1", got)
	}
}

type trackingOutbound struct {
	serve      func(net.Conn)
	closeCount atomic.Int32
}

func (o *trackingOutbound) Type() string { return "tracking" }
func (o *trackingOutbound) Tag() string  { return "tracking" }
func (o *trackingOutbound) Network() []string {
	return []string{"tcp"}
}
func (o *trackingOutbound) Dependencies() []string { return nil }
func (o *trackingOutbound) DialContext(context.Context, string, M.Socksaddr) (net.Conn, error) {
	client, server := net.Pipe()
	if o.serve != nil {
		go o.serve(server)
	} else {
		_ = server.Close()
	}
	return &trackingConn{Conn: client, closeCount: &o.closeCount}, nil
}
func (o *trackingOutbound) ListenPacket(context.Context, M.Socksaddr) (net.PacketConn, error) {
	return nil, net.ErrClosed
}
func (o *trackingOutbound) Close() error { return nil }

var _ adapter.Outbound = (*trackingOutbound)(nil)

type trackingConn struct {
	net.Conn
	closeOnce  sync.Once
	closeCount *atomic.Int32
}

func (c *trackingConn) Close() error {
	c.closeOnce.Do(func() {
		c.closeCount.Add(1)
	})
	return c.Conn.Close()
}
