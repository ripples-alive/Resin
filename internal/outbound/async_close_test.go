package outbound

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/sagernet/sing-box/adapter"
	M "github.com/sagernet/sing/common/metadata"
)

type closeCountingOutbound struct {
	closed atomic.Int32
}

func (o *closeCountingOutbound) Type() string { return "counting" }
func (o *closeCountingOutbound) Tag() string  { return "counting" }
func (o *closeCountingOutbound) Network() []string {
	return []string{"tcp", "udp"}
}
func (o *closeCountingOutbound) Dependencies() []string { return nil }
func (o *closeCountingOutbound) DialContext(context.Context, string, M.Socksaddr) (net.Conn, error) {
	return nil, errors.New("counting outbound: dial not supported")
}
func (o *closeCountingOutbound) ListenPacket(context.Context, M.Socksaddr) (net.PacketConn, error) {
	return nil, errors.New("counting outbound: listen packet not supported")
}
func (o *closeCountingOutbound) Close() error {
	o.closed.Add(1)
	return nil
}

func TestCloseOutboundAsyncClosesSynchronouslyWhenQueueFull(t *testing.T) {
	oldCh := asyncCloseCh
	asyncCloseCh = make(chan adapter.Outbound, 1)
	asyncCloseOnce = sync.Once{}
	asyncCloseOnce.Do(func() {}) // mark workers as already started so the test can keep the queue full deterministically
	t.Cleanup(func() {
		asyncCloseCh = oldCh
		asyncCloseOnce = sync.Once{}
	})

	queued := &closeCountingOutbound{}
	asyncCloseCh <- queued

	fallback := &closeCountingOutbound{}
	closeOutboundAsync(fallback, "test queue full")

	if got := fallback.closed.Load(); got != 1 {
		t.Fatalf("fallback outbound Close calls = %d, want 1", got)
	}
	if got := queued.closed.Load(); got != 0 {
		t.Fatalf("queued outbound Close calls = %d, want 0 before workers drain", got)
	}
}
