package outbound

import (
	"log"
	"sync"

	"github.com/sagernet/sing-box/adapter"
)

const (
	asyncOutboundCloseWorkers = 64
	asyncOutboundCloseQueue   = 16384
)

var (
	asyncCloseOnce sync.Once
	asyncCloseCh   = make(chan adapter.Outbound, asyncOutboundCloseQueue)
)

func startAsyncOutboundCloseWorkers() {
	for i := 0; i < asyncOutboundCloseWorkers; i++ {
		go func() {
			for ob := range asyncCloseCh {
				closeOutbound(ob)
			}
		}()
	}
}

func closeOutboundAsync(ob adapter.Outbound, reason string) {
	if ob == nil {
		return
	}
	asyncCloseOnce.Do(startAsyncOutboundCloseWorkers)
	select {
	case asyncCloseCh <- ob:
	default:
		log.Printf("outbound: async close queue full; dropping %s close for type=%s tag=%s", reason, ob.Type(), ob.Tag())
	}
}
