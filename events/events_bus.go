package events

import (
	"github.com/libp2p/go-libp2p-core/event"
)

type EventBus struct {
	b event.Bus
}
