package baseorbitdb

import (
	"github.com/stateless-minds/go-orbit-db/iface"
	"github.com/libp2p/go-libp2p/core/peer"
)

type EventExchangeHeads struct {
	Peer    peer.ID
	Message *iface.MessageExchangeHeads
}

func NewEventExchangeHeads(p peer.ID, msg *iface.MessageExchangeHeads) EventExchangeHeads {
	return EventExchangeHeads{
		Peer:    p,
		Message: msg,
	}
}
