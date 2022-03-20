package baseorbitdb

import "github.com/libp2p/go-libp2p-core/peer"

type EventExchangeHeads struct {
	Peer    peer.ID
	Message *MessageExchangeHeads
}

func NewEventExchangeHeads(p peer.ID, msg *MessageExchangeHeads) EventExchangeHeads {
	return EventExchangeHeads{
		Peer:    p,
		Message: msg,
	}
}
