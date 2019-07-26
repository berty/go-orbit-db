package peermonitor

import (
	"github.com/berty/go-orbit-db/events"
	"github.com/libp2p/go-libp2p-core/peer"
)

type Event events.Event

type EventPeerJoin struct {
	Peer peer.ID
}

type EventPeerLeave struct {
	Peer peer.ID
}

func NewEventPeerJoin(p peer.ID) *EventPeerJoin {
	return &EventPeerJoin{
		Peer: p,
	}
}

func NewEventPeerLeave(p peer.ID) *EventPeerLeave {
	return &EventPeerLeave{
		Peer: p,
	}
}
