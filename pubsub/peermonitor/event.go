package peermonitor

import (
	"berty.tech/go-orbit-db/events"
	"github.com/libp2p/go-libp2p-core/peer"
)

// EventPeerJoin Is an event triggered when a peer joins the channel
type EventPeerJoin struct {
	Peer peer.ID
}

// EventPeerLeave Is an event triggered when a peer leave the channel
type EventPeerLeave struct {
	Peer peer.ID
}

// NewEventPeerJoin creates a new EventPeerJoin event
func NewEventPeerJoin(p peer.ID) events.Event {
	return &EventPeerJoin{
		Peer: p,
	}
}

// NewEventPeerLeave creates a new EventPeerLeave event
func NewEventPeerLeave(p peer.ID) events.Event {
	return &EventPeerLeave{
		Peer: p,
	}
}
