package pubsub

import (
	"github.com/libp2p/go-libp2p-core/peer"

	"berty.tech/go-orbit-db/events"
	"berty.tech/go-orbit-db/iface"
)

// Creates a new Message event
func NewEventMessage(content []byte) *iface.EventPubSubMessage {
	return &iface.EventPubSubMessage{
		Content: content,
	}
}

// NewEventPayload Creates a new Message event
func NewEventPayload(payload []byte) *iface.EventPubSubPayload {
	return &iface.EventPubSubPayload{
		Payload: payload,
	}
}

// NewEventPeerJoin creates a new EventPubSubJoin event
func NewEventPeerJoin(p peer.ID) events.Event {
	return &iface.EventPubSubJoin{
		Peer: p,
	}
}

// NewEventPeerLeave creates a new EventPubSubLeave event
func NewEventPeerLeave(p peer.ID) events.Event {
	return &iface.EventPubSubLeave{
		Peer: p,
	}
}
