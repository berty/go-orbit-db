package pubsub

import (
	"github.com/libp2p/go-libp2p-core/event"
	"github.com/libp2p/go-libp2p-core/peer"

	"berty.tech/go-orbit-db/iface"
)

type Event interface{}

type PubSubPayloadEmitter struct {
	event.Emitter
}

func NewPubSubPayloadEmitter(bus event.Bus) (*PubSubPayloadEmitter, error) {
	emitter, err := bus.Emitter(new(iface.EventPubSubPayload))
	if err != nil {
		return nil, err
	}

	return &PubSubPayloadEmitter{emitter}, nil

}

func (e *PubSubPayloadEmitter) Emit(evt *iface.EventPubSubPayload) error {
	return e.Emitter.Emit(*evt)
}

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
func NewEventPeerJoin(p peer.ID) Event {
	return &iface.EventPubSubJoin{
		Peer: p,
	}
}

// NewEventPeerLeave creates a new EventPubSubLeave event
func NewEventPeerLeave(p peer.ID) Event {
	return &iface.EventPubSubLeave{
		Peer: p,
	}
}
