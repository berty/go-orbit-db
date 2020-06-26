package pubsub

import "berty.tech/go-orbit-db/events"

// MessageEvent Indicates a new message posted on a pubsub topic
type MessageEvent struct {
	Topic   string
	Content []byte
}

// Creates a new Message event
func NewMessageEvent(topic string, content []byte) events.Event {
	return &MessageEvent{
		Topic:   topic,
		Content: content,
	}
}

// EventPayload An event received on new messages
type EventPayload struct {
	Payload []byte
}

// NewEventPayload Creates a new Message event
func NewEventPayload(payload []byte) *EventPayload {
	return &EventPayload{
		Payload: payload,
	}
}
