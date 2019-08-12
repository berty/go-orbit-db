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
