package pubsub

import "github.com/berty/go-orbit-db/events"

type Event events.Event

// MessageEvent Indicates a new message posted on a pubsub topic
type MessageEvent struct {
	Topic   string
	Content []byte
}

// Creates a new Message event
func NewMessageEvent(topic string, content []byte) Event {
	return &MessageEvent{
		Topic:   topic,
		Content: content,
	}
}
