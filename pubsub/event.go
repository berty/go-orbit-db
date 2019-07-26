package pubsub

import "github.com/berty/go-orbit-db/events"

type Event events.Event

type MessageEvent struct {
	Topic   string
	Content []byte
}

func NewMessageEvent(topic string, content []byte) Event {
	return &MessageEvent{
		Topic:   topic,
		Content: content,
	}
}
