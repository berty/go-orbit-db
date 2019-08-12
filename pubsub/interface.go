package pubsub

import (
	"context"
	"io"

	"berty.tech/go-orbit-db/events"
)

// Subscription is a pub sub subscription to a topic
type Subscription interface {
	events.EmitterInterface
	io.Closer
}

type Interface interface {
	// Subscribe Subscribes to a topic
	Subscribe(ctx context.Context, topic string) (Subscription, error)

	// Unsubscribe Unsubscribe from a topic
	Unsubscribe(topic string) error

	// Close Unsubscribe from all topics
	Close() error

	// Publish Posts a new message on a topic
	Publish(ctx context.Context, topic string, message []byte) error
}
