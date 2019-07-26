package pubsub

import (
	"context"
	"github.com/berty/go-orbit-db/events"
	"io"
)

type Subscription interface {
	events.EmitterInterface
	io.Closer
}

type Interface interface {
	Subscribe(ctx context.Context, topic string) (Subscription, error)
	Unsubscribe(topic string) error
	Close() error
	Publish(ctx context.Context, topic string, message []byte) error
}
