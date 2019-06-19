package pubsub

import (
	"context"
	"github.com/berty/go-orbit-db/pubsub/peermonitor"
	iface "github.com/ipfs/interface-go-ipfs-core"
	"io"
)

type Subscription interface {
	io.Closer
	MessageChan() chan iface.PubSubMessage
	PeerChan() chan *peermonitor.Event
}

type Interface interface {
	Subscribe(ctx context.Context, topic string) (Subscription, error)
	Unsubscribe(topic string) error
	Disconnect()
	Publish(ctx context.Context, topic string, message []byte) error
}
