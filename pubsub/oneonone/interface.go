package oneonone

import (
	"context"
	"github.com/berty/go-orbit-db/events"
	p2pcore "github.com/libp2p/go-libp2p-core"
)

const PROTOCOL = "ipfs-pubsub-direct-channel/v1"

type Event events.Event

type Channel interface {
	events.EmitterInterface
	ID() string
	Peers() []p2pcore.PeerID
	Connect(context.Context) error
	Send(context.Context, []byte) error
	Close() error
}