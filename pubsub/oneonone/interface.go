package oneonone

import (
	"context"
	"github.com/berty/go-orbit-db/events"
	p2pcore "github.com/libp2p/go-libp2p-core"
)

const PROTOCOL = "ipfs-pubsub-direct-channel/v1"

// Channel Channel is a pubsub used for a direct communication between peers
// new messages are received via events
type Channel interface {
	events.EmitterInterface

	// ID Returns the Channel ID (pubsub topic)
	ID() string

	// Peers Returns the lists of peers expected to be in the channel
	Peers() []p2pcore.PeerID

	// Connect Waits for the other peer to be connected
	Connect(context.Context) error

	// Sends Sends a message to the other peer
	Send(context.Context, []byte) error

	// Close Closes the connection
	Close() error
}
