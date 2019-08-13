package peermonitor

import (
	"context"

	"berty.tech/go-orbit-db/events"
	"github.com/libp2p/go-libp2p-core/peer"
)

// Interface Watches for peers on the pub sub, emits messages on join/leave
type Interface interface {
	events.EmitterInterface

	// Start Starts watching the topic for new joins or leaves
	Start(ctx context.Context) func()

	// Stop Stops the watcher
	Stop()

	// GetPeers Lists peers currently present on the topic
	GetPeers() []peer.ID

	// HasPeer Checks if a peer is present on the topic
	HasPeer(id peer.ID) bool
}
