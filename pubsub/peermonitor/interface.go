package peermonitor

import (
	"context"
	"github.com/berty/go-orbit-db/events"
	"github.com/libp2p/go-libp2p-core/peer"
)

type Interface interface {
	events.EmitterInterface
	Start(ctx context.Context) func()
	Stop()
	GetPeers() []peer.ID
	HasPeer(id peer.ID) bool
}
