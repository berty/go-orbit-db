package peermonitor

import (
	"context"
	"github.com/libp2p/go-libp2p-core/peer"
)

type Interface interface {
	Start(ctx context.Context) func()
	Stop()
	GetPeers() []peer.ID
	HasPeer(id peer.ID) bool
}

type EventAction uint64

const (
	_                           = iota
	EventActionJoin EventAction = iota
	EventActionLeave
)

type Event struct {
	Peer   peer.ID
	Action EventAction
}
