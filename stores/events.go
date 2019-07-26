package stores

import (
	"berty.tech/go-ipfs-log/entry"
	"github.com/berty/go-orbit-db/address"
	"github.com/berty/go-orbit-db/events"
	"github.com/berty/go-orbit-db/stores/replicator"
	"github.com/ipfs/go-cid"
	p2pcore "github.com/libp2p/go-libp2p-core"
	"go.uber.org/zap"
)

type Event events.Event

type EventReplicate struct {
	Address address.Address
	Entry   *entry.Entry
}

func NewEventReplicate(addr address.Address, e *entry.Entry) *EventReplicate {
	return &EventReplicate{
		Address: addr,
		Entry:   e,
	}
}

type EventReplicateProgress struct {
	Address           address.Address
	Hash              cid.Cid
	Entry             *entry.Entry
	ReplicationStatus replicator.ReplicationInfo
}

func NewEventReplicateProgress(addr address.Address, h cid.Cid, e *entry.Entry, replicationStatus replicator.ReplicationInfo) *EventReplicateProgress {
	return &EventReplicateProgress{
		Address:           addr,
		Hash:              h,
		Entry:             e,
		ReplicationStatus: replicationStatus,
	}
}

type EventReplicated struct {
	Address address.Address
	LogLength int
}

func NewEventReplicated(addr address.Address, logLength int) *EventReplicated {
	return &EventReplicated{
		Address: addr,
		LogLength: logLength,
	}
}

type EventLoad struct {
	Address address.Address
	Heads   []*entry.Entry
}

func NewEventLoad(addr address.Address, heads []*entry.Entry) *EventLoad {
	logger().Debug("emitting stores.load event", zap.String("addr", addr.String()))
	return &EventLoad{
		Address: addr,
		Heads:   heads,
	}
}

type EventLoadProgress struct {
	Address           address.Address
	Hash              cid.Cid
	Entry             *entry.Entry
	ReplicationStatus replicator.ReplicationInfo
}

func NewEventLoadProgress(addr address.Address, h cid.Cid, e *entry.Entry, replicationStatus replicator.ReplicationInfo) *EventLoadProgress {
	return &EventLoadProgress{
		Address:           addr,
		Hash:              h,
		Entry:             e,
		ReplicationStatus: replicationStatus,
	}
}

type EventReady struct {
	Address address.Address
	Heads   []*entry.Entry
}

func NewEventReady(addr address.Address, heads []*entry.Entry) *EventReady {
	logger().Debug("emitting stores.ready event", zap.String("addr", addr.String()))
	return &EventReady{
		Address: addr,
		Heads:   heads,
	}
}

type EventWrite struct {
	Address address.Address
	Entry   *entry.Entry
	Heads   []*entry.Entry
}

func NewEventWrite(addr address.Address, e *entry.Entry, heads []*entry.Entry) *EventWrite {
	logger().Debug("emitting stores.write event", zap.String("addr", addr.String()))
	return &EventWrite{
		Address: addr,
		Entry:   e,
		Heads:   heads,
	}
}

type EventClosed struct {
	Address address.Address
}

func NewEventClosed(addr address.Address) *EventClosed {
	logger().Debug("emitting stores.closed event", zap.String("addr", addr.String()))
	return &EventClosed{
		Address: addr,
	}
}

type EventNewPeer struct {
	Peer p2pcore.PeerID
}

func NewEventNewPeer(p p2pcore.PeerID) *EventNewPeer {
	return &EventNewPeer{
		Peer: p,
	}
}
