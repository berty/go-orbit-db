package stores

import (
	"berty.tech/go-ipfs-log/entry"
	"github.com/berty/go-orbit-db/address"
	"github.com/berty/go-orbit-db/stores/replicator"
	"github.com/ipfs/go-cid"
	p2pcore "github.com/libp2p/go-libp2p-core"
)

//type EventReplicate struct {
//	Address address.Address
//	Entry   *entry.Entry
//}
//
//func NewEventReplicate(addr address.Address, e *entry.Entry) *EventReplicate {
//	return &EventReplicate{
//		Address: addr,
//		Entry:   e,
//	}
//}

// EventReplicateProgress An event containing the current replication progress.
type EventReplicateProgress struct {
	Address           address.Address
	Hash              cid.Cid
	Entry             *entry.Entry
	ReplicationStatus replicator.ReplicationInfo
}

// NewEventReplicateProgress Creates a new EventReplicateProgress event
func NewEventReplicateProgress(addr address.Address, h cid.Cid, e *entry.Entry, replicationStatus replicator.ReplicationInfo) *EventReplicateProgress {
	return &EventReplicateProgress{
		Address:           addr,
		Hash:              h,
		Entry:             e,
		ReplicationStatus: replicationStatus,
	}
}

// EventNewPeer An event sent when data has been replicated
type EventReplicated struct {
	Address   address.Address
	LogLength int
}

// NewEventReplicated Creates a new EventReplicated event
func NewEventReplicated(addr address.Address, logLength int) *EventReplicated {
	return &EventReplicated{
		Address:   addr,
		LogLength: logLength,
	}
}

// EventNewPeer An event sent when data has been loaded
type EventLoad struct {
	Address address.Address
	Heads   []*entry.Entry
}

// NewEventLoad Creates a new EventLoad event
func NewEventLoad(addr address.Address, heads []*entry.Entry) *EventLoad {
	return &EventLoad{
		Address: addr,
		Heads:   heads,
	}
}

//type EventLoadProgress struct {
//	Address           address.Address
//	Hash              cid.Cid
//	Entry             *entry.Entry
//	ReplicationStatus replicator.ReplicationInfo
//}
//
//func NewEventLoadProgress(addr address.Address, h cid.Cid, e *entry.Entry, replicationStatus replicator.ReplicationInfo) *EventLoadProgress {
//	return &EventLoadProgress{
//		Address:           addr,
//		Hash:              h,
//		Entry:             e,
//		ReplicationStatus: replicationStatus,
//	}
//}

// EventNewPeer An event sent when the store is ready
type EventReady struct {
	Address address.Address
	Heads   []*entry.Entry
}

// NewEventReady Creates a new EventReady event
func NewEventReady(addr address.Address, heads []*entry.Entry) *EventReady {
	return &EventReady{
		Address: addr,
		Heads:   heads,
	}
}

// EventNewPeer An event sent when something has been written
type EventWrite struct {
	Address address.Address
	Entry   *entry.Entry
	Heads   []*entry.Entry
}

// NewEventWrite Creates a new EventWrite event
func NewEventWrite(addr address.Address, e *entry.Entry, heads []*entry.Entry) *EventWrite {
	return &EventWrite{
		Address: addr,
		Entry:   e,
		Heads:   heads,
	}
}

// EventNewPeer An event sent when the store is closed
type EventClosed struct {
	Address address.Address
}

// NewEventClosed Creates a new EventClosed event
func NewEventClosed(addr address.Address) *EventClosed {
	return &EventClosed{
		Address: addr,
	}
}

// EventNewPeer An event sent when a new peer is discovered on the pubsub channel
type EventNewPeer struct {
	Peer p2pcore.PeerID
}

// NewEventNewPeer Creates a new EventNewPeer event
func NewEventNewPeer(p p2pcore.PeerID) *EventNewPeer {
	return &EventNewPeer{
		Peer: p,
	}
}
