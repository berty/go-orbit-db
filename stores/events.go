package stores

import (
	ipfslog "berty.tech/go-ipfs-log"
	"berty.tech/go-orbit-db/address"
	"berty.tech/go-orbit-db/stores/replicator"
	cid "github.com/ipfs/go-cid"
	p2pcore "github.com/libp2p/go-libp2p-core"
)

type EventReplicate struct {
	Address address.Address
	Hash    cid.Cid
}

func NewEventReplicate(addr address.Address, c cid.Cid) *EventReplicate {
	return &EventReplicate{
		Address: addr,
		Hash:    c,
	}
}

// EventReplicateProgress An event containing the current replication progress.
type EventReplicateProgress struct {
	Address           address.Address
	Hash              cid.Cid
	Entry             ipfslog.Entry
	ReplicationStatus replicator.ReplicationInfo
}

// NewEventReplicateProgress Creates a new EventReplicateProgress event
func NewEventReplicateProgress(addr address.Address, h cid.Cid, e ipfslog.Entry, replicationStatus replicator.ReplicationInfo) *EventReplicateProgress {
	return &EventReplicateProgress{
		Address:           addr,
		Hash:              h,
		Entry:             e,
		ReplicationStatus: replicationStatus,
	}
}

// EventReplicated An event sent when data has been replicated
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

// EventLoad An event sent when data has been loaded
type EventLoad struct {
	Address address.Address
	Heads   []ipfslog.Entry
}

// NewEventLoad Creates a new EventLoad event
func NewEventLoad(addr address.Address, heads []ipfslog.Entry) *EventLoad {
	return &EventLoad{
		Address: addr,
		Heads:   heads,
	}
}

//type EventLoadProgress struct {
//	Address           address.Address
//	Hash              cid.Cid
//	Entry             ipfslog.Entry
//	ReplicationStatus replicator.ReplicationInfo
//}
//
//func NewEventLoadProgress(addr address.Address, h cid.Cid, e ipfslog.Entry, replicationStatus replicator.ReplicationInfo) *EventLoadProgress {
//	return &EventLoadProgress{
//		Address:           addr,
//		Hash:              h,
//		Entry:             e,
//		ReplicationStatus: replicationStatus,
//	}
//}

// EventReady An event sent when the store is ready
type EventReady struct {
	Address address.Address
	Heads   []ipfslog.Entry
}

// NewEventReady Creates a new EventReady event
func NewEventReady(addr address.Address, heads []ipfslog.Entry) *EventReady {
	return &EventReady{
		Address: addr,
		Heads:   heads,
	}
}

// EventWrite An event sent when something has been written
type EventWrite struct {
	Address address.Address
	Entry   ipfslog.Entry
	Heads   []ipfslog.Entry
}

// NewEventWrite Creates a new EventWrite event
func NewEventWrite(addr address.Address, e ipfslog.Entry, heads []ipfslog.Entry) *EventWrite {
	return &EventWrite{
		Address: addr,
		Entry:   e,
		Heads:   heads,
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
