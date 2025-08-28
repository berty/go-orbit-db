package stores

import (
	ipfslog "github.com/stateless-minds/go-ipfs-log"
	"github.com/stateless-minds/go-orbit-db/address"
	"github.com/stateless-minds/go-orbit-db/stores/replicator"
	cid "github.com/ipfs/go-cid"
	peer "github.com/libp2p/go-libp2p/core/peer"
)

var Events = []interface{}{
	new(EventWrite),
	new(EventReady),
	new(EventReplicateProgress),
	new(EventLoad),
	new(EventReplicated),
	new(EventReplicate),
}

type EventReplicate struct {
	Address address.Address
	Hash    cid.Cid
}

func NewEventReplicate(addr address.Address, c cid.Cid) EventReplicate {
	return EventReplicate{
		Address: addr,
		Hash:    c,
	}
}

// EventReplicateProgress An event containing the current replication progress.
type EventReplicateProgress struct {
	Max               int
	Progress          int
	Address           address.Address
	Hash              cid.Cid
	Entry             ipfslog.Entry
	ReplicationStatus replicator.ReplicationInfo
}

// NewEventReplicateProgress Creates a new EventReplicateProgress event
func NewEventReplicateProgress(addr address.Address, h cid.Cid, e ipfslog.Entry, replicationStatus replicator.ReplicationInfo) EventReplicateProgress {
	return EventReplicateProgress{
		Max:      replicationStatus.GetMax(),
		Progress: replicationStatus.GetProgress(),
		Address:  addr,
		Hash:     h,
		Entry:    e,
	}
}

// EventReplicated An event sent when data has been replicated
type EventReplicated struct {
	Address   address.Address
	LogLength int
	Entries   []ipfslog.Entry
}

// NewEventReplicated Creates a new EventReplicated event
func NewEventReplicated(addr address.Address, entries []ipfslog.Entry, logLength int) EventReplicated {
	return EventReplicated{
		Address:   addr,
		LogLength: logLength,
		Entries:   entries,
	}
}

// EventLoad An event sent when data has been loaded
type EventLoad struct {
	Address address.Address
	Heads   []ipfslog.Entry
}

// NewEventLoad Creates a new EventLoad event
func NewEventLoad(addr address.Address, heads []ipfslog.Entry) EventLoad {
	return EventLoad{
		Address: addr,
		Heads:   heads,
	}
}

type EventLoadProgress struct {
	Address       address.Address
	Hash          cid.Cid
	Entry         ipfslog.Entry
	progress, max int
}

func NewEventLoadProgress(addr address.Address, h cid.Cid, e ipfslog.Entry, progress, max int) EventLoadProgress {
	return EventLoadProgress{
		Address:  addr,
		Hash:     h,
		Entry:    e,
		progress: progress, max: max,
	}
}

// EventReady An event sent when the store is ready
type EventReady struct {
	Address address.Address
	Heads   []ipfslog.Entry
}

// NewEventReady Creates a new EventReady event
func NewEventReady(addr address.Address, heads []ipfslog.Entry) EventReady {
	return EventReady{
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
func NewEventWrite(addr address.Address, e ipfslog.Entry, heads []ipfslog.Entry) EventWrite {
	return EventWrite{
		Address: addr,
		Entry:   e,
		Heads:   heads,
	}
}

// EventNewPeer An event sent when a new peer is discovered on the pubsub channel
type EventNewPeer struct {
	Peer peer.ID
}

// NewEventNewPeer Creates a new EventNewPeer event
func NewEventNewPeer(p peer.ID) EventNewPeer {
	return EventNewPeer{
		Peer: p,
	}
}
