package replicator

import (
	"context"

	ipfslog "berty.tech/go-ipfs-log"
	"berty.tech/go-ipfs-log/identityprovider"
	"berty.tech/go-orbit-db/accesscontroller"
	coreapi "github.com/ipfs/boxo/coreiface"
	cid "github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/event"
)

// storeInterface An interface used to avoid import cycles
type storeInterface interface {
	OpLog() ipfslog.Log
	IPFS() coreapi.CoreAPI
	Identity() *identityprovider.Identity
	AccessController() accesscontroller.Interface
	SortFn() ipfslog.SortFn
	IO() ipfslog.IO
}

// Replicator Replicates stores information among peers
type Replicator interface {
	// Stop Stops the replication
	Stop()

	// Load Loads new data to replicate
	Load(ctx context.Context, heads []ipfslog.Entry)

	// GetQueue Returns the list of CID in the queue
	GetQueue() []cid.Cid

	EventBus() event.Bus
}

// ReplicationInfo Holds information about the current replication state
type ReplicationInfo interface {
	// GetProgress Get the value of progress
	GetProgress() int

	// GetProgress Get the value of progress
	SetProgress(i int)

	// GetMax Get the value of max
	GetMax() int

	// SetMax Sets the value of max
	SetMax(i int)

	// Reset Resets all values to 0
	Reset()
}
