// iface package containing common structures and interfaces of orbitdb
package iface

import (
	ipfslog "berty.tech/go-ipfs-log"
	"berty.tech/go-ipfs-log/entry"
	"berty.tech/go-ipfs-log/identityprovider"
	"berty.tech/go-ipfs-log/keystore"
	"context"
	"github.com/berty/go-orbit-db/accesscontroller"
	"github.com/berty/go-orbit-db/address"
	"github.com/berty/go-orbit-db/events"
	"github.com/berty/go-orbit-db/stores/operation"
	"github.com/berty/go-orbit-db/stores/replicator"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	coreapi "github.com/ipfs/interface-go-ipfs-core"
)

// CreateDBOptions lists the arguments to create a store
type CreateDBOptions struct {
	Directory               *string
	Overwrite               *bool
	LocalOnly               *bool
	Create                  *bool
	StoreType               *string
	AccessControllerAddress string
	AccessController        accesscontroller.Interface
	Replicate               *bool
	Keystore                *keystore.Keystore
	Cache                   datastore.Datastore
	Identity                *identityprovider.Identity
}

// DetermineAddressOptions Lists the arguments used to determine a store address
type DetermineAddressOptions struct {
	OnlyHash         *bool
	Replicate        *bool
	AccessController accesscontroller.Interface
}

// OrbitDB Provides the main OrbitDB interface used to open and create stores
type OrbitDB interface {
	// IPFS Returns the instance of the IPFS API used by the current DB
	IPFS() coreapi.CoreAPI

	// Identity Returns the identity used by the current DB
	Identity() *identityprovider.Identity

	// Open Opens an existing data store
	Open(ctx context.Context, dbAddress string, options *CreateDBOptions) (Store, error)

	// Log Creates or opens an EventLogStore
	Log(ctx context.Context, address string, options *CreateDBOptions) (EventLogStore, error)

	// KeyValue Creates or opens an KeyValueStore
	KeyValue(ctx context.Context, address string, options *CreateDBOptions) (KeyValueStore, error)

	// Create Creates a new store
	Create(ctx context.Context, name string, storeType string, options *CreateDBOptions) (Store, error)

	// Close Closes the current DB and all the related stores
	Close() error

	// DetermineAddress Returns the store address for the given parameters
	DetermineAddress(ctx context.Context, name string, storeType string, options *DetermineAddressOptions) (address.Address, error)
}

// StreamOptions Defines the parameters that can be given to the Stream function of an EventLogStore
type StreamOptions struct {
	GT     *cid.Cid
	GTE    *cid.Cid
	LT     *cid.Cid
	LTE    *cid.Cid
	Amount *int
}

// Store Defines the operations common to all stores types
type Store interface {
	events.EmitterInterface

	// Close Closes the store
	Close() error

	// Address Returns the address for the current store
	Address() address.Address

	// Index Returns the StoreIndex struct for the current store instance
	Index() StoreIndex

	// Type Returns the current store type as a string
	Type() string

	// ReplicationStatus Returns the current ReplicationInfo status
	ReplicationStatus() replicator.ReplicationInfo

	// Drop Removes all the local store content
	Drop() error

	// Load Fetches entries on the network
	Load(ctx context.Context, amount int) error

	// Sync Merges stores with the given heads
	Sync(ctx context.Context, heads []*entry.Entry) error

	// LoadMoreFrom Loads more entries from the given CIDs
	LoadMoreFrom(ctx context.Context, amount uint, entries []cid.Cid)

	// SaveSnapshot Save the current state of the store and returns a CID
	SaveSnapshot(ctx context.Context) (cid.Cid, error)

	// LoadFromSnapshot Loads store content from a snapshot
	LoadFromSnapshot(ctx context.Context) error

	// OpLog Returns the underlying IPFS Log instance for the store
	OpLog() *ipfslog.Log

	// IPFS Returns the IPFS instance for the store
	IPFS() coreapi.CoreAPI

	// DBName Returns the store name as a string
	DBName() string

	// Identity Returns the identity used for the current store
	Identity() *identityprovider.Identity

	// AccessController Returns the access controller for this store
	AccessController() accesscontroller.Interface

	// AddOperation Adds an operation to this store
	AddOperation(ctx context.Context, op operation.Operation, onProgressCallback chan<- *entry.Entry) (*entry.Entry, error)
}

// EventLogStore A type of store that provides an append only log
type EventLogStore interface {
	Store

	// Add Appends data to the log
	Add(ctx context.Context, data []byte) (operation.Operation, error)

	// Get Fetches an entry of the log
	Get(ctx context.Context, cid cid.Cid) (operation.Operation, error)

	// Stream Populates a chan of entries from this store
	Stream(ctx context.Context, resultChan chan operation.Operation, options *StreamOptions) error

	// List Fetches a list of operation that occurred on this store
	List(ctx context.Context, options *StreamOptions) ([]operation.Operation, error)
}

// EventLogStore A type of store that provides a key value store
type KeyValueStore interface {
	Store

	// All Returns a consolidated key value map from the entries of this store
	All() map[string][]byte

	// Put Sets the value for a key of the map
	Put(ctx context.Context, key string, value []byte) (operation.Operation, error)

	// Delete Clears the value for a key of the map
	Delete(ctx context.Context, key string) (operation.Operation, error)

	// Get Retrieves the value for a key of the map
	Get(ctx context.Context, key string) ([]byte, error)
}

// StoreIndex Index contains the state of a datastore,
// ie. what data we currently have.
//
// Index receives a call from a Store when the operations log for the Store
// was updated, ie. new operations were added. In updateIndex, the Index
// implements its CRDT logic: add, remove or update items in the data
// structure. Each new operation received from the operations log is applied
// in order onto the current state, ie. each new operation changes the data
// and the state changes.
//
// Implementing each CRDT as an Index, we can implement both operation-based
// and state-based CRDTs with the same higher level abstractions.
// To read the current state of the database, Index provides a single public
// function: `get()`. It is up to the Store to decide what kind of query
// capabilities it provides to the consumer.
type StoreIndex interface {
	// Get Returns the state of the datastore, ie. most up-to-date data
	Get(key string) interface{}

	// UpdateIndex Applies operations to the Index and updates the state
	UpdateIndex(log *ipfslog.Log, entries []*entry.Entry) error
}

// NewStoreOptions Lists the options to create a new store
type NewStoreOptions struct {
	Index                  IndexConstructor
	AccessController       accesscontroller.Interface
	Cache                  datastore.Datastore
	CacheDestroy           func() error
	ReplicationConcurrency uint
	ReferenceCount         *int
	Replicate              *bool
	MaxHistory             *int
	Directory              string
}

// StoreConstructor Defines the expected constructor for a custom store
type StoreConstructor func(context.Context, coreapi.CoreAPI, *identityprovider.Identity, address.Address, *NewStoreOptions) (Store, error)

// IndexConstructor Defines the expected constructor for a custom index
type IndexConstructor func(publicKey []byte) StoreIndex

// OnWritePrototype Defines the callback function prototype which is triggered on a write
type OnWritePrototype func(ctx context.Context, addr cid.Cid, entry *entry.Entry, heads []cid.Cid) error
