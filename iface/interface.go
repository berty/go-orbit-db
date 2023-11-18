package iface

import (
	"context"
	"time"

	ipfslog "berty.tech/go-ipfs-log"
	"berty.tech/go-ipfs-log/entry"
	"berty.tech/go-ipfs-log/identityprovider"
	"berty.tech/go-ipfs-log/iface"
	"berty.tech/go-ipfs-log/keystore"
	"berty.tech/go-orbit-db/accesscontroller"
	"berty.tech/go-orbit-db/address"
	"berty.tech/go-orbit-db/events"
	"berty.tech/go-orbit-db/stores/operation"
	"berty.tech/go-orbit-db/stores/replicator"
	coreapi "github.com/ipfs/boxo/coreiface"
	cid "github.com/ipfs/go-cid"
	datastore "github.com/ipfs/go-datastore"
	"github.com/libp2p/go-libp2p/core/event"
	peer "github.com/libp2p/go-libp2p/core/peer"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

// Message Marshaler

type MessageExchangeHeads struct {
	Address string         `json:"address"`
	Heads   []*entry.Entry `json:"heads"`
}

type MessageMarshaler interface {
	Marshal(msg *MessageExchangeHeads) ([]byte, error)
	Unmarshal(data []byte, msg *MessageExchangeHeads) error
}

// CreateDBOptions lists the arguments to create a store
type CreateDBOptions struct {
	EventBus                event.Bus
	Directory               *string
	Overwrite               *bool
	LocalOnly               *bool
	Create                  *bool
	StoreType               *string
	AccessControllerAddress string
	AccessController        accesscontroller.ManifestParams
	Replicate               *bool
	Keystore                keystore.Interface
	Cache                   datastore.Datastore
	Identity                *identityprovider.Identity
	SortFn                  ipfslog.SortFn
	IO                      ipfslog.IO
	Timeout                 time.Duration
	MessageMarshaler        MessageMarshaler
	Logger                  *zap.Logger
	CloseFunc               func()
	StoreSpecificOpts       interface{}
}

type CreateDocumentDBOptions struct {
	KeyExtractor func(interface{}) (string, error)
	Marshal      func(interface{}) ([]byte, error)
	Unmarshal    func(data []byte, v interface{}) error
	ItemFactory  func() interface{}
}

// DetermineAddressOptions Lists the arguments used to determine a store address
type DetermineAddressOptions struct {
	OnlyHash         *bool
	Replicate        *bool
	AccessController accesscontroller.ManifestParams
}

// BaseOrbitDB Provides the main OrbitDB interface used to open and create stores
type BaseOrbitDB interface {
	// IPFS Returns the instance of the IPFS API used by the current DB
	IPFS() coreapi.CoreAPI

	// Identity Returns the identity used by the current DB
	Identity() *identityprovider.Identity

	// Open Opens an existing data store
	Open(ctx context.Context, dbAddress string, options *CreateDBOptions) (Store, error)

	// Create Creates a new store
	Create(ctx context.Context, name string, storeType string, options *CreateDBOptions) (Store, error)

	// Close Closes the current DB and all the related stores
	Close() error

	// DetermineAddress Returns the store address for the given parameters
	DetermineAddress(ctx context.Context, name string, storeType string, options *DetermineAddressOptions) (address.Address, error)

	// RegisterStoreType Registers a new store type
	RegisterStoreType(storeType string, constructor StoreConstructor)

	// UnregisterStoreType Removes a store type
	UnregisterStoreType(storeType string)

	// RegisterAccessControllerType Registers a new access controller type
	RegisterAccessControllerType(AccessControllerConstructor) error

	// UnregisterAccessControllerType Unregisters an access controller type
	UnregisterAccessControllerType(string)

	// GetAccessControllerType Retrieves an access controller type constructor if it exists
	GetAccessControllerType(string) (AccessControllerConstructor, bool)

	// EventBus Returns the eventsBus
	EventBus() event.Bus

	// Logger Returns the logger
	Logger() *zap.Logger

	// Tracer Returns the tracer
	Tracer() trace.Tracer
}

// OrbitDBDocumentStore An OrbitDB instance providing a Document store
type OrbitDBDocumentStore interface {
	BaseOrbitDB
	OrbitDBDocumentStoreProvider
}

// OrbitDBDocumentStoreProvider Exposes a method providing a document store
type OrbitDBDocumentStoreProvider interface {
	// Docs Creates or opens an DocumentStore
	Docs(ctx context.Context, address string, options *CreateDBOptions) (DocumentStore, error)
}

// OrbitDBKVStore An OrbitDB instance providing a KeyValue store
type OrbitDBKVStore interface {
	BaseOrbitDB
	OrbitDBKVStoreProvider
}

// OrbitDBKVStoreProvider Exposes a method providing a key value store
type OrbitDBKVStoreProvider interface {
	// KeyValue Creates or opens an KeyValueStore
	KeyValue(ctx context.Context, address string, options *CreateDBOptions) (KeyValueStore, error)
}

// OrbitDBLogStore An OrbitDB instance providing an Event Log store
type OrbitDBLogStore interface {
	BaseOrbitDB
	OrbitDBLogStoreProvider
}

// OrbitDBLogStoreProvider Exposes a method providing an event log store
type OrbitDBLogStoreProvider interface {
	// Log Creates or opens an EventLogStore
	Log(ctx context.Context, address string, options *CreateDBOptions) (EventLogStore, error)
}

// OrbitDB Provides an OrbitDB interface with the default access controllers and store types
type OrbitDB interface {
	BaseOrbitDB

	OrbitDBKVStoreProvider
	OrbitDBLogStoreProvider
	OrbitDBDocumentStoreProvider
}

// StreamOptions Defines the parameters that can be given to the Stream function of an EventLogStore
type StreamOptions struct {
	GT     *cid.Cid
	GTE    *cid.Cid
	LT     *cid.Cid
	LTE    *cid.Cid
	Amount *int
}

type StoreEvents interface {
	Subscribe()
}

// Store Defines the operations common to all stores types
type Store interface {
	// Deprecated: use EventBus() instead
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

	// Replicator Returns the Replicator object
	Replicator() replicator.Replicator

	// Cache Returns the Cache object
	Cache() datastore.Datastore

	// Drop Removes all the local store content
	Drop() error

	// Load Fetches entries on the network
	Load(ctx context.Context, amount int) error

	// Sync Merges stores with the given heads
	Sync(ctx context.Context, heads []ipfslog.Entry) error

	// LoadMoreFrom Loads more entries from the given CIDs
	LoadMoreFrom(ctx context.Context, amount uint, entries []ipfslog.Entry)

	// LoadFromSnapshot Loads store content from a snapshot
	LoadFromSnapshot(ctx context.Context) error

	// OpLog Returns the underlying IPFS Log instance for the store
	OpLog() ipfslog.Log

	// IPFS Returns the IPFS instance for the store
	IPFS() coreapi.CoreAPI

	// DBName Returns the store name as a string
	DBName() string

	// Identity Returns the identity used for the current store
	Identity() *identityprovider.Identity

	// AccessController Returns the access controller for this store
	AccessController() accesscontroller.Interface

	// AddOperation Adds an operation to this store
	AddOperation(ctx context.Context, op operation.Operation, onProgressCallback chan<- ipfslog.Entry) (ipfslog.Entry, error)

	// Logger Returns the logger
	Logger() *zap.Logger

	// Tracer Returns the tracer
	Tracer() trace.Tracer

	IO() iface.IO

	// subscribe to events on this store
	EventBus() event.Bus
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

// KeyValueStore A type of store that provides a key value store
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

type DocumentStoreGetOptions struct {
	CaseInsensitive bool
	PartialMatches  bool
}

// DocumentStore A type of store that provides a document store
type DocumentStore interface {
	Store

	// Put Stores the document
	Put(ctx context.Context, document interface{}) (operation.Operation, error)

	// Delete Clears the document for a key
	Delete(ctx context.Context, key string) (operation.Operation, error)

	// PutBatch Add values as multiple operations and returns the latest
	PutBatch(ctx context.Context, values []interface{}) (operation.Operation, error)

	// PutAll Add values as a single operation and returns it
	PutAll(ctx context.Context, values []interface{}) (operation.Operation, error)

	// Get Retrieves the document for a key
	Get(ctx context.Context, key string, opts *DocumentStoreGetOptions) ([]interface{}, error)

	// Query Finds documents using a filter function
	Query(ctx context.Context, filter func(doc interface{}) (bool, error)) ([]interface{}, error)
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
	UpdateIndex(log ipfslog.Log, entries []ipfslog.Entry) error
}

// NewStoreOptions Lists the options to create a new store
type NewStoreOptions struct {
	EventBus               event.Bus
	Index                  IndexConstructor
	AccessController       accesscontroller.Interface
	Cache                  datastore.Datastore
	CacheDestroy           func() error
	ReplicationConcurrency uint
	ReferenceCount         *int
	Replicate              *bool
	MaxHistory             *int
	Directory              string
	SortFn                 ipfslog.SortFn
	Logger                 *zap.Logger
	Tracer                 trace.Tracer
	IO                     ipfslog.IO
	PubSub                 PubSubInterface
	MessageMarshaler       MessageMarshaler
	PeerID                 peer.ID
	DirectChannel          DirectChannel
	CloseFunc              func()
	StoreSpecificOpts      interface{}
}

type DirectChannelOptions struct {
	Logger *zap.Logger
}

type DirectChannel interface {
	// Connect Waits for the other peer to be connected
	Connect(context.Context, peer.ID) error

	// Send Sends a message to the other peer
	Send(ctx context.Context, peer peer.ID, data []byte) error

	// Close Closes the connection
	Close() error
}

type DirectChannelEmitter interface {
	Emit(*EventPubSubPayload) error
	Close() error
}

type DirectChannelFactory func(ctx context.Context, emitter DirectChannelEmitter, opts *DirectChannelOptions) (DirectChannel, error)

// StoreConstructor Defines the expected constructor for a custom store
type StoreConstructor func(coreapi.CoreAPI, *identityprovider.Identity, address.Address, *NewStoreOptions) (Store, error)

// IndexConstructor Defines the expected constructor for a custom index
type IndexConstructor func(publicKey []byte) StoreIndex

// OnWritePrototype Defines the callback function prototype which is triggered on a write
type OnWritePrototype func(ctx context.Context, addr cid.Cid, entry ipfslog.Entry, heads []cid.Cid) error

// AccessControllerConstructor Required prototype for custom controllers constructors
type AccessControllerConstructor func(context.Context, BaseOrbitDB, accesscontroller.ManifestParams, ...accesscontroller.Option) (accesscontroller.Interface, error)

// PubSubTopic is a pub sub subscription to a topic
type PubSubTopic interface {
	// Publish Posts a new message on a topic
	Publish(ctx context.Context, message []byte) error

	// Peers Lists peers connected to the topic
	Peers(ctx context.Context) ([]peer.ID, error)

	// WatchPeers subscribes to peers joining or leaving the topic
	WatchPeers(ctx context.Context) (<-chan events.Event, error)

	// WatchMessages Subscribes to new messages
	WatchMessages(ctx context.Context) (<-chan *EventPubSubMessage, error)

	// Topic Returns the topic name
	Topic() string
}

type PubSubInterface interface {
	// TopicSubscribe Subscribes to a topic
	TopicSubscribe(ctx context.Context, topic string) (PubSubTopic, error)
}

type PubSubSubscriptionOptions struct {
	Logger *zap.Logger
	Tracer trace.Tracer
}

// EventPubSubMessage Indicates a new message posted on a pubsub topic
type EventPubSubMessage struct {
	Content []byte
}

// EventPubSubPayload An event received on new messages
type EventPubSubPayload struct {
	Payload []byte
	Peer    peer.ID
}

// EventPubSubJoin Is an event triggered when a peer joins the channel
type EventPubSubJoin struct {
	Topic string
	Peer  peer.ID
}

// EventPubSubLeave Is an event triggered when a peer leave the channel
type EventPubSubLeave struct {
	Topic string
	Peer  peer.ID
}
