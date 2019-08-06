package orbitdb

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

// TODO: interface?
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

type DetermineAddressOptions struct {
	OnlyHash         *bool
	Replicate        *bool
	AccessController accesscontroller.Interface
}

type ExchangedHeads struct {
	Address string         `json:"address,omitempty"`
	Heads   []*entry.Entry `json:"heads,omitempty"`
}

type OrbitDB interface {
	IPFS() coreapi.CoreAPI
	Identity() *identityprovider.Identity

	Open(ctx context.Context, dbAddress string, options *CreateDBOptions) (Store, error)
	Log(ctx context.Context, address string, options *CreateDBOptions) (EventLogStore, error)
	KeyValue(ctx context.Context, address string, options *CreateDBOptions) (KeyValueStore, error)
	Create(ctx context.Context, name string, storeType string, options *CreateDBOptions) (Store, error)
	Close() error

	DetermineAddress(ctx context.Context, name string, storeType string, options *DetermineAddressOptions) (address.Address, error)
}

type StreamOptions struct {
	GT     *cid.Cid
	GTE    *cid.Cid
	LT     *cid.Cid
	LTE    *cid.Cid
	Amount *int
}

type Store interface {
	events.EmitterInterface
	Close() error
	Address() address.Address

	Index() StoreIndex
	Type() string
	ReplicationStatus() replicator.ReplicationInfo
	Drop() error
	Load(ctx context.Context, amount int) error
	Sync(ctx context.Context, heads []*entry.Entry) error
	LoadMoreFrom(ctx context.Context, amount uint, entries []cid.Cid)
	SaveSnapshot(ctx context.Context) (cid.Cid, error)
	LoadFromSnapshot(ctx context.Context) error
	OpLog() *ipfslog.Log
	Ipfs() coreapi.CoreAPI
	DBName() string
	Identity() *identityprovider.Identity
	AccessController() accesscontroller.Interface
	AddOperation(ctx context.Context, op operation.Operation, onProgressCallback chan<- *entry.Entry) (*entry.Entry, error)
}

type EventLogStore interface {
	Store
	Add(ctx context.Context, data []byte) (operation.Operation, error)
	Get(ctx context.Context, cid cid.Cid) (operation.Operation, error)
	Stream(ctx context.Context, resultChan chan operation.Operation, options *StreamOptions) error
	List(ctx context.Context, options *StreamOptions) ([]operation.Operation, error)
}

type KeyValueStore interface {
	Store
	All() map[string][]byte
	Put(ctx context.Context, key string, value []byte) (operation.Operation, error)
	Delete(ctx context.Context, key string) (operation.Operation, error)
	Get(ctx context.Context, key string) ([]byte, error)
}

type StoreIndex interface {
	Get(key string) interface{}
	UpdateIndex(log *ipfslog.Log, entries []*entry.Entry) error
}

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

type StoreConstructor func(context.Context, coreapi.CoreAPI, *identityprovider.Identity, address.Address, *NewStoreOptions) (Store, error)

type IndexConstructor func(publicKey []byte) StoreIndex

type OnWritePrototype func(ctx context.Context, addr cid.Cid, entry *entry.Entry, heads []cid.Cid) error
