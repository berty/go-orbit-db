package stores

import (
	ipfslog "berty.tech/go-ipfs-log"
	"berty.tech/go-ipfs-log/entry"
	"berty.tech/go-ipfs-log/identityprovider"
	"context"
	"github.com/berty/go-orbit-db/accesscontroller"
	"github.com/berty/go-orbit-db/address"
	"github.com/berty/go-orbit-db/ipfs"
	"github.com/berty/go-orbit-db/stores/operation"
	"github.com/berty/go-orbit-db/stores/replicator"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
)

type Interface interface {
	Close() error
	Address() address.Address

	Index() Index
	Type() string
	ReplicationStatus() replicator.ReplicationInfo
	Drop() error
	Load(ctx context.Context, amount int) error
	Sync(ctx context.Context, heads []*entry.Entry) error
	LoadMoreFrom(ctx context.Context, amount uint, entries []cid.Cid)
	SaveSnapshot(ctx context.Context) (cid.Cid, error)
	LoadFromSnapshot(ctx context.Context) error
	Subscribe(chan Event)
	Unsubscribe(chan Event)
	OpLog() *ipfslog.Log
	Ipfs() ipfs.Services
	DBName() string
	Identity() *identityprovider.Identity
	AccessController() accesscontroller.Interface
	AddOperation(ctx context.Context, op operation.Operation, onProgressCallback chan<- *entry.Entry) (*entry.Entry, error)
}

type Index interface {
	Get(key string) interface{}
	UpdateIndex(log *ipfslog.Log, entries []*entry.Entry) error
}

type NewStoreOptions struct {
	Index                  IndexConstructor
	AccessController       accesscontroller.Interface
	Cache                  datastore.Datastore
	ReplicationConcurrency uint
	ReferenceCount         *int
	Replicate              *bool
	MaxHistory             *int
	Directory              string
}

type Constructor func(context.Context, ipfs.Services, *identityprovider.Identity, address.Address, *NewStoreOptions) (Interface, error)

type IndexConstructor func(publicKey []byte) Index

type OnWritePrototype func(ctx context.Context, addr cid.Cid, entry *entry.Entry, heads []cid.Cid) error
