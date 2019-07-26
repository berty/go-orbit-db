package orbitdb

import (
	"berty.tech/go-ipfs-log/entry"
	"berty.tech/go-ipfs-log/identityprovider"
	"berty.tech/go-ipfs-log/keystore"
	"context"
	"github.com/berty/go-orbit-db/accesscontroller"
	"github.com/berty/go-orbit-db/address"
	"github.com/berty/go-orbit-db/ipfs"
	"github.com/berty/go-orbit-db/stores"
	"github.com/berty/go-orbit-db/stores/eventlogstore"
	"github.com/berty/go-orbit-db/stores/kvstore"
	"github.com/ipfs/go-datastore"
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
	IPFS() ipfs.Services
	Identity() *identityprovider.Identity

	Open(ctx context.Context, dbAddress string, options *CreateDBOptions) (stores.Interface, error)
	Log(ctx context.Context, address string, options *CreateDBOptions) (eventlogstore.OrbitDBEventLogStore, error)
	KeyValue(ctx context.Context, address string, options *CreateDBOptions) (kvstore.OrbitDBKeyValue, error)
	Create(ctx context.Context, name string, storeType string, options *CreateDBOptions) (stores.Interface, error)
	Close() error

	DetermineAddress(ctx context.Context, name string, storeType string, options *DetermineAddressOptions) (address.Address, error)
}
